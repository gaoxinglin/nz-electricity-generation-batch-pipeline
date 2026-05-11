"""
Ingest dbt run-result artifacts into raw.raw_dbt_run.

Reads `dbt/target/run_results.json`, flattens each result row, and appends
to a single fact-of-runs table. One run of dbt produces one invocation_id
and N rows (one per node executed — models + tests + seeds).

Supports both targets:
  --target=duckdb (default) → DuckDB at NZEG_DUCKDB_PATH
  --target=snowflake        → Snowflake via env vars (same as load_snowflake_price.py)

Append-only: never deletes prior invocations. The Pipeline Health dashboard
uses MAX(invocation_id) per node for the latest status and full history for
trends. Idempotent on the same invocation_id (DELETE-then-INSERT inside one
transaction).

Usage:
    # Local mode (after `make local-full` or `dbt run` from repo root):
    python scripts/ingest_dbt_artifacts.py \\
        --artifact dbt/target/run_results.json \\
        --target duckdb --db data/nzeg.duckdb

    # Cloud mode (called from v2 DAG post-dbt):
    python scripts/ingest_dbt_artifacts.py \\
        --artifact /opt/dbt/target/run_results.json \\
        --target snowflake
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger("ingest_dbt_artifacts")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# (column, source-type) — kept as VARCHAR in raw for predictable cross-warehouse
# behaviour. stg layer casts.
RAW_COLUMNS = [
    "invocation_id",
    "generated_at",
    "dbt_version",
    "node_unique_id",
    "node_type",
    "node_name",
    "status",
    "execution_time_seconds",
    "failures",
    "message",
    "relation_name",
    "adapter_message",
]


def parse_artifact(path: Path) -> list[dict]:
    """Read run_results.json and return rows as dicts (all values stringified)."""
    data = json.loads(path.read_text())
    meta = data["metadata"]
    invocation_id = meta["invocation_id"]
    generated_at = meta["generated_at"]
    dbt_version = meta.get("dbt_version", "")

    rows: list[dict] = []
    for r in data["results"]:
        uid = r["unique_id"]
        parts = uid.split(".")
        node_type = parts[0] if parts else ""
        node_name = parts[-1] if len(parts) >= 2 else uid
        adapter_msg = ""
        if isinstance(r.get("adapter_response"), dict):
            adapter_msg = str(r["adapter_response"].get("_message", ""))[:500]

        rows.append(
            {
                "invocation_id": invocation_id,
                "generated_at": generated_at,
                "dbt_version": dbt_version,
                "node_unique_id": uid,
                "node_type": node_type,
                "node_name": node_name,
                "status": r.get("status", ""),
                "execution_time_seconds": str(r.get("execution_time", "")),
                "failures": "" if r.get("failures") is None else str(r["failures"]),
                "message": (r.get("message") or "")[:500],
                "relation_name": r.get("relation_name") or "",
                "adapter_message": adapter_msg,
            }
        )
    return rows


# ─── DuckDB target ─────────────────────────────────────────────


def ensure_duckdb_table(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cols = ",\n        ".join(f'"{c}" VARCHAR' for c in RAW_COLUMNS)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS raw.raw_dbt_run (
            {cols}
        )
    """)


def write_duckdb(rows: list[dict], db_path: Path) -> None:
    import duckdb
    conn = duckdb.connect(str(db_path))
    try:
        ensure_duckdb_table(conn)
        if not rows:
            return
        invocation_id = rows[0]["invocation_id"]
        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                "DELETE FROM raw.raw_dbt_run WHERE invocation_id = ?",
                [invocation_id],
            )
            placeholders = ", ".join(["?"] * len(RAW_COLUMNS))
            conn.executemany(
                f"INSERT INTO raw.raw_dbt_run VALUES ({placeholders})",
                [[r[c] for c in RAW_COLUMNS] for r in rows],
            )
            conn.execute("COMMIT")
            logger.info("DuckDB: ingested %d rows for invocation %s",
                        len(rows), invocation_id)
        except Exception:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


# ─── Snowflake target ──────────────────────────────────────────


def write_snowflake(rows: list[dict]) -> None:
    import snowflake.connector
    from cryptography.hazmat.primitives.serialization import (
        Encoding, NoEncryption, PrivateFormat, load_pem_private_key,
    )
    key_path = os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"])
    with open(key_path, "rb") as f:
        pkey = load_pem_private_key(f.read(), password=None).private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=pkey,
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.environ["SNOWFLAKE_ROLE"],
        schema="RAW",
    )
    try:
        cur = conn.cursor()
        cols_sql = ",\n        ".join(f'"{c}" VARCHAR' for c in RAW_COLUMNS)
        cur.execute(f"CREATE TABLE IF NOT EXISTS raw_dbt_run ({cols_sql})")
        if not rows:
            return
        invocation_id = rows[0]["invocation_id"]
        cur.execute("BEGIN")
        try:
            cur.execute(
                "DELETE FROM raw_dbt_run WHERE invocation_id = %s",
                (invocation_id,),
            )
            placeholders = ", ".join(["%s"] * len(RAW_COLUMNS))
            cur.executemany(
                f"INSERT INTO raw_dbt_run ({', '.join(RAW_COLUMNS)}) "
                f"VALUES ({placeholders})",
                [tuple(r[c] for c in RAW_COLUMNS) for r in rows],
            )
            cur.execute("COMMIT")
            logger.info("Snowflake: ingested %d rows for invocation %s",
                        len(rows), invocation_id)
        except Exception:
            cur.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingest dbt run_results.json into warehouse")
    p.add_argument("--artifact", required=True, help="Path to run_results.json")
    p.add_argument("--target", choices=["duckdb", "snowflake"], default="duckdb")
    p.add_argument("--db", default="data/nzeg.duckdb",
                   help="DuckDB path (target=duckdb only)")
    args = p.parse_args(argv)

    path = Path(args.artifact)
    if not path.exists():
        logger.warning("no artifact at %s — skipping", path)
        return 0

    rows = parse_artifact(path)
    logger.info("parsed %d node results", len(rows))

    if args.target == "duckdb":
        write_duckdb(rows, Path(args.db))
    else:
        write_snowflake(rows)
    return 0


if __name__ == "__main__":
    sys.exit(main())
