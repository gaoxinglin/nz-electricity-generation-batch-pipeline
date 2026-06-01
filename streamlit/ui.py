"""Shared presentation helpers for the Streamlit dashboard.

The dashboard is a portfolio data product, so each page should answer a clear
market question before it exposes detailed charts or tables.
"""

from __future__ import annotations

from collections.abc import Sequence
from html import escape

import pandas as pd
import streamlit as st


def inject_global_styles() -> None:
    """Apply lightweight, theme-aware styling used across pages."""
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1.25rem;
                padding-bottom: 2.5rem;
                max-width: 1480px;
            }
            div[data-testid="metric-container"] {
                background: var(--secondary-background-color);
                border: 1px solid rgba(120, 120, 120, 0.16);
                border-radius: 8px;
                padding: 14px 16px;
                box-shadow: none;
            }
            div[data-testid="stCaptionContainer"] {
                color: rgba(100, 100, 100, 0.95);
            }
            .nzeg-question {
                border-left: 4px solid #1976D2;
                background: rgba(25, 118, 210, 0.08);
                border-radius: 0 8px 8px 0;
                padding: 0.9rem 1rem;
                margin: 0.25rem 0 1rem 0;
            }
            .nzeg-question strong {
                display: block;
                margin-bottom: 0.2rem;
            }
            .nzeg-insight {
                border: 1px solid rgba(120, 120, 120, 0.18);
                border-radius: 8px;
                padding: 0.85rem 1rem;
                margin: 0.45rem 0 0.9rem 0;
                background: var(--secondary-background-color);
            }
            .nzeg-insight strong {
                display: block;
                margin-bottom: 0.2rem;
            }
            .nzeg-scope {
                display: flex;
                gap: 0.6rem;
                flex-wrap: wrap;
                margin: 0.25rem 0 1rem 0;
            }
            .nzeg-pill {
                border: 1px solid rgba(120, 120, 120, 0.22);
                border-radius: 999px;
                padding: 0.28rem 0.7rem;
                background: rgba(120, 120, 120, 0.06);
                font-size: 0.9rem;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def page_header(title: str, question: str, detail: str | None = None) -> None:
    inject_global_styles()
    st.title(title)
    body = f"<strong>Question this page answers</strong>{escape(question)}"
    if detail:
        body += f"<br><span>{escape(detail)}</span>"
    st.markdown(f'<div class="nzeg-question">{body}</div>', unsafe_allow_html=True)


def insight_box(title: str, body: str) -> None:
    st.markdown(
        f'<div class="nzeg-insight"><strong>{escape(title)}</strong>{escape(body)}</div>',
        unsafe_allow_html=True,
    )


def scope_bar(items: dict[str, str]) -> None:
    pills = "".join(
        f'<span class="nzeg-pill"><strong>{escape(label)}:</strong> {escape(value)}</span>'
        for label, value in items.items()
    )
    st.markdown(f'<div class="nzeg-scope">{pills}</div>', unsafe_allow_html=True)


def fmt_month(ym: str | int | pd.Timestamp) -> str:
    if pd.isna(ym):
        return "N/A"
    return pd.to_datetime(str(ym).zfill(6), format="%Y%m").strftime("%b %Y")


def fmt_date(value: object) -> str:
    if pd.isna(value):
        return "N/A"
    return pd.to_datetime(value).strftime("%d %b %Y")


def fmt_int(value: object) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{int(value):,}"


def fmt_float(value: object, digits: int = 1, suffix: str = "") -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):,.{digits}f}{suffix}"


def fmt_gwh(value: object) -> str:
    return fmt_float(value, 1, " GWh")


def fmt_mw(value: object) -> str:
    return fmt_float(value, 0, " MW")


def fmt_price(value: object, digits: int = 0) -> str:
    if pd.isna(value):
        return "N/A"
    return f"${float(value):,.{digits}f}/MWh"


def fmt_pct(value: object, digits: int = 1) -> str:
    return fmt_float(value, digits, "%")


def fmt_delta(value: object, digits: int = 1, suffix: str = "") -> str | None:
    if pd.isna(value):
        return None
    return f"{float(value):+,.{digits}f}{suffix}"


def safe_divide(numerator: float, denominator: float) -> float | None:
    if denominator in (0, 0.0) or pd.isna(denominator):
        return None
    return float(numerator) / float(denominator)


def date_range_filter(
    df: pd.DataFrame,
    column: str,
    *,
    key: str,
    label: str = "Date range",
) -> tuple[pd.Timestamp, pd.Timestamp]:
    values = pd.to_datetime(df[column].dropna())
    d0 = values.min().date()
    d1 = values.max().date()
    selected = st.date_input(label, value=(d0, d1), min_value=d0, max_value=d1, key=key)
    if isinstance(selected, tuple) and len(selected) == 2:
        return pd.to_datetime(selected[0]), pd.to_datetime(selected[1])
    return pd.to_datetime(d0), pd.to_datetime(d1)


def month_range_filter(
    df: pd.DataFrame,
    column: str = "year_month",
    *,
    key: str,
) -> tuple[str, str]:
    months = sorted(df[column].dropna().astype(str).unique().tolist())
    if len(months) == 1:
        st.caption(f"Available month: {fmt_month(months[0])}")
        return months[0], months[0]

    labels = [fmt_month(month) for month in months]
    by_label = dict(zip(labels, months, strict=True))
    start_col, end_col = st.columns(2)
    start_label = start_col.selectbox("From month", labels, index=0, key=f"{key}_start")
    end_label = end_col.selectbox("To month", labels, index=len(labels) - 1, key=f"{key}_end")
    start_ym = by_label[start_label]
    end_ym = by_label[end_label]
    if start_ym > end_ym:
        st.info("The selected start month is after the end month, so the dashboard has swapped them.")
        return end_ym, start_ym
    return start_ym, end_ym


def multiselect_all(
    label: str,
    options: Sequence[str],
    *,
    key: str,
) -> list[str]:
    clean_options = sorted([value for value in options if pd.notna(value)])
    if not clean_options:
        st.caption(f"No {label.lower()} values are available for this dataset.")
        return []
    return st.multiselect(label, clean_options, default=clean_options, key=key)


def latest_previous_delta(
    df: pd.DataFrame,
    *,
    month_col: str,
    value_col: str,
    latest_month: str,
    group_sum: bool = True,
) -> float | None:
    latest_date = pd.to_datetime(latest_month, format="%Y%m")
    prior_month = (latest_date - pd.DateOffset(years=1)).strftime("%Y%m")
    if group_sum:
        latest = df.loc[df[month_col] == latest_month, value_col].sum()
        prior = df.loc[df[month_col] == prior_month, value_col].sum()
    else:
        latest_values = df.loc[df[month_col] == latest_month, value_col]
        prior_values = df.loc[df[month_col] == prior_month, value_col]
        latest = latest_values.iloc[0] if not latest_values.empty else pd.NA
        prior = prior_values.iloc[0] if not prior_values.empty else pd.NA
    ratio = safe_divide(float(latest) - float(prior), float(prior)) if pd.notna(prior) else None
    return ratio * 100 if ratio is not None else None
