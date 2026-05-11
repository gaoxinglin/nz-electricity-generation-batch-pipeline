# PRD: NZ Electricity Wholesale Market Analytics Platform

**项目代号**: NZEG-V2
**PRD 版本**: **5.0（Implementer-Ready：Lead DE Bootstrap Round）**
**日期**: 2026-05-11
**作者**: Andrew

---

## 变更记录

| PRD 版本 | 日期 | 变更说明 |
|---------|------|---------|
| 1.0 | 2026-05-09 | 初稿 |
| 2.0 | 2026-05-09 | Senior DE Review：发现 15 个问题（3 红灯、5 黄灯、7 绿灯） |
| 3.0 | 2026-05-09 | 合并所有修复。核心变更：(1) Generation_MD 确认为日度粒度；(2) POC_Code 直接 join；(3) cross-db macro 改为完整 CTE；(4) 工时上调 40%；(5) 新增 Known Limitations、Schema Contract、幂等性设计、负电价处理 |
| 4.0 | 2026-05-11 | Senior DE Review Round 2 修复（4 红灯、5 黄灯、6 绿灯）。核心变更：(1) DST 边界：macro 覆盖 TP01-TP50，stg 测试 1-50；(2) fct_price incremental 改为 `unique_key + delete+insert`，修复 interim→final 冲突；(3) 补充 dim_catchment + catchment→island 映射 seed；(4) int_generation_by_poc 明确物化为 table；(5) DAG 拆成两层（核心/可选）；(6) 新增 §13 Monitoring & Alerting、§14 V1→V2 迁移策略；(7) 时区显式声明；(8) POC 匹配率质量门控；(9) Island Spread 双指标；(10) profiles.yml 提交策略修正；(11) Git LFS 补全；(12) 回填费用估算 |
| 4.1 | 2026-05-11 | Senior DE Review Round 3 修复 + 简洁化。**逻辑修复**：(1) 删除 stg_price 错误的 macro 调用示例（price 本身是长表）；(2) fct_generation/fct_price 去掉 `node_key` FK，poc_code 作为退化维度，dim_node 仅在 mart 层 join，消除 DAG 间依赖矛盾；(3) macro 输出列名参数化（`value_col_name`）；(4) `check_bymonth_or_daily` 补充伪代码；(5) §6.2 调度表对齐新 DAG 结构；(6) fct_generation 行数测试改为 per-month；(7) DS-1 TradingPeriod 范围澄清。**简洁化**：(8) 双 DAG 合并为单 DAG + `NONE_FAILED` 任务组；(9) 删除 `nz_timezone`、`poc_match_*_threshold` 三个无用 var；(10) 删除 PythonSensor 404 检测（Airflow `email_on_failure` 已够用）；(11) §14 V1→V2 迁移压缩为一段；(12) 删除重复 `---`、修正 DAG 文件名 |
| 4.2 | 2026-05-11 | Senior DE Review Round 4：**产品减法**。**Scope 削减（−5 天 / −7 模型 / −1 数据源）**：(1) Hydro 数据源整体推到 V3（省 3 天 + 删 dim_catchment、fct_hydro、mart_hydro_price_driver、hydro_catchment_mapping seed、download_hydro/validate_hydro）；(2) 删除 Git LFS 与预制 DuckDB sample，`make demo` 改为现下 1 个月数据（省 0.75 天）；(3) 合并 mart：删 `mart_island_price_spread`（Q9 改为 mart_price_daily 上的 dashboard 查询）和 `mart_generation_price_monthly`（无对应 Q）。**新增**：(4) §15 CI/CD 详情；(5) §3.1 Secrets 管理一行；(6) §10.1 加 Streamlit Cloud + Snowflake 冷启动说明。**逻辑修复**：(7) §5.2 fct_price 描述 `node × tp × date` → `POC × tp × date`；(8) §6.1 DAG 图改为真并行；(9) 删除 `--retry-missing flag` 悬空引用；(10) §5.2 合并 int_price_daily 和 int_island_price 的 non_proxy 字段，避免重复 |
| 4.3 | 2026-05-11 | Senior DE Review Round 5：用词精确性 + 可执行性。**可执行性修复**：(1) CI fork PR 只跑 dev target、main push 才跑 prod；(2) Makefile 用 `uv sync` 替代 `pip install` 直接装包；(3) `backfill` target 改为完整可执行命令；(4) Phase 0 明确"为 download 脚本加 `--months`/`--years` 参数"；(5) Phase 0.0 Mini POC "TP1-TP3" 改为明确"macro 只 unpivot TP1-TP3 三列"。**用词精确**：(6-15) 修正"对齐"、"FLATTEN 后"、"kWh at fact"、"XS"、"被吞"、"按依赖顺序"等 10 处口语化或歧义表达。**重复内容删除**：(16-20) 删 §3.3.4 数据加载双模式（与附录 B 重复）、§3.3.5 简化为伪代码、§3.3.6 schema 列表引用化、§10.1 Proxy Price + DST 段去重，共省 ~40 行。**附录清理**：(21-24) 删 Hydro/EMI Forum URL；Makefile target 由 18 个削到 10 个；附录 C 注释 lookback_days 默认 vs re-run 关系。**一致性**：(25-28) "课程项目" → "ELT 实操项目"；术语 "TP 级"/"半小时" 关联；目录注释对齐配置文件实际内容 |
| **5.0** | **2026-05-11** | **Lead DE Bootstrap Round：Implementer-Ready**。修复 10 项 Day-1 阻塞点。**新增 §16 Implementer Bootstrap**：(1) Snowflake account + profiles.yml 凭据 onboarding 流程；(2) Mini POC 固定测试 fixture（指定日期 + 数据获取脚本）；(3) V1→V2 文件级改造边界（staging 改文件 / DAG 并存 / marts 不动）。**建模决策闭合**：(4) fct_price 列设计明确为 `date_key` + `trading_date` 双列；(5) `mart_price_daily` 取消 Phase 1.8 占位，Phase 2 一次到位含 island/region；(6) `int_price_daily` grain 改为 `POC × date × tp`（与 generation 对齐，支持 spike join）；(7) lookback SQL 模式明确为方案 A（从 `MAX(trading_date)` 倒推），含完整 SQL；(8) `dim_date.season` 按 NIWA 气象季定义（NZ 南半球：12-02 summer 等），加 `season_year` 完整规则。**Spec 完整化**：(9) `--months N` 语义 = 最近 N 个完整月（不含当月）；(10) `stg_nsp` 直接信任 NSP 原值，不做 region 标准化（避免维护白名单）。**次要清理**：(11) §8 目录补登记 `poc_match_rate.sql`、`fct_generation_monthly_min_rows.sql`；(12) `check_bymonth_or_daily` 明确 Azure Blob HEAD 404 处理；(13) §14 兼容性条款明确 `dim_plant.poc_code` 来源 V1 raw；(14) §15.1 CI Job B 改为 "在 `continue-on-error: true` + 失败时 issue comment"，避免 GitHub Actions 缺乏 post-merge soft-fail 的问题；(15) §8 + Phase 4 新增 `Dockerfile` 更新任务（dbt-duckdb + 新 Python deps） |
| **5.1-impl** | **2026-05-11** | **Phase 0 Implementation Done**（Lead DE Bootstrap 全部 8 子任务通过）。**交付**：(a) `pyproject.toml` 加 dbt-core/dbt-snowflake/dbt-duckdb/duckdb/pandas/pyarrow 等本地依赖；(b) `dbt/profiles.yml` + `dbt/profiles.yml.example` 改为 `dev=DuckDB / prod=Snowflake / ci=dummy SF` 三 target；(c) 三个 cross-db macros：`unpivot_trading_periods`、`generate_date_spine`、`day_of_week`；(d) `scripts/download_generation.py`、`scripts/download_price.py`（含 `--months/--years/--year-month` flag，daily fallback 留 Phase 1.1）；(e) `scripts/load_local.py`（DuckDB 事务式 DELETE+INSERT，幂等）；(f) `scripts/mini_poc_fixture.py` — **PASS：202401 两日切片 489 行 SF/DuckDB 完全相同**；(g) `stg_generation.sql` 内联 FLATTEN → macro 调用，输出 schema 不变（DuckDB 实跑 111,024 行）。**PRD 外发现的隐含 bug 顺手修复**：(h) `sources.yml` database 硬编码 → `{{ target.database }}` + schema 按 target 切换；(i) `stg_generation_null_audit.sql` 用 SF-only FLATTEN → 内联 target-aware unpivot；(j) 4 个 V1 mart 的 incremental 分支用 `TO_CHAR/DATEADD/TO_DATE` + `incremental_strategy='merge'` → 新增 `yyyymm_minus_one_month` macro + 改 `delete+insert`（SF/DuckDB 都支持，schema 不变）。**实测发现 vs PRD**：fct_generation 10 年估算 ~13M 行 / ~0.4GB（远低于 PRD §9 预估的 50-140M，原估算可能 over-counted；DuckDB 内存上限非约束）。**Makefile** 收敛至 7 个 target；**.gitignore** 加 `*.duckdb / *.duckdb.wal / *.parquet`。**端到端**：`dbt run` + 二次 run（exercise incremental）双过；`dbt test` 46/47 PASS，1 WARN = V1 既存 `unique_dim_plant_gen_code` 数据质量问题（与本期改动无关）。**已知运维细节**：`.env` 的 `SNOWFLAKE_PRIVATE_KEY_PATH` 指向容器内路径 `/opt/airflow/secrets/snowflake_rsa_key.p8`，host 跑 dbt/Python 时需覆盖为 `~/.ssh/snowflake_rsa_key.p8`（不应改 .env，否则 Airflow 容器内会断） |
| **5.7-impl** | **2026-05-12** | **Phase 5 Implementation Done — Observability Tier 1 全交付 + Tier 2 cost mart**（7 子任务全过）。**交付**：(a) `scripts/ingest_dbt_artifacts.py` 解析 `target/run_results.json` 写入 raw 层（DuckDB INSERT / SF MERGE 双路径，幂等 by invocation_id）；(b) `stg_dbt_run`（typed view）+ `fct_dbt_run`（每 invocation×node 一行 + `invocation_rank_desc` 便于"最近 N 次"查询）；(c) `mart_warehouse_cost` 读 SF `ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY + QUERY_HISTORY`，grain = day × warehouse，含 `usd_estimated = credits × var('snowflake_usd_per_credit', 2.0)`，`enabled=(target.type=='snowflake')`；(d) 第 10 个 Streamlit 页 `pages/pipeline_health.py` 含 4 KPI（freshness / model success 30d / test pass 30d / invocations tracked）带 ✅/❌ SLO badge + 日成功率 line + top-10 中位 runtime + 最新非 pass 表 + SF cost panel；(e) v2 DAG 加 `slack_alert` on_failure_callback（SLACK_WEBHOOK_URL 未设则降级 email_on_failure，URL 设置则 POST 含 dag_id/task_id/exception/log_url 的 block payload）+ `ingest_dbt_artifacts` 任务（`trigger_rule=ALL_DONE` 保证失败也记录）；(f) README 加 SLO 表（freshness ≤7d / model 30d ≥95% / test 30d ≥99% / cost informational）+ Observability 章节；(g) Makefile demo/local-full/local-subset 三 target 加 ingest 步骤。**测试**：raw_dbt_run / stg_dbt_run / fct_dbt_run / mart_warehouse_cost 全部加 schema.yml 测试 + fct_dbt_run 加 `unique_combination_of_columns`；v2 DAG 测试加 `ingest_dbt_artifacts` 入 expected task set。**端到端**：DuckDB `dbt run` 23/23 PASS（23 model = V2 21 + 2 新 dbt-run mart；mart_warehouse_cost SF-only 跳过）、`dbt test` 119/120 PASS（同 V1 既存 1 WARN）；ingest 跑通 invocation 持久化 120 行；6/6 DAG test PASS（V1 7-task + V2 15-task，新增 ingest）；Streamlit `/_stcore/health` ok。**未做项明确**：(1) Tier 2 剩余项 = S3 存储 mart + 行数/价格 drift 检测 + dbt docs CI → GitHub Pages（推 V3）；(2) Tier 3 = OpenTelemetry / 专业 obs 栈 / formal SLO burn rate（仅叙事，不实现） |
| **5.6-spec** | **2026-05-11** | **Observability Gap Analysis & Tiered Roadmap**（spec-only，无代码改动）。**问题陈述**：原 §13 实际是"告警 + dbt 数据质量测试"，未覆盖历史趋势、跨次运行回归检测、成本可见性、SLO 定义、跨系统 trace。**新增 §13.4** 含 gap 表（10 项观察维度，4 项已做 / 6 项未做）+ 三 tier 路线图：**Tier 1**（V2.x，~1.5 天）= dbt artifact 持久化到 S3 → `fct_dbt_run` mart → 第 10 个 Streamlit "Pipeline Health" 页 + Slack webhook + SLO 文档化；**Tier 2**（V3，~2-3 天）= Snowflake `ACCOUNT_USAGE` 成本 mart + S3 存储 mart + 行数 / 价格 drift 异常检测 + dbt docs CI 部署到 GitHub Pages；**Tier 3**（仅叙事）= OpenTelemetry 端到端 trace、专业 obs 后端、formal SLO/error budget。**第一性原理论证**：Tier 1 是"现在这次 run 对不对" → "近 30 天趋势是否退化" 的分水岭，ROI 高因数据源（dbt artifacts）本来就在生成只是没存，复用 Streamlit + dbt mart 不引入新栈。§11 Out of Scope 加 3 个 Tier 引用条目 |
| **5.5-impl** | **2026-05-11** | **Phase 4 Implementation Done**（Airflow DAG + Docker + CI + 文档 7 子任务全过；完成 V2 全栈交付）。**交付**：(a) `airflow/dags/nz_electricity_v2.py` 新增并存于 V1：3 个并行 ingest 分支（generation/price/NSP）+ `check_run_dbt` 短路 + `run_dbt`/`run_dbt_tests`（trigger_rule=NONE_FAILED 保证 NSP 失败不阻塞 dbt）。**关键实现**：(b) V1 callables 不能直接 import（会触发 `AirflowDagDuplicatedIdException`，因为顶层 `with DAG(...)` 会重新实例化 V1 DAG），改为 **内联复制**到 v2 文件（pragmatic trade-off，PRD §16.3 "V1 frozen" 守住，drift 风险低）；(c) price/NSP 分支调用 `scripts/download_price.py`、`scripts/load_snowflake_price.py`、`scripts/download_nsp.py` 等 Phase 1/2 已有模块，复用本地 CLI 工具。**Docker**：(d) `requirements-airflow.txt` 加 `dbt-duckdb==1.8.4 / requests / pandas / duckdb`；Dockerfile 无需改动（自动拾取）。**Terraform**：(e) `snowflake.tf` 加 `snowflake_table.raw_price`（6 列 VARCHAR）+ `snowflake_table.raw_nsp`（28 列 VARCHAR + ts，用 `dynamic "column" for_each` 简化定义）。**CI**：(f) `.github/workflows/ci.yml` 拆 5 个 job：`lint-python` / `dbt-parse-dev`（DuckDB，PR 安全）/ `lint-sql`（sqlfluff jinja）/ `dbt-parse-prod`（仅 main push，SF dummy creds）/ `pytest`（DAG integrity）；fork PR 不再需要 SF creds 即可跑 CI 主要部分。(g) `tests/test_dag_integrity.py` 加 `test_v2_dag_task_set`（14 task）+ `test_v2_branch_join`（3 branch → check_run_dbt 收束）。**文档**：(h) `README.md` 全面重写（V1→V2 表格、双模式对比、quick-start、9 page dashboard、Phase commit 历史指针）；(i) `docs/runbook.md` 加 §0 V1→V2 cutover/rollback checklist + §0.1 6 个常见双模式 troubleshoot 错误表。**端到端**：DuckDB 全量 `dbt run` 21/21 model PASS、`dbt test` 111/112 PASS（1 WARN = V1 既存）；SF parse + 双 target 全过；`pytest tests/` 6/6 PASS（V1 + V2 DAG bag 无 import error 且 14 task wiring 正确）；Streamlit headless boot `/_stcore/health = ok`。**累计 V2 项目状态**：23 dbt models（21 在 DuckDB）+ 4 cross-db macros + 9 streamlit pages + 2 DAG + 6 scripts；DuckDB 测试 112 tests / SF parse 全过。**留给 V3**：(1) NZTM → WGS84 lat/lng 反投影（需 pyproj）；(2) Hydro Modelling 数据源；(3) IsProxyPriceFlag 若 EMI 重新发布 → 切换 stg_price 的 placeholder 为真实值（已留 `is_proxy` 列接口） |
| **5.4-impl** | **2026-05-11** | **Phase 3 Implementation Done**（Dashboard 升级 5 子任务全过）。**交付**：(a) `streamlit/loader.py` 重构为双模式：`NZEG_MODE=local` 走 DuckDB read_only（避免与 dbt 写锁冲突，自动从 streamlit/ 子目录回溯到仓库根定位 `.duckdb` 文件）；`NZEG_MODE=cloud` 走 Snowflake（沿用 V1 key-pair auth）；保留所有 V1 `load_monthly/load_renewable/load_ranking/load_seasonal/load_monthly_raw` 函数签名；新增 `load_price_daily/load_price_spikes/load_renewable_price_impact` 三个 V2 loader；schema 名按 mode 派发（local: `main_analytics/main_raw`, cloud: `RAW_ANALYTICS/RAW_RAW`）。(b) `streamlit/app.py` 导航重组为分组：**Generation (V1)**（5 V1 页面）+ **Wholesale Price (V2)**（4 新页面）；sidebar 加 mode badge `🟢 Local / ☁️ Cloud`。(c) `pages/price_overview.py`：5-metric KPI strip（POC-days / avg / peak / spike-TPs / negative-TPs）+ daily mean by island line chart + price distribution histogram + spike/negative TP density bar charts；island & date range filters。(d) `pages/price_spikes.py`：4-metric KPI（spike events / distinct POCs / peak price / **unmatched-gen %**）+ unmatched-gen 警告条（当 >50% 时显示"load-only POC 解释"）+ day×TP spike density heatmap + top-20 POC 条形图 + 匹配 POC 的 fuel mix area chart。(e) `pages/renewable_price.py`：4-metric KPI 含 **Pearson r** + renewable-band bar chart（6 个 band: 0% / 1-24% / 25-49% / 50-74% / 75-99% / 100%）+ scatter（8k sampled）；caption 强调非单调关系。(f) `pages/island_spread.py`：4-metric KPI 含"spread % of SI" delta + NI/SI daily line + spread bar chart（正负不同色）+ spread distribution histogram；caption 解释 HVDC Cook Strait link 对 spread 的影响。**端到端**：DuckDB 模式 streamlit headless 启动 `/_stcore/health` 返回 `ok`；4 个新 page AST 语法 OK；3 个 loader 返回正确列结构（已验证）。**实测发现**：(1) 2024-01 mart_price_daily 中 ~10% 的 POC-day 没有 island 信息（NSP 未覆盖），filter 端用 `dropna(subset=['island'])` 优雅处理；(2) unmatched-generation 在 spike 视图中达 79%（与 Phase 2 一致），UI 用 warning 条 + caption 说明这是网络节点性质而非数据问题 |
| **5.3-impl** | **2026-05-11** | **Phase 2 Implementation Done**（NSP + dim_node + Cross-source Marts 9 子任务全过）。**重大 PRD 偏差与处理**：(1) NSP 文件实际是**每日 dated CSV**（`{YYYYMMDD}_NetworkSupplyPointsTable.csv`）而非单一静态文件；新 `scripts/download_nsp.py` 探测过去 14 天回溯，保存为稳定文件名 `NetworkSupplyPointsTable.csv`。(2) NSP 坐标实际为 **NZTM**（NZ Transverse Mercator）`easting/northing`，非 PRD §2.3 声称的 lat/lng；保留为整数列 `nztm_easting/nztm_northing`，反投影到 WGS84 lat/lng 需 `pyproj`，推 V3。(3) NSP 文件**27 列**（PRD 概述只列出 9 个关键列），新增字段如 `Network reporting region ID`、`Embedded under POC code`、`SB ICP`、`MEP`、`Certification expiry` 等保留在 raw 层。(4) NSP 一个 POC 可对应**多个 NSP 行**（766 NSPs vs 725 distinct POCs）；dim_node 按 `(poc_code, start_date DESC, nsp_code ASC)` 排序后取 rn=1 确保 POC-grain 唯一。**交付**：(a) `scripts/download_nsp.py`（302 redirect 跟随 + 14 天回溯）；(b) `scripts/load_local.py` 扩展 `load_nsp`（小文件全量重载，所有列 VARCHAR）；(c) `dbt/models/staging/stg_nsp.sql`（信任原值，只 lowercase+trim+`Current flag='1'` 过滤；725 当前 POCs）；(d) `dbt/models/core/dim_node.sql`（POC-grain Type 1，含 island/region/zone/network_participant + NZTM 坐标）；(e) `dbt/seeds/nz_public_holidays.csv`（2016-2030 共 159 个 NZ 法定假日，含 Matariki 2022+）；(f) `dbt/models/core/dim_date.sql`（自建用 `generate_date_spine` macro，5478 行；NIWA 南半球季节 + `season_year` 滚动 12 月归下一年；`day_of_week` macro ISO 1-7；`is_weekend`/`is_nz_holiday` LEFT JOIN seed）；(g) `dim_fuel.sql`（fuel_codes seed 升级为 fuel_type-grain dim，9 行；`BOOL_OR(is_renewable)` 改用 `MAX(CASE...)=1` 跨 SF/DuckDB）；(h) `dbt/models/intermediate/int_generation_by_poc.sql`（plant 聚合到 POC×date×tp×fuel；从 stg_generation 取因 fct_generation V1 frozen 不含 poc_code；107,856 行，与 stg_generation SUM 完全 reconciliation）；(i) `mart_price_daily.sql`（incremental delete+insert + lookback_days；POC×date 粒度含 island/region；avg/min/max/std + spike_tp_count + negative_tp_count；202401 实测 7,440 POC-days，NI 均价 $192.46 vs SI $190.61）；(j) `mart_price_spike_events.sql`（grain POC×date×tp 含 fuel mix；29,802 spike events，发现 **unmatched_generation 率 79%** —— 因 240 个 price POCs 大部分是 load 节点，无 generation；非数据质量问题，是网络节点性质）；(k) `mart_renewable_price_impact.sql`（INNER JOIN gen-share 与价格；98k 行；发现 100% renew POC 均价 $187.6，50-74% renew POC 均价 $211 —— 非线性关系）。**测试**：(l) `schema.yml` 加 8 个新模型测试 + 4 个 `unique_combination_of_columns` 复合 grain 测试；(m) 2 个 singular tests：`test_int_generation_by_poc_reconciliation`（与 stg_generation SUM 一致）、`test_poc_match_rate`（fct_price → dim_node 匹配率 < 80% 时 fail，实测 90% 通过）。**端到端**：DuckDB 全量 21/21 model + 111/112 test PASS（同 V1 既存 1 WARN）；SF parse + 8 个 Phase 2 模型 compile 全过。**实测发现 vs PRD**：(1) PRD §5.3 "POC 匹配率门控" 原指 generation→price 匹配，实际应为 price→dim_node 匹配（generation 自带 POC，不需要 NSP 桥接，§5.3 已有此 V3 修正）；(2) 价格 POC 中 80% 是 load 节点（无 generation），spike 事件多发生在 load 节点，dashboard 文案应注明 "spike 反映 load 端价格压力，非 generator 端"|
| **5.2-impl** | **2026-05-11** | **Phase 1 Implementation Done**（Final Energy Prices 集成 9 子任务全过）。**重大 PRD 偏差与处理**：(1) 实测 EMI Final Energy Prices CSV 只有 **4 列**：`TradingDate, TradingPeriod, PointOfConnection, DollarsPerMegawattHour`——PRD §2.3 声称的 7 列（含 Island / IsProxyPriceFlag / PublishDateTime）**在实际文件中不存在**。处理：stg_price/fct_price/int_price_daily 维持原列设计，但 `is_proxy` 退化为 `FALSE` 占位（保持下游 `avg_price_non_proxy` 语义可重用），`island` 字段从 fct 层去掉留待 Phase 2 通过 `dim_node` LEFT JOIN 注入；`pricing_regime` 由 `trading_date < var('pricing_regime_cutover')` 派生（不再依赖 PublishDateTime）。(2) 实测 ByMonth 归档**仍覆盖当月**（202604 HTTP 200），PRD `BYMONTH_CUTOFF=2024-12` 触发条件无意义；保留 `--bymonth-cutoff` 参数但默认 `9999-12`，HTTP 404 时仍回退到 daily-stitch。**交付**：(a) `scripts/download_price.py` ByMonth + daily-stitch fallback；(b) `scripts/validate_price.py` 4-列 strict schema + 月份完整性检查；(c) `scripts/load_local.py` 扩展 raw_price 加载，202401 实跑 357,120 行；(d) `scripts/load_snowflake_price.py` 提供 SF COPY INTO 函数供 Phase 4 v2 DAG 调用；(e) `dbt/models/staging/stg_price.sql`（含 pricing_regime + is_proxy 占位 + 去重）；(f) `dbt/models/staging/stg_price_outlier_audit.sql`（正向 spike + 负价 view，202401 实测 29,802 spike / 0 negative）；(g) `dbt/models/core/fct_price.sql`（incremental delete+insert + 方案 A lookback：`COALESCE(MAX(trading_date), '1900-01-01') - lookback_days` via `dbt.dateadd`，跨 SF/DuckDB），`date_key = EXTRACT(YEAR)*10000 + ... + EXTRACT(DAY)` 替代 SF-only `TO_CHAR`；(h) `dbt/models/intermediate/int_price_daily.sql`（grain POC×date×tp + 窗口聚合 avg_price_all/non_proxy）；(i) `schema.yml` + 1 singular reconciliation test（`fct_price` 行数 == `raw_price` 去重行数）；dbt_project.yml 加 `price_spike_threshold/negative_price_threshold/bymonth_cutoff/lookback_days/pricing_regime_cutover` 5 个 var；intermediate 子目录注册。**端到端**：DuckDB 全量 `dbt run` 13 model PASS、`dbt test` 72/73 PASS（同 V1 既存 1 WARN）；SF 端 `dbt parse` + 4 个 Phase 1 模型 `dbt compile` 全过，实际 run 留 Phase 4 DAG 落地后（raw_price 表 SF 侧尚未填）。**实测发现 vs PRD**：raw_price 单月 357k 行 → 10 年估算 ~43M 行；POC 数 240（PRD 估算合理）；价格平均 $192/MWh（高于 PRD 预设 NZ 正常 $50-150，可能反映 2024 年高电价场景）|

---

## 1. 项目概览

### 1.1 背景与动机

V1 项目（NZ Electricity Generation Pipeline）构建了一条从 EMI 网站抓取月度发电 CSV → S3 → Snowflake → dbt → Streamlit 的完整 ELT 管道。项目在技术完整性上已覆盖 Airflow 编排、dbt 维度建模、Snowflake 事务加载、Terraform IaC 和 CI/CD。

**V1 的不足**：

- 单一数据源（generation），业务分析维度有限，无法回答市场行为类问题
- dbt lineage 是单源线性结构，缺少 cross-source join 和多事实表的建模复杂度
- 缺少 event-driven 分析（如价格尖峰检测），全部是固定维度的聚合报表
- 无本地运行模式，评审者需要 Snowflake 账号才能验证

### 1.2 V2 目标

将项目从"NZ 发电数据可视化"升级为 **"NZ 电力批发市场多源分析平台"**，通过引入 3 个额外数据源（价格、水文、节点映射），实现：

- dbt 从单事实表变为 **多事实表星型模型 + cross-source mart**
- 分析从静态聚合升级为 **事件检测 + 因果分析**（TP 级 generation × price join）
- 新增 **本地/云端双运行模式**，降低开发和评审门槛

### 1.3 面试叙事定位

> "V1 是端到端 ELT 实操项目，验证了管道能力。做完之后我意识到真实场景需要多源异构数据融合、更复杂的维度建模和事件驱动分析。V2 保持相同技术栈，但在业务深度和工程成熟度上做了本质升级——generation 和 price 在 POC × trading period（TP，每个 TP = 30 分钟）级别精确 join，可以做到 TP 级（半小时粒度）的市场行为分析。"

---

## 2. 数据源清单

### 2.1 现有数据源（V1 保留）

| 数据源 | 粒度 | 格式 | 周期 | 规模 |
|--------|------|------|------|------|
| EMI Generation_MD | plant × POC × trading_period × **date** | 宽表 CSV（57 列），按月打包 | 月度文件，日度行 | 需在 Phase 0 精确计数（FLATTEN 后预估 50-140M 行） |

> ⚠️ **V3 修正**: V1 PRD 曾将 generation 粒度误记为"月度"。EMI 官方说明确认 `Trading_date denotes the date on which the injections occurred`——每行是一天的数据。文件按月打包但内部是日度粒度。此外，每行包含 `POC_Code` 列，可直接关联 price 数据。

### 2.2 新增数据源

| # | 数据源 | 粒度 | 格式 | 下载方式 | 规模估算 | 优先级 |
|---|--------|------|------|----------|---------|--------|
| DS-1 | **Final Energy Prices** | node × trading_period × day | 长表 CSV（7 列） | Azure Blob（ByMonth 归档） | **~30M 行**（10 年） | P0（必须） |
| DS-2 | **NSP Table（Network Supply Points）** | 一次性参考表 | CSV | EMI Datasets 下载 | ~2,500 行 | P0（必须） |

> **V4.2 减法**: 原 DS-3 Hydro Modelling Dataset 推迟到 V3。理由：(1) 工作量 3 天 + Schema 调研风险；(2) 仅服务 Q10 一个 dashboard 页面；(3) catchment→island 映射本身是手工近似；(4) Cross-source 叙事核心已由 generation × price 撑住。Hydro 单独作为 V3 范围更合适，"V2→V3 加入水文因果链"也是更连贯的 narrative。

### 2.3 各数据源详情

#### DS-1: Final Energy Prices

- **URL 模式**: `https://emidatasets.blob.core.windows.net/publicdata/Datasets/Wholesale/DispatchAndPricing/FinalEnergyPrices/ByMonth/YYYYMM_FinalEnergyPrices.csv`
- **Schema**: `TradingDate`, `TradingPeriod (通常 1-48；DST 日 1-46 或 1-50)`, `PublishDateTime`, `PointOfConnection`, `Island`, `IsProxyPriceFlag (Y/N)`, `DollarsPerMegawattHour`
- **注意事项**:
  - **定价机制变更**: 2022-11-01 前使用 ex-post final pricing（事后定价）；之后切换为 real-time pricing（实时定价，先发布 interim，约 1 工作日后修正为 final）。stg_price 必须添加 `pricing_regime` 列标记，**跨该日期的时间序列分析需按 regime 分段说明，不能简单比较平均值**
  - **ByMonth 截止年份**: ByMonth 归档可能只到 2024 年底。2025+ 需要按日文件下载后合并。download_price.py 需要 `BYMONTH_CUTOFF` 配置参数
  - Proxy price 行需标记但保留（不过滤）
  - **负电价**: NZ 市场可出现负价（可再生过剩时），保留不过滤，在 audit 层监控
  - **时间窗口**: EMI 数据从 1996-10 起可用。**V2 选择 2016-01 作为统一起点**——这是 generation 和 price 两侧都有稳定数据的最早合理起点（早期年份 schema 变化大、节点合并频繁）

#### DS-2: NSP Table

- **来源**: EMI Wholesale → Mappings and Geospatial → Network Supply Points Table
- **作用**: 维度参考表，提供 POC → Region / Island / Network / Zone / 坐标（lat/lng）映射
- **刷新频率**: 不定期更新，Airflow 中设为每月拉取最新版
- **关键列**: `NSP`, `PointOfConnection`, `Island`, `Region`, `Zone`, `Network`, `Latitude`, `Longitude`, `ReconciliationType`

> ⚠️ **V3 修正**: NSP Table 的首要作用从"桥接 generation 和 price"降级为"提供 region/island/坐标等维度属性"。桥接功能由 Generation_MD 自带的 `POC_Code` 直接完成。

#### 辅助验证数据源（不纳入管道）

- **LoadGenerationPrice**: EMI 日度 CSV，含 Load/Generation (MW) + Price ($/MWh) per POC × TP。可用于交叉验证 fct_generation × fct_price 的 join 结果。不纳入正式管道，因为缺少 fuel_code 维度。

---

## 3. 双运行模式设计

### 3.1 设计原则

- 开发者（或面试官）克隆项目后应能在 **无 Snowflake 账号** 的情况下完整运行管道并查看 dashboard
- 切换到云端模式只需修改环境变量，不改代码
- **Schema-as-contract**: 每个数据源的 validate 脚本做 strict schema 验证（精确列名 + 列数 + 类型），不匹配则主动 fail，不静默处理
- **幂等性保证**: 本地模式和 Cloud 模式均使用 DELETE-INSERT per month 的事务式加载，多次执行不产生重复数据
- **Secrets 管理**: Local 用 `.env`（gitignore'd）；Cloud Airflow 用 Connections + Variables；CI 用 GitHub Secrets 注入到 `dbt compile --target prod` 步骤；任何地方都不在代码或 PRD 中出现明文凭据

### 3.2 模式定义

| 特性 | 🖥️ Local 模式 | ☁️ Cloud 模式 |
|------|--------------|--------------|
| 数据仓库 | DuckDB（本地文件） | Snowflake |
| 对象存储 | 本地 `data/raw/` 目录 | AWS S3 |
| 编排 | Makefile 命令行 | Airflow DAG |
| dbt profile | `dev`（DuckDB adapter） | `prod`（Snowflake adapter） |
| Dashboard 数据 | DuckDB 直连（read_only） | Snowflake 连接 |
| 数据量 | 可选子集（1 年 or 全量） | 全量 10 年 |
| 基础设施 | 无需 Terraform | Terraform 管理 |
| 适用场景 | 开发、调试、面试演示 | 生产仿真、全量测试 |

### 3.3 实现方案

#### 3.3.1 dbt dual-profile

```yaml
# dbt/profiles.yml.example  ← 提交到 Git（安全，仅含占位符）
# 实际使用：cp dbt/profiles.yml.example dbt/profiles.yml，填入真实值
# dbt/profiles.yml           ← 加入 .gitignore（含真实密码）
nz_electricity:
  target: dev  # 默认 local 模式
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('NZEG_DUCKDB_PATH', '../data/nzeg.duckdb') }}"
      schema: analytics
      threads: 4
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'NZ_ELECTRICITY') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'TRANSFORM_WH') }}"
      schema: analytics
      threads: 4
```

#### 3.3.2 SQL 兼容性策略

| 功能 | Snowflake | DuckDB | 策略 |
|------|-----------|--------|------|
| TP UNPIVOT | `LATERAL FLATTEN(ARRAY_CONSTRUCT(...))` | `UNNEST([...])` | dbt macro `unpivot_trading_periods`（**封装完整 CTE**，含 SELECT + FROM） |
| COPY INTO | `COPY INTO ... FROM @stage` | 不适用（Python `read_csv_auto`） | 加载脚本分支 |
| METADATA$FILE_LAST_MODIFIED | Snowflake 特有 | Python `os.path.getmtime()` | 加载脚本分支 |
| Date spine | `GENERATOR(ROWCOUNT => N)` | `generate_series(start, end, interval '1 day')` | dbt macro `generate_date_spine` |
| Date 函数 | `DATEADD`, `DATEDIFF` | `+/-` interval, `date_diff` | dbt macro 封装 |
| DOW | `DAYOFWEEK(date)` | `EXTRACT(DOW FROM date)` | dbt macro `day_of_week` |

#### 3.3.3 核心 cross-db macro

> ⚠️ **V3 修正**: macro 从"只封装 FROM 子句"改为"封装完整 CTE（SELECT + FROM）"，解决 Snowflake `f.value:tp::INT` vs DuckDB `unnested.tp` 的列引用语法差异。
>
> ⚠️ **V4 修正（DST）**: NZ 使用 NZST/NZDT（UTC+12/+13），每年两次 DST 切换：春季拨快某天只有 **46 个 TP**，秋季拨慢某天有 **50 个 TP**。`tp_columns` 调用方必须传入 **TP01–TP50 共 50 列**（DST 日中不存在的 TP 列值为 NULL，WHERE 过滤掉即可）。stg 层测试范围应为 1–50，而非 1–48。

> ⚠️ **V4.1 修正**: 输出值列名参数化（`value_col_name`，默认 `value`）。仅用于 **Generation_MD 宽表**（TP01-TP50 共 50 列）。Final Energy Prices 已是长表，不需要此 macro。

```sql
-- macros/cross_db/unpivot_trading_periods.sql
-- 适用范围：仅 Generation_MD 等"TP 列横向铺开"的宽表。
-- tp_columns must always include TP01-TP50 (50 entries) to cover DST.
{% macro unpivot_trading_periods(relation, tp_columns, id_columns, value_col_name='value') %}
  {% if target.type == 'snowflake' %}
    SELECT
      {% for col in id_columns %}{{ col }}, {% endfor %}
      f.value:tp::INT AS tp_number,
      f.value:val::FLOAT AS {{ value_col_name }}
    FROM {{ relation }},
    LATERAL FLATTEN(
      input => ARRAY_CONSTRUCT(
        {% for tp in tp_columns %}
          OBJECT_CONSTRUCT('tp', {{ tp.index }}, 'val', {{ tp.col }})
          {% if not loop.last %}, {% endif %}
        {% endfor %}
      )
    ) AS f
    WHERE f.value:val IS NOT NULL
  {% elif target.type == 'duckdb' %}
    SELECT
      {% for col in id_columns %}{{ col }}, {% endfor %}
      unnested.tp AS tp_number,
      unnested.val AS {{ value_col_name }}
    FROM {{ relation }},
    UNNEST([
      {% for tp in tp_columns %}
        {tp: {{ tp.index }}, val: {{ tp.col }}}
        {% if not loop.last %}, {% endif %}
      {% endfor %}
    ]) AS unnested(tp, val)
    WHERE unnested.val IS NOT NULL
  {% endif %}
{% endmacro %}
```

**调用示例（仅用于 stg_generation.sql）：**

```sql
-- TP01-TP50 共 50 列（覆盖 DST 场景；缺失 TP 为 NULL 已被 WHERE 过滤）
{% set tp_cols = [] %}
{% for i in range(1, 51) %}
  {% do tp_cols.append({'index': i, 'col': 'tp' ~ '%02d' % i}) %}
{% endfor %}

{{ unpivot_trading_periods(
     ref('raw_generation'),
     tp_cols,
     ['site_code', 'poc_code', 'gen_code', 'fuel_code', 'trading_date'],
     value_col_name='generation_kwh'
   ) }}
```

> stg_price 不使用此 macro：Final Energy Prices 已是 `(trading_date, trading_period, poc_code, ...)` 长表，stg_price 只做 type cast、`pricing_regime` 标记、`is_proxy` flag。

#### 3.3.4 命令入口

完整 Makefile target 速查见 **附录 B**。两种模式由环境变量 `NZEG_MODE=local|cloud` 切换，无代码分支。

#### 3.3.5 Streamlit 双连接

`streamlit/loader.py` 根据 `NZEG_MODE` 返回对应连接：local → `duckdb.connect(path, read_only=True)`（read_only 避免与 dbt 写锁冲突）；cloud → `snowflake.connector.connect(...)`，凭据从环境变量读取。完整实现见 `streamlit/loader.py`，约 20 行。

#### 3.3.6 Schema 验证策略

每个数据源的 validate 脚本做 strict 验证：精确列名 + 列数比对（schema 见 §2.3 各 DS 段），不匹配则抛 `SchemaValidationError`。同时做**月份完整性检查**：

```python
# 列名硬编码常量（与 §2.3 DS-1 一致；schema 变化时一处更新两边同步）
EXPECTED_PRICE_COLUMNS = [...]  # 见 §2.3 DS-1 Schema

def validate_price_schema(df):
    if list(df.columns) != EXPECTED_PRICE_COLUMNS:
        raise SchemaValidationError(f"Price schema mismatch: {set(df.columns) ^ set(EXPECTED_PRICE_COLUMNS)}")

def validate_price_month_completeness(raw_dir: str, start_ym: str, end_ym: str):
    """Fail loudly if any expected month's file is missing.

    Silent gaps in raw_price cause months with no price data — downstream
    marts show NULL price silently, which looks like a business anomaly.
    """
    expected = _generate_month_list(start_ym, end_ym)
    actual = {
        f[:6]  # YYYYMM prefix
        for f in os.listdir(raw_dir)
        if f.endswith("_FinalEnergyPrices.csv")
    }
    missing = set(expected) - actual
    if missing:
        raise DataCompletenessError(
            f"Missing price files for months: {sorted(missing)}. "
            f"Re-run: python scripts/download_price.py --months {','.join(sorted(missing))}"
        )
```

#### 3.3.7 本地加载幂等性

```python
def load_price_month(conn, month_str, csv_path):
    """Idempotent: DELETE existing month data, then INSERT from CSV."""
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(
            "DELETE FROM raw_price WHERE trading_date >= ? AND trading_date < ?",
            [f"{month_str}-01", next_month(month_str)]
        )
        conn.execute(
            f"INSERT INTO raw_price SELECT * FROM read_csv_auto('{csv_path}')"
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
```

---

## 4. 业务问题（升级版）

### 4.1 V1 保留问题（重构）

| # | 页面 | 问题 | 变化 |
|---|------|------|------|
| Q1 | Overview | NZ 电力市场当前状态总览？ | 新增价格 KPI |
| Q2 | Fuel Trends | 发电结构如何演变？ | 不变 |
| Q3 | Plant Ranking | 哪些电站贡献最大？ | 不变 |
| Q4 | Renewable Share | 可再生能源占比趋势？ | 不变 |
| Q5 | Seasonal Patterns | 季节性差异？ | 不变 |

### 4.2 V2 新增问题

| # | 页面 | 问题 | 数据源组合 | Join 粒度 |
|---|------|------|-----------|----------|
| Q6 | **Price Overview** | 批发电价的整体分布和趋势如何？正向 spike 和负价事件频率？ | Price + NSP | TP 级 |
| Q7 | **Price Spikes** | 价格尖峰（>$300/MWh）发生时，该 POC 的发电燃料结构是什么？ | Price + Generation (via POC_Code) | **TP 级精确 join** |
| Q8 | **Renewable vs Price** | 某 POC 在可再生注入占比高的 TP，电价是否显著更低？ | Generation + Price (via POC_Code) | **TP 级精确 join** |
| Q9 | **Island Spread** | 北岛和南岛之间的价差模式是什么？何时扩大？ | Price + NSP | TP 级（dashboard 直接在 mart_price_daily 上做 island 维度查询，区分 `avg_price_all` 和 `avg_price_non_proxy` 双指标；默认 non-proxy） |

> Q10（Hydro-Price Driver）随 DS-3 推迟到 V3。

---

## 5. 数据建模

### 5.1 Star Schema

```
                    ┌──────────────┐
                    │  dim_date    │
                    │──────────────│
                    │ date_key(PK) │
                    │ trading_date │
                    │ year/quarter │
                    │ month/dow    │
                    │ season       │
                    │ is_weekend   │
                    │ is_nz_holiday│
                    └──────┬───────┘
                           │
     ┌────────────┐   ┌────┴──────────┐   ┌──────────────┐
     │ dim_plant  │   │fct_generation │   │  dim_fuel    │
     │────────────│   │───────────────│   │──────────────│
     │ plant_key  │◄──│ plant_key(FK) │   │ fuel_key(PK) │
     │ site_code  │   │ fuel_key(FK)  │──►│ fuel_code    │
     │ gen_code   │   │ date_key(FK)  │   │ fuel_name    │
     │ plant_name │   │ poc_code      │   │ is_renewable │
     │ poc_code   │   │ tp_number     │   └──────────────┘
     │ island     │   │ gen_kwh       │
     └────────────┘   └───────────────┘

                      ┌───────────────┐
                      │  fct_price    │
                      │───────────────│
                      │ date_key(FK)  │  ← INT 代理键，join dim_date 用
                      │ trading_date  │  ← DATE，incremental filter + 退化属性
                      │ poc_code      │  ← 退化维度
                      │ island        │  ← 退化维度
                      │ tp_number     │
                      │ price_nzd_mwh │
                      │ is_proxy      │
                      │ pricing_regime│
                      └───────────────┘

  Cross-source 维度（仅 mart 层 join 使用，不在 fct 层做 FK）：

  ┌──────────────┐
  │  dim_node    │
  │──────────────│
  │ poc_code(PK) │
  │ nsp_code     │
  │ island       │
  │ region       │
  │ zone         │
  │ network      │
  │ latitude     │
  │ longitude    │
  └──────────────┘
```

> **建模决策（V4.1）**: `fct_generation` 和 `fct_price` 都用 `poc_code` 作为退化维度（字符串），不引入 `node_key` 代理键。理由：(1) `poc_code` 本身已是稳定字符串，无需代理键；(2) 避免 `fct_price` 对 `dim_node` 的硬 FK 依赖（dim_node 来自 NSP，单独失败不应阻塞 fct_price 构建）；(3) 需要 region/coordinates 时，在 mart 层 `LEFT JOIN dim_node ON poc_code` 即可。

### 5.2 dbt Lineage

```
Sources
├── raw_generation (V1 — 月度文件，日度行，含 POC_Code)
├── raw_price (NEW — Final Energy Prices)
└── raw_nsp_table (NEW — NSP mapping)

Staging
├── stg_generation (table — unpivot_trading_periods macro, dedup, type cast)
│   └── stg_generation_null_audit (view — TP NULL 监控)
├── stg_price (table — type cast, pricing_regime flag, proxy flag)
│   └── stg_price_outlier_audit (view — 正向 spike + 负价监控)
└── stg_nsp (table — 清洗, 标准化 region 名)

Intermediate
├── int_generation_by_poc (table — SUM generation by POC × date × tp × fuel，消除 multi-plant-per-POC；物化为 table 避免多 mart 重触发 FLATTEN)
├── int_generation_daily_by_fuel (table — daily 发电量 by fuel type)
└── int_price_daily (table — grain: POC × date × tp；含 `price_nzd_mwh`、`is_proxy`、`avg_price_all`、`avg_price_non_proxy`。**注**：grain 与 generation 对齐为 TP 级，便于 mart_price_spike_events 在同一 int 上 join；mart 层做 daily 聚合（无需单独 daily-grain int）)

Dimensions
├── dim_date (table — 自建 macro + NZ 公共假日 seed，不依赖 dbt_date package)
├── dim_plant (table — Type 1 SCD, composite key, 含 POC_Code)
├── dim_fuel (seed → table — fuel code → name → is_renewable)
└── dim_node (table — from NSP，POC/region/island/coordinates)

Facts
├── fct_generation (table — plant × POC × tp × date, generation_kwh)
└── fct_price (table — POC × tp × date, price_nzd_mwh)

Marts (V1 保留)
├── mart_generation_daily (incremental)
├── mart_generation_monthly (incremental)
├── mart_renewable_ratio (incremental)
├── mart_plant_ranking (incremental)
└── mart_seasonal_pattern (table)

Marts (V2 新增)
├── mart_price_daily (incremental — daily 价格统计 by POC + island/region，含 all/non_proxy 双指标；Q6/Q9 都基于此 mart)
├── mart_price_spike_events (incremental — spike via POC join int_generation_by_poc)
└── mart_renewable_price_impact (incremental — renewable% vs avg_price per POC × TP)
```

> **V4.2 减法**：删除 `int_island_price`（与 int_price_daily 重复）、`mart_island_price_spread`（Q9 直接在 mart_price_daily 上 GROUP BY island 查询）、`mart_generation_price_monthly`（不对应任何 Q）、所有 hydro 链路（推 V3）。lineage 从 32 个 model 缩到 25。

### 5.3 关键建模决策

| 决策 | 选择 | 理由 |
|------|------|------|
| generation-price join key | `POC_Code × TradingDate × TradingPeriod` | Generation_MD 自带 POC_Code，可直接与 FinalEnergyPrices 的 PointOfConnection join。**无需 region 级近似**。 |
| 一个 POC 多个 plant | 在 fct 层保持 plant 粒度；在 `int_generation_by_poc` 聚合到 POC 级后再 join price | 避免 fanout。cross-source mart 的粒度是 `POC × date × tp`。 |
| POC 匹配率质量门控 | 在 `mart_price_spike_events` 构建后，dbt test 断言 `unmatched_poc_rate < 5%`；在 Dashboard Q7/Q8 页面展示 "Generation-Price join coverage: XX%" | Generation_MD 的 POC 命名规范（如 `HAY2201` vs `HAY2201G`）可能有历史漂移，低匹配率会使 Q7/Q8 分析失真；5% 阈值为 warn 级，>20% 为 error 级 |
| POC 随时间变化 | 使用 Generation_MD 当月行中的 POC_Code | 不做历史追踪（个人项目合理简化）。See Known Limitations. |
| price spike 阈值 | $300/MWh 正向 + $0 负向（可配置，dbt var） | NZ 市场正常 $50-150/MWh。通过 `vars:` 配置。 |
| pricing regime flag | stg_price 添加 `pricing_regime` 列 | 2022-11-01 前后定价机制不同 |
| 时区策略 | 所有 date 列（TradingDate、PublishDateTime）均以 **NZ 本地日期（DATE，无时区）** 存储；stg 层显式 `CAST(... AS DATE)` 去除任何时区偏移；不使用 TIMESTAMP WITH TIME ZONE | EMI 数据本身是 NZ 本地时间；Snowflake 默认 UTC，若 PublishDateTime 含时区标记而不显式转换，`CAST TO DATE` 在 NZDT 下可能跨天；`is_weekend` / `is_nz_holiday` 必须基于 NZ 本地日期计算 |
| dim_date 来源 | 自建 macro + NZ 公共假日 seed | 不依赖 dbt_date package，避免 DuckDB 兼容性风险 |
| fct_price incremental 策略 | `unique_key = [poc_code, trading_date, tp_number]` + `incremental_strategy = 'delete+insert'`，`lookback_days = 3` | Raw 层 DELETE-INSERT 会更新 raw_price，但 dbt incremental 如果只追加新 trading_date 会跳过已有日期的 final 修正。`delete+insert` + lookback 确保最近 3 天的行全部重算，覆盖 interim→final 窗口 |
| 价格月度 re-run | 每月 final 确认后（次工作日 2pm）触发 `dbt run --select fct_price+ --vars '{lookback_days: 32}'` | 确保当月所有 interim 行被最终 final 价格覆盖 |
| stg_generation 物化为 table | `materialized='table'` | LATERAL FLATTEN 把 50 列宽表展开为长表是重操作（50× 行数膨胀）；物化为 table 后，下游多个 mart 直接扫表，不重复触发展开 |
| 单位策略：fact 层 kWh / mart 层 GWh | fact 保留 raw 整数 kWh；mart 计算时 `/ 1000000.0` 转为 FLOAT GWh | kWh 与原始数据一致，便于 reconciliation 测试；GWh 是面向 dashboard 的可读单位（NZ 月度发电量在数百 GWh 级） |
| **fct_price 日期列设计** | 同时持有 `date_key INT`（dim_date 代理键）和 `trading_date DATE`（源列 + incremental filter 用） | 代理键给后续 BI 工具友好；trading_date 给 `delete+insert` strategy 的 WHERE 子句用，避免每次 join dim_date 反查日期 |
| **fct_price incremental SQL 实现** | 方案 A：`WHERE trading_date >= (SELECT COALESCE(MAX(trading_date), '1900-01-01') FROM {{ this }}) - INTERVAL var('lookback_days') DAY`（仅在 `{% if is_incremental() %}` 分支） | 从表内最大日期倒推 lookback。**理由**：raw 层 DELETE-INSERT 已确保 raw_price 是最新 final，dbt 层只需重算 "已有数据末尾 + lookback" 窗口。`COALESCE` 保护首次空表场景。**不用** `CURRENT_DATE`，因为 backfill 跑历史月份时 CURRENT_DATE 模式会漏数据 |
| **`mart_price_daily` 演进** | Phase 1 不建立简化版；Phase 2 在 dim_node 就绪后一次性建立完整版（含 island/region 维度） | 避免 Phase 1 / Phase 2 改两遍同一个 SQL。Phase 1 提前移除该 mart，工时挪到 Phase 2 |
| **dim_date.season 定义（NZ 南半球）** | `CASE WHEN month IN (12,1,2) THEN 'Summer' WHEN month IN (3,4,5) THEN 'Autumn' WHEN month IN (6,7,8) THEN 'Winter' ELSE 'Spring' END`；`season_year = CASE WHEN month = 12 THEN year+1 ELSE year END`（12 月归入下一年的 Summer，便于跨年 summer 连续聚合） | NIWA 气象季定义。Mart 层 `mart_seasonal_pattern` 使用 `season_year` 而非 calendar year |
| **`--months N` / `--years N` 语义** | "最近 N 个**完整**月/年（不含当月）"。N=1 即上个月；N=12 即过去 1 年。**不接受 0 或负值**（脚本启动时 argparse 校验） | 排除"当月还在进行中"的歧义；统一 generation 和 price 的 download 脚本行为 |
| **`stg_nsp` 不做 region 标准化** | 直接 `SELECT * FROM raw_nsp_table`，只做列名 lowercase + trim whitespace。`region` 字段值原样保留 | 避免维护 region 白名单；NSP 是 EMI 官方维度表，区域名称视为 source of truth。Dashboard 层若发现脏数据再添加规则 |
| **V1→V2 文件级改造边界** | (a) V1 `stg_generation.sql` **改文件**：内联 FLATTEN → macro 调用，模型名不变。(b) V1 DAG `nz_electricity_monthly.py` **并存**：保留作为 V1 兼容路径，V2 DAG 文件名 `nz_electricity_v2.py`。(c) V1 5 个 marts **不动**：不改 schema、不重写引用 int_generation_by_poc | V1 dashboard 在迁移期持续可用；V2 上线后可手动归档 V1 DAG（不进 PRD 范围）|

---

## 6. Airflow DAG 设计（Cloud 模式）

### 6.1 DAG 结构

> ⚠️ **V4.1 简化**: 单 DAG + `TriggerRule.NONE_FAILED` 替代双 DAG。Core 分支（Generation/Price）失败阻塞 dbt；Optional 分支（NSP）失败时，dbt_run 仍触发，使用上一次成功 load 的 dim_node 快照。
>
> ⚠️ **V4.2 减法**: 去掉 Hydro 分支（移至 V3）。

```
nz_electricity_v2 DAG（每月 15 日触发）

  ┌── Core: Generation ──────────┐     ┌── Core: Price ─────────────────┐
  │ download → validate → upload │     │ check_bymonth_or_daily →       │
  │   → load_sf                  │     │   download → validate →        │
  │                              │     │   upload → load_sf             │
  └──────────────┬───────────────┘     └────────────────┬───────────────┘
                 │                                      │
                 └──────────────┬───────────────────────┘
                                │  trigger_rule=ALL_SUCCESS
                                │  (Core 全成功才继续)
                                ▼
                 ┌── Optional: NSP ──────────────┐
                 │ download → validate (≥2000)   │
                 │   → load_sf                   │
                 └──────────────┬────────────────┘
                                │  trigger_rule=NONE_FAILED
                                │  (NSP 失败时 dbt 仍执行，复用旧 dim_node 快照)
                                ▼
                          dbt_run → dbt_test

  注：
  - Generation 与 Price 互相独立，并行执行
  - 都成功后才启动 NSP（也可与 Core 并行，但 NSP 极轻量，串行更清晰）
  - dbt_run 上游用 NONE_FAILED：Core 全成功 ⇒ 执行；NSP 失败 ⇒ 仍执行，用旧 dim_node
```

**`check_bymonth_or_daily` 伪代码**（Price 分支的第一个 task）：

```python
def check_bymonth_or_daily(year_month: str, bymonth_cutoff: str) -> dict:
    """决定 download 模式。返回 {'mode': 'bymonth'|'daily', 'urls': [...]}"""
    if year_month <= bymonth_cutoff:
        # 历史月份：单文件 ByMonth 归档
        return {'mode': 'bymonth',
                'urls': [f'{BLOB_BASE}/ByMonth/{year_month}_FinalEnergyPrices.csv']}
    else:
        # 当前/近期月份：遍历该月每一天的 daily 文件
        days = _days_in_month(year_month)
        urls = [f'{BLOB_BASE}/{year_month}{d:02d}_FinalEnergyPrices.csv'
                for d in range(1, days + 1)]
        return {'mode': 'daily', 'urls': urls}

# downstream download task: 对每个 URL 做 HTTP HEAD 请求
# - Azure Blob：文件不存在时返回 404，存在时返回 200 + Content-Length
# - 仅 status==200 的 URL 才放入实际下载队列；其余 (404 / 403 / 5xx) 跳过并记录到 XCom
# - daily 模式下，"当月未来日期" 的 404 是预期行为（数据还没发布），不告警
# - bymonth 模式下任何 404 都视为下游问题，触发 SchemaValidationError
```

### 6.2 调度策略

| 分支 | 触发规则 | 调度备注 |
|------|---------|---------|
| Core: Generation | `ALL_SUCCESS` | EMI 月度发布，延迟约 2 周；DAG 每月 15 日触发；与 Price 并行 |
| Core: Price | `ALL_SUCCESS` | 内部 `check_bymonth_or_daily` 分支选择；与 Generation 并行 |
| Optional: NSP | `NONE_FAILED`（在 dbt_run 上游） | 参考表，低频更新；失败时复用旧 dim_node 快照 |
| `dbt_run` | `NONE_FAILED` | Core 任一失败则不执行；NSP 失败则用旧 dim_node 继续 |
| `dbt_test` | `ALL_SUCCESS` | dbt_run 完成后立即测试 |

> 当月 price 数据在 final 确认后（通常为次工作日 2pm），通过 DAG run conf `{"price_rerun": true}` 触发一次 fct_price+ 的 `--vars '{lookback_days: 32}'` 重算，覆盖 interim → final 修正。

### 6.3 Pools 配置

| Pool | Slots | 用途 |
|------|-------|------|
| `emi_download_pool` | 2 | 控制对 EMI 网站的并发请求 |
| `dbt_pool` | 1 | 序列化 dbt run |

---

## 7. 分阶段实施计划

### Phase 0: 双运行基础设施 + Mini POC（5 天）

| 步骤 | 任务 | 交付物 | 耗时 |
|------|------|--------|-----|
| **0.0** | **Mini POC**: macro 内 `tp_columns` 只传 TP1-TP3 三列 + 取 2 天 generation 数据，在 Snowflake 和 DuckDB 上分别跑 `unpivot_trading_periods`，比对输出 row count + 行内容完全一致。**不通过不继续** | POC 脚本 + diff 报告 | **0.5 天** |
| 0.1 | 安装 `dbt-duckdb` adapter，配置 `profiles.yml.example` | profiles 配置 | 0.5 天 |
| 0.2 | 编写 cross-db macros：`unpivot_trading_periods`（参数化版，含跨方言验证）, `generate_date_spine`, `day_of_week` | `macros/cross_db/*.sql` | 1 天 |
| 0.3 | 将 stg_generation 改写为使用 macro，验证双运行输出一致 | 修改后的 `stg_generation.sql` | 0.5 天 |
| 0.4 | 编写 `scripts/load_local.py`（事务式 CSV → DuckDB） | `scripts/load_local.py` | 0.5 天 |
| 0.5 | **为现有 download_generation.py / download_price.py 添加 `--months N` 和 `--years N` 参数**（demo / subset 场景必需） | scripts diff | 0.5 天 |
| 0.6 | 更新 Makefile + .gitignore；精确计数 fct_generation 行数，评估 DuckDB 内存上限 | Makefile, 行数报告 | 1 天 |
| 0.7 | 端到端验证：`make local-full` 在无 Snowflake 环境完整运行 | 测试通过 | 0.5 天 |

### Phase 1: Final Energy Prices 集成（6.5 天）

| 步骤 | 任务 | 交付物 | 耗时 |
|------|------|--------|-----|
| 1.1 | Price download 脚本（**ByMonth + daily 双逻辑 + BYMONTH_CUTOFF 参数 + `--months/--years` flag**） | `scripts/download_price.py` | **1.5 天** |
| 1.2 | Price validate 脚本（strict schema check: 7 列精确匹配） | `scripts/validate_price.py` | 0.5 天 |
| 1.3 | 双模式加载（DuckDB 事务式 + Snowflake COPY INTO） | 加载脚本 | 0.5 天 |
| 1.4 | `stg_price`（type cast, pricing_regime, proxy flag） | model | 0.5 天 |
| 1.5 | `stg_price_outlier_audit`（**正向 spike + 负价** 双向监控） | model | 0.5 天 |
| 1.6 | `fct_price`（POC × tp × date，`date_key` + `trading_date` 双列，incremental delete+insert 方案 A，见 §5.3） | model | 1 天 |
| 1.7 | `int_price_daily`（grain: POC × date × tp，含 all/non_proxy 双指标） | model | 0.5 天 |
| 1.8 | dbt tests: not_null, unique, accepted_values, range, reconciliation | tests | 0.5 天 |
| 1.9 | 双模式全流程验证（暂不含 island/region；mart_price_daily 在 Phase 2 建） | 测试通过 | 1 天 |

### Phase 2: NSP + dim_node + Cross-source Marts（4.5 天）

| 步骤 | 任务 | 交付物 | 耗时 |
|------|------|--------|-----|
| 2.1 | NSP download + `stg_nsp`（不做 region 标准化，见 §5.3） | scripts + model | 0.5 天 |
| 2.2 | `dim_node`（POC → region/island/zone/coordinates） | model | 0.5 天 |
| 2.3 | `dim_date`（自建 macro + NZ 公共假日 seed，季节按 NIWA 气象季，见 §5.3） | model + seed | 0.5 天 |
| 2.4 | `dim_fuel`（从 seed 升级，加 is_renewable） | model | 0.25 天 |
| 2.5 | **`int_generation_by_poc`**（plant 聚合到 POC 级 by fuel） | model | 0.5 天 |
| 2.6 | `mart_price_daily`（一次性建立完整版，含 island/region；覆盖 Q6 和 Q9） | model | 0.75 天 |
| 2.7 | `mart_price_spike_events`（spike detection + join int_generation_by_poc） | model | 0.5 天 |
| 2.8 | `mart_renewable_price_impact`（renewable% vs price per POC × TP） | model | 0.5 天 |
| 2.9 | dbt tests + cross-source reconciliation | tests | 0.5 天 |

### Phase 3: Dashboard 升级（3 天）

| 步骤 | 任务 | 交付物 | 耗时 |
|------|------|--------|-----|
| 3.1 | 重构 `loader.py`（双连接 + DuckDB read_only） | loader.py | 0.5 天 |
| 3.2 | Price Overview 页面（含负价分布） | page | 0.5 天 |
| 3.3 | Price Spikes 页面 | page | 0.75 天 |
| 3.4 | Renewable vs Price 页面 | page | 0.75 天 |
| 3.5 | Island Spread 页面（直接查 mart_price_daily，按 island GROUP BY） | page | 0.5 天 |

### Phase 4: Airflow DAG + Docker + CI/CD + 文档（2.75 天）

| 步骤 | 任务 | 交付物 | 耗时 |
|------|------|--------|-----|
| 4.1 | 单 DAG `nz_electricity_v2`：Core 分支（Generation+Price 并行，ALL_SUCCESS）→ Optional NSP（NONE_FAILED）→ dbt_run | dag file | 0.5 天 |
| 4.2 | **Dockerfile 更新**（新增 dbt-duckdb + V2 Python 依赖到 `requirements-airflow.txt`，确认 `docker compose build` 通过） | Dockerfile, requirements-airflow.txt | 0.25 天 |
| 4.3 | Terraform 更新（新增 raw_price / raw_nsp_table S3 prefix） | tf files | 0.25 天 |
| 4.4 | CI 更新（fork PR job + main push job 分离，见 §15.1） | ci.yml | 0.5 天 |
| 4.5 | README V2 全面重写（含部署策略：local=DuckDB, cloud=Snowflake；最低硬件规格；Snowflake onboarding 见 §16） | README.md | 0.5 天 |
| 4.6 | runbook 更新（含 V1→V2 迁移步骤，见 §14） | runbook.md | 0.25 天 |
| 4.7 | 端到端全量测试（Snowflake + DuckDB 双模式） | 测试报告 | 0.5 天 |

### 工时总览

| Phase | 天数 | 累计 |
|-------|------|------|
| Phase 0 | 5 | 5 |
| Phase 1 | 6.5 | 11.5 |
| Phase 2 | 4.5 | 16 |
| Phase 3 | 3 | 19 |
| Phase 4 | 2.75 | 21.75 |
| **Buffer** | **3.25** | **25** |
| **总计** | | **25 天 / 5 周** |

---

## 8. 项目目录结构

```
nz-electricity-wholesale-market-pipeline/
├── .github/workflows/
│   ├── ci.yml                              # Ruff + SQLFluff + dbt compile (dev+prod) + pytest
│   └── dbt-docs.yml                        # dbt docs → GitHub Pages
├── .env.example                            # NZEG_MODE, BYMONTH_CUTOFF, 等
├── .gitignore                              # 含 dbt/profiles.yml, data/
├── Makefile                                # 双模式入口
├── Dockerfile                              # V2 需新增 dbt-duckdb；基础镜像不变（见 Phase 4.2）
├── docker-compose.yml
│
├── airflow/dags/
│   └── nz_electricity_v2.py               # 单 DAG：Core (Generation+Price 并行) + Optional (NSP, NONE_FAILED)
│
├── scripts/
│   ├── download_generation.py
│   ├── download_price.py                  # ByMonth + daily 双逻辑 + BYMONTH_CUTOFF
│   ├── download_nsp.py
│   ├── validate_generation.py             # strict schema check
│   ├── validate_price.py                  # strict schema check (7 列精确匹配)
│   ├── load_local.py                      # 事务式 DELETE-INSERT → DuckDB
│   └── load_snowflake.py                  # 事务式 DELETE + COPY INTO → Snowflake
│
├── dbt/
│   ├── dbt_project.yml                     # vars 完整列表见附录 C
│   ├── profiles.yml.example                # ← 提交到 Git；实际 profiles.yml 加入 .gitignore
│   ├── packages.yml                        # dbt_utils only（无 dbt_date）
│   ├── macros/cross_db/
│   │   ├── unpivot_trading_periods.sql    # 封装完整 CTE
│   │   ├── generate_date_spine.sql
│   │   └── day_of_week.sql
│   ├── seeds/
│   │   ├── fuel_codes.csv
│   │   └── nz_public_holidays.csv
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/
│   │   │   ├── stg_generation.sql
│   │   │   ├── stg_generation_null_audit.sql
│   │   │   ├── stg_price.sql
│   │   │   ├── stg_price_outlier_audit.sql  # 正向 spike + 负价
│   │   │   ├── stg_nsp.sql
│   │   │   └── _staging.yml
│   │   ├── intermediate/
│   │   │   ├── int_generation_by_poc.sql   # plant → POC 聚合
│   │   │   ├── int_generation_daily_by_fuel.sql
│   │   │   └── int_price_daily.sql         # 含 all/non_proxy 双指标
│   │   ├── core/
│   │   │   ├── dim_date.sql                # 自建（macro + seed）
│   │   │   ├── dim_plant.sql               # 含 poc_code
│   │   │   ├── dim_fuel.sql
│   │   │   ├── dim_node.sql
│   │   │   ├── fct_generation.sql
│   │   │   └── fct_price.sql              # unique_key=[poc,date,tp], delete+insert
│   │   └── marts/
│   │       ├── mart_generation_daily.sql
│   │       ├── mart_generation_monthly.sql
│   │       ├── mart_renewable_ratio.sql
│   │       ├── mart_plant_ranking.sql
│   │       ├── mart_seasonal_pattern.sql
│   │       ├── mart_price_daily.sql        # 含 island/region 维度，覆盖 Q6/Q9
│   │       ├── mart_price_spike_events.sql
│   │       └── mart_renewable_price_impact.sql
│   └── tests/
│       ├── reconciliation_gen_fct_mart.sql
│       ├── reconciliation_price_fct_mart.sql
│       ├── price_spike_threshold_sanity.sql
│       ├── poc_match_rate.sql                 # POC 匹配率阈值，详见 §13.2
│       └── fct_generation_monthly_min_rows.sql # 每月 ≥ 5000 行，详见 §13.2
│
├── streamlit/
│   ├── app.py
│   ├── loader.py                           # DuckDB (read_only) / Snowflake 双连接
│   ├── charts.py
│   └── pages/
│       ├── 01_overview.py
│       ├── 02_fuel_trends.py
│       ├── 03_plant_ranking.py
│       ├── 04_renewable_share.py
│       ├── 05_seasonal_patterns.py
│       ├── 06_price_overview.py
│       ├── 07_price_spikes.py
│       ├── 08_renewable_vs_price.py
│       └── 09_island_spread.py
│
├── terraform/
├── tests/
│   ├── test_dag_integrity.py
│   └── test_load_local.py
├── data/                                   # 所有子内容 .gitignore'd；仅展示运行时目录结构
│   ├── raw/                                 # CSV 落盘位置
│   └── nzeg.duckdb                          # local 模式数据仓库文件
└── docs/
    ├── runbook.md
    ├── PRD.md
    └── screenshots/
```

---

## 9. 性能预估

| 指标 | V1 | V2（预估） | 备注 |
|------|----|-----------|----|
| 总原始数据量 | ~100 MB | **~1.5-2 GB** | price ~1.2GB + generation（日度行暴增） |
| fct_generation 行数 | 需确认 | **50-140M**（FLATTEN 后） | Phase 0 任务：精确计数 |
| fct_price 行数 | N/A | **~30M** | |
| `dbt run --full-refresh`（Snowflake X-Small (XS) warehouse） | ~2 min | **~10-15 min** | fct_generation 行数增大 |
| Incremental 月度运行 | ~30 sec | **~2-3 min** | |
| **首次全量回填（10 年，XS warehouse）** | N/A | **~1-2 小时 / ~$15-30** | full-refresh 跑 fct_generation（140M）+ fct_price（30M）约消耗 1-2 credits；XS warehouse ≈ $2/credit。建议回填前临时切换到 Small (S) warehouse，可缩短至 ~20 min，总费用相近 |
| Snowflake 月度稳态 credits | < 0.5 | **< 2** | |
| DuckDB full-refresh（本地全量） | N/A | **~10-20 min** | 取决于内存（建议 ≥ 16 GB RAM）；推荐 `NZEG_LOCAL_YEARS=1` |
| `make demo` 端到端（1 月数据） | N/A | **~60 秒** | 现下 1 个月 CSV（~30s）+ load + dbt run + Streamlit 启动；无需 Git LFS |
| S3 存储 | ~100 MB | **~2 GB** | |
| 最低本地运行规格 | N/A | **16 GB RAM / 4 cores** | 全量 DuckDB（140M gen + 30M price）同时在内存中约需 12-14 GB；低于 16 GB 需限制 `NZEG_LOCAL_YEARS=2` 或更少 |

---

## 10. 风险与缓解

| 风险 | 影响 | 缓解策略 |
|------|------|---------|
| DuckDB-Snowflake SQL 不兼容 | 双模式 dbt run 失败 | Phase 0 Mini POC 先验证；所有差异封装在 macro 中；CI 跑 `dbt compile` both targets |
| EMI Price 数据 schema 变化 | stg_price 失败 | strict schema validation；fail-early |
| fct_generation 行数远超预期 | DuckDB 内存不足 | Phase 0 精确计数；提供 `NZEG_LOCAL_YEARS=1` 子集方案 |
| ByMonth 归档截止年份不确定 | download_price 逻辑出错 | `BYMONTH_CUTOFF` 可配置参数 |
| Interim → final price 修正 | 数据不一致 | 事务式 DELETE-INSERT per month + fct_price `delete+insert` + lookback_days=3 |
| 一个 POC 多个 plant | generation-price join fanout | `int_generation_by_poc` 先聚合到 POC 级再 join |
| DuckDB 写锁冲突 | dbt 和 Streamlit 同时运行报错 | Streamlit 用 `read_only=True`；文档注明不要并行 |

### 10.1 Known Limitations

- **NSP 时间旅行**: dim_node 使用 NSP Table 的当前快照。2016-2026 期间部分节点的 region 归属可能有变化（停用、新建、重划区域），对历史 region 级分析可能有轻微偏差。Island 级分析不受影响。
- **POC 历史变化**: 电厂的 POC 可能随时间变化。当前使用 Generation_MD 当月行中的 POC_Code，不做历史追踪。
- **负价覆盖**: stg_price 保留负价，但 Dashboard 上的部分聚合指标（如"平均价格"）可能被极端负值拉低。需在 Dashboard tooltip 中说明。
- **DST 过渡日 TP 序列不连续**: 详见 §3.3.3 macro 修正说明。Dashboard Q7/Q8 中 DST 日数据应加 `⚠` 标注；不影响月度/年度聚合
- **Streamlit Cloud + Snowflake 冷启动**: 线上 demo 首次查询有 ~30s 唤醒延迟——Snowflake DASHBOARD_WH 自动挂起后需要重新激活，叠加 Streamlit Cloud 免费版冷启动。第二次查询起恢复秒级。Dashboard 第一页加 `st.cache_data` 减少重复查询

---

## 11. Out of Scope

以下明确不在 V2 范围内，可在面试中作为"下一步计划"口头提及：

- **Hydro Modelling Dataset + Q10 Hydro-Price Driver**: V3 首要扩展方向。需要 dim_catchment seed、fct_hydro、mart_hydro_price_driver，约 3 天工作量。V2 阶段先用 generation × price 撑住 cross-source 叙事
- **Bids and Offers（投标报价）数据**: V3 方向之一
- **LoadGenerationPrice 日度合并文件**: 可用于交叉验证，但不纳入正式管道（缺少 fuel_code）
- **MongoDB 集成**: 对此项目无自然 use case
- **Machine Learning 价格预测**: mart 层已为 ML pipeline 准备好特征
- **Real-time streaming**: AT Streaming Pipeline 项目已覆盖
- **多租户/RBAC**: 个人项目
- **数据版本管理**: ROI 不高
- ~~**Observability Tier 1**~~ ✅ **Done in Phase 5** (`fct_dbt_run` + Pipeline Health page + Slack webhook + SLO docs)
- ~~**Observability Tier 2 — cost mart**~~ ✅ **Done in Phase 5** (`mart_warehouse_cost` from SF ACCOUNT_USAGE)
- **Observability Tier 2 — remaining (V3)**: S3 存储 mart + 行数 / 价格 drift 异常检测 + dbt docs CI 部署到 GitHub Pages
- **Observability Tier 3**: OpenTelemetry 端到端 trace、专业 obs 后端（Datadog/Honeycomb）、formal SLO/error budget — 个人项目仅作叙事提及，不实现

---

## 12. 成功标准

| 维度 | 标准 |
|------|------|
| 功能 | 9 个 dashboard 页面全部可用，回答 Q1-Q9 业务问题 |
| 规模 | Snowflake 中 fct_generation + fct_price ≥ 60M 行，dbt lineage ≥ 25 models |
| 工程 | 双模式（local/cloud）一键切换，CI 全绿 |
| 演示 | `make demo` 在 **~60 秒** 内（含下载 1 个月 CSV + dbt run + Streamlit 启动）从零打开 dashboard，无需 Git LFS |
| 叙事 | 能清晰讲述 V1→V2 的升级理由、POC 级精确 join 的设计决策、trade-off |
| 部署 | 本地演示: DuckDB (`make demo`)；线上 demo: Streamlit Cloud → Snowflake（首次冷启动 ~30s，见 §10.1） |

---

## 13. Monitoring & Alerting

### 13.1 Airflow 级告警

| 事件 | 告警方式 |
|------|---------|
| 任何 Task 失败（重试耗尽） | DAG `default_args` 设 `email_on_failure=True`、`email=[os.environ['AIRFLOW_ALERT_EMAIL']]`；SMTP 配置在 `airflow.cfg` |
| DAG SLA 超时（> 2 小时） | 在 DAG 上设 `sla_miss_callback=send_alert`，回调函数发邮件到 `AIRFLOW_ALERT_EMAIL` |

### 13.2 dbt 数据质量告警

| 检查 | 实现 | 阈值 | 行为 |
|------|------|------|------|
| `fct_generation` 和 `fct_price` 的 `tp_number` 范围 | `dbt_expectations.expect_column_values_to_be_between(1, 50)` | 超出 1-50 | error |
| 负价监控 | `stg_price_outlier_audit` view 行数 | 同日负价节点 > 100 | warn |
| POC 匹配率 | `tests/poc_match_rate.sql`（见下） | unmatched > 20% | error |
| 月份完整性 | `tests/price_month_completeness.sql` | 任何预期月份缺失 | error |
| fct_generation per-month 行数 | `tests/fct_generation_monthly_min_rows.sql` | 任一 trading_month 行数 < 5,000 | error |

```sql
-- tests/poc_match_rate.sql
-- 阈值硬编码在此处。修改时直接编辑该 SQL 文件。
WITH gen_pocs AS (
    SELECT DISTINCT poc_code FROM {{ ref('int_generation_by_poc') }}
),
price_pocs AS (
    SELECT DISTINCT poc_code FROM {{ ref('fct_price') }}
),
stats AS (
    SELECT
        SUM(CASE WHEN p.poc_code IS NULL THEN 1 ELSE 0 END)::FLOAT AS unmatched,
        COUNT(*)::FLOAT AS total
    FROM gen_pocs g LEFT JOIN price_pocs p USING (poc_code)
)
SELECT unmatched, total, unmatched / NULLIF(total, 0) AS unmatched_rate
FROM stats
WHERE unmatched / NULLIF(total, 0) > 0.20  -- 阻塞阈值：失配率 > 20% 测试失败
```

```sql
-- tests/fct_generation_monthly_min_rows.sql
-- per-month 检查，而非整表行数（整表 140M 时整表测试无意义）
SELECT trading_month, COUNT(*) AS row_count
FROM {{ ref('fct_generation') }}
GROUP BY 1
HAVING COUNT(*) < 5000
```

### 13.3 本地模式监控

本地无 Airflow，靠 `make dbt-test` 退出码：`make local-full` 中 dbt-test 失败会终止 Makefile pipeline，dashboard 不启动。

---

## 14. V1→V2 迁移策略

**推荐方案：原地扩展**。在现有 Snowflake database (`NZ_ELECTRICITY`) 中：

1. `terraform apply` 新增 `raw_price` / `raw_nsp_table` 表与 S3 prefix
2. `dbt seed --target prod`（新增 nz_public_holidays）
3. 分批 `dbt run --select` 构建并逐批验证（dbt 自身按 DAG 排序，分批的目的是减小一次性失败的爆炸半径）：staging+ → dim_node+ → fct_price+ → mart_price_daily+
4. `dbt test --target prod` 全绿后切换 Streamlit V2 pages

**V1 兼容性保证**：V1 的 5 个 mart 不修改 schema；`dim_plant` 新增 `poc_code` 列——该列来源于 V1 `RAW.RAW_GENERATION.poc_code`（V1 staging 模型 `stg_generation.sql` 已 SELECT 出 `poc_code`，仅 `dim_plant.sql` 此前未投影出该列）。V1 dashboard 在迁移期间持续可用，逐页切换至 V2。

如需干净环境（如面试 demo），可走"全新 database + 全量回填"，参考 §9 回填费用估算（~$15-30）。

---

## 15. CI/CD

### 15.1 CI 流水线（GitHub Actions）

`.github/workflows/ci.yml` 分两类 job：

**A. 所有 PR（含 fork PR）必跑** — 不需要任何 secrets：

| 步骤 | 命令 | 失败时 |
|------|------|--------|
| 1. Lint Python | `ruff check .` | block merge |
| 2. Lint SQL | `sqlfluff lint dbt/models/` | block merge |
| 3. Unit tests | `pytest tests/`（含 `test_dag_integrity.py`、`validate_price_month_completeness`、schema validate 测试） | block merge |
| 4. dbt compile (dev) | `cd dbt && dbt compile --target dev` | block merge |

**B. main 分支 push 才跑** — 需要 GitHub Secrets：

| 步骤 | 命令 | 失败时 |
|------|------|--------|
| 5. dbt compile (prod) | 步骤设 `continue-on-error: true`，跑 `cd dbt && dbt compile --target prod` | 后续步骤用 `if: failure()` 触发 `gh issue create` 自动开 issue 通知，不 fail workflow |

> CI 只跑 `dbt compile`，不跑 `dbt run`——避免 CI 触及真实 Snowflake/AWS 资源产生费用。Compile 已能捕获 Jinja 错误、ref/source 引用错误、SQL 语法错误（双方言）。**fork PR 拿不到 secrets，故 prod compile 不在 PR job 中执行**——dev compile 足以验证 dbt 模型正确性，prod 兼容性在 main push 时把关。
>
> GitHub Actions 没有原生 "post-merge soft-fail"：通过 `continue-on-error` + 失败时开 issue 实现"告警但不阻塞 workflow 视图"的等效效果。

### 15.2 Secrets 注入

- `SNOWFLAKE_ACCOUNT/USER/PASSWORD/DATABASE/WAREHOUSE` 等存在 **GitHub Repository Secrets**
- 仅在 main push job（B）中通过 `env:` 块注入到 `dbt compile --target prod` 步骤
- DuckDB target 不需要 secrets（本地文件）

### 15.3 dbt-docs 部署

`.github/workflows/dbt-docs.yml` 在 main 分支合并后运行 `dbt docs generate --target dev` → 推送到 `gh-pages` 分支 → GitHub Pages 自动发布。用 dev (DuckDB) target 避免触及 Snowflake。

---

## 16. Implementer Bootstrap（Day 1 起步指南）

> 本节给"拿到 PRD 第一天开始写代码"的开发者用。读完 §1-§15 后，按此节顺序操作即可起步。

### 16.1 Snowflake Account Onboarding

**目标**：拿到一组可工作的 Snowflake 凭据，配置好本地 `dbt/profiles.yml`，跑通 `dbt debug --target prod`。

| 步骤 | 操作 | 验收 |
|------|------|------|
| 1 | 注册 Snowflake 30 天 trial：https://signup.snowflake.com/ → 选 Standard / AWS / ap-southeast-2（最接近 NZ） | 拿到 account identifier（如 `xy12345.ap-southeast-2`） |
| 2 | 用 ACCOUNTADMIN 跑 V1 的 `terraform apply`（或手工建 `NZ_ELECTRICITY` database + `TRANSFORM_WH` XS warehouse + `RAW`/`ANALYTICS` schema） | Snowsight 中能看到 database 和 warehouse |
| 3 | 复制 `dbt/profiles.yml.example` 到 `dbt/profiles.yml`，填入 trial account 的 user/password；**profiles.yml 已在 .gitignore** | `cd dbt && dbt debug --target prod` 全绿 |
| 4 | 导出环境变量到 `.env`（从 `.env.example` 复制）：`SNOWFLAKE_ACCOUNT/USER/PASSWORD/DATABASE/WAREHOUSE` | 跑 `make cloud-up` 后 Airflow Connection 自动建立 |

> Trial 给 $400 credit / 30 天。回填一次 + 月度运行 + 测试用约 5-10 credit，30 天内安全。如果 trial 到期，注册新 account 即可（V2 数据是无状态的，可重新回填）。

### 16.2 Mini POC 固定测试 Fixture

**Phase 0.0 的 "2 天 generation 数据"明确为以下 fixture**，确保 Snowflake 和 DuckDB 两侧同源：

| 项 | 值 |
|---|----|
| 日期 | `2024-01-15` 和 `2024-01-16` |
| 数据源 | EMI Generation_MD 202401 月度文件（CSV） |
| 行数 | 全月所有 plant × 2 天 ≈ 600 行（FLATTEN 后 600 × 3 TP = 1800 行） |
| TP 范围 | macro 调用时 `tp_columns` 只传 TP1/TP2/TP3 三个对象 |

**对齐脚本** `scripts/mini_poc_fixture.py`（Phase 0.0 交付物之一）：

```python
# 1. 下载 202401_Generation_MD.csv 到 /tmp/
# 2. 过滤出 Trading_Date in ('2024-01-15', '2024-01-16') → /tmp/mini_poc.csv
# 3a. Snowflake 路径：COPY INTO raw_generation FROM @stage/mini_poc.csv
# 3b. DuckDB 路径：CREATE TABLE raw_generation AS SELECT * FROM read_csv_auto('/tmp/mini_poc.csv')
# 4. 两侧分别跑 stg_generation（用 TP1-3 三列的 macro 调用），导出为 parquet
# 5. python -c "assert pandas.read_parquet('sf.parquet').equals(pandas.read_parquet('duckdb.parquet'))"
```

**通过标准**：两侧 parquet 文件按 `(site_code, gen_code, trading_date, trading_period)` 排序后 row count + 每行 `generation_kwh` 完全相等（整数 kWh，无浮点容差问题）。

### 16.3 V1 → V2 文件级改造决策（前置阅读）

§5.3 决策表已写明，此处作为 Day-1 速查：

| V1 文件 | V2 操作 | 备注 |
|---|---|---|
| `dbt/models/staging/stg_generation.sql` | **改文件**：内联 FLATTEN → 调用 `unpivot_trading_periods` macro（输出 schema 不变） | Phase 0.3 |
| `dbt/models/core/dim_plant.sql` | **改文件**：SELECT list 加 `poc_code`（V1 raw 已有此列） | Phase 0.3 |
| `dbt/models/core/fct_generation.sql` | **不改**：粒度和列保持 V1 状态 | — |
| V1 的 5 个 marts | **不改**：schema 冻结 | — |
| `airflow/dags/nz_electricity_monthly.py` | **保留**：作为 V1 兼容路径并存 | Phase 4.1 |
| `airflow/dags/nz_electricity_v2.py` | **新建** | Phase 4.1 |

### 16.4 Day-1 验证 Checklist

按以下顺序跑通即可进入 Phase 0.0：

```
✅ git clone <repo> && cd <repo>
✅ uv sync                                       # 装依赖
✅ cp .env.example .env && vim .env              # 填 SNOWFLAKE_*
✅ cp dbt/profiles.yml.example dbt/profiles.yml  # 填凭据
✅ cd dbt && dbt debug --target dev              # DuckDB 连接验证
✅ cd dbt && dbt debug --target prod             # Snowflake 连接验证
✅ make demo                                     # ~60s，跑通 1 月 demo
✅ python scripts/mini_poc_fixture.py            # Phase 0.0 起步
```

任一步骤失败，先检查 .env 和 profiles.yml，再回 §16.1 重做对应步骤。

---

## 附录 A: 关键 URL 汇总

| 数据 | URL |
|------|-----|
| Generation_MD | `https://www.emi.ea.govt.nz/Wholesale/Datasets/Generation/Generation_MD/` |
| Final Energy Prices (ByMonth) | `https://emidatasets.blob.core.windows.net/publicdata/Datasets/Wholesale/DispatchAndPricing/FinalEnergyPrices/ByMonth/YYYYMM_FinalEnergyPrices.csv` |
| Final Energy Prices (Daily) | `https://emidatasets.blob.core.windows.net/publicdata/Datasets/Wholesale/DispatchAndPricing/FinalEnergyPrices/YYYYMMDD_FinalEnergyPrices.csv` |
| NSP Table | `https://www.emi.ea.govt.nz/Wholesale/Datasets/MappingsAndGeospatial/NetworkSupplyPointsTable` |
| LoadGenerationPrice (验证用) | `https://www.emi.ea.govt.nz/Datasets/Wholesale/Final_pricing/LoadGenerationPrice` |
| EA Datasets Hub | `https://www.ea.govt.nz/data-and-insights/datasets/` |
| Hydro Modelling（V3 计划用，V2 不接入） | `https://www.ea.govt.nz/data-and-insights/datasets/environment/hydrological-modelling-dataset/` |

## 附录 B: Makefile 双模式命令速查

> 依赖管理用 **uv**（与 V1 工具链一致），版本在 `pyproject.toml` / `uv.lock` 锁定。

```makefile
# ==================== 面试演示 ====================
demo:                    ## ~60s 启动：现下 1 个月数据 → DuckDB → Streamlit。无需预制文件或 Git LFS
	uv sync
	mkdir -p data/raw
	uv run python scripts/download_generation.py --output data/raw/ --months 1
	uv run python scripts/download_price.py --output data/raw/ --months 1
	uv run python scripts/download_nsp.py --output data/raw/
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	cd dbt && uv run dbt seed --target dev && uv run dbt run --target dev
	NZEG_MODE=local uv run streamlit run streamlit/app.py

# ==================== Local 模式 ====================
local-full:              ## 一键全流程（全量 10 年数据）。等价于 setup → download → load → dbt run → dbt test → streamlit
	uv sync
	mkdir -p data/raw
	uv run python scripts/download_generation.py --output data/raw/
	uv run python scripts/download_price.py --output data/raw/
	uv run python scripts/download_nsp.py --output data/raw/
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	cd dbt && uv run dbt seed --target dev && uv run dbt run --target dev && uv run dbt test --target dev
	NZEG_MODE=local uv run streamlit run streamlit/app.py

local-subset:            ## 1 年数据子集（中等规模验证）
	uv run python scripts/download_generation.py --output data/raw/ --years 1
	uv run python scripts/download_price.py --output data/raw/ --years 1
	uv run python scripts/load_local.py --db data/nzeg.duckdb --source data/raw/
	cd dbt && uv run dbt seed --target dev && uv run dbt run --target dev
	NZEG_MODE=local uv run streamlit run streamlit/app.py

dbt-test:                ## 单独跑 dbt test (DuckDB)
	cd dbt && uv run dbt test --target dev

# ==================== Cloud 模式 ====================
cloud-up:                ## 启动 Airflow (Docker Compose)
	docker compose up -d --build

cloud-backfill:          ## 触发 Airflow 回填（示例：2016-01-01 至上月）
	docker compose exec airflow-scheduler airflow dags backfill \
	    nz_electricity_v2 \
	    --start-date 2016-01-01 \
	    --end-date $(shell date -v-1m +%Y-%m-01)

cloud-dbt-full:          ## Snowflake 全量刷新（首次回填后调用一次）
	cd dbt && uv run dbt seed --target prod \
	    && uv run dbt run --target prod --full-refresh \
	    && uv run dbt test --target prod

cloud-dashboard:         ## Streamlit → Snowflake
	NZEG_MODE=cloud uv run streamlit run streamlit/app.py
```

> Makefile 收敛到 7 个 target：`demo` / `local-full` / `local-subset` / `dbt-test` / `cloud-up` / `cloud-backfill` / `cloud-dbt-full` / `cloud-dashboard`。更细粒度的步骤（如单独 download / load）在 README 中以裸命令展示。

## 附录 C: dbt_project.yml 关键配置

```yaml
name: nz_electricity
version: '2.0.0'

vars:
  price_spike_threshold: 300        # $/MWh, 正向 spike 阈值
  negative_price_threshold: 0       # $/MWh, 负价阈值
  bymonth_cutoff: '2024-12'         # Price ByMonth 归档最后可用月份
  lookback_days: 3                  # fct_price incremental 重算窗口。日常 DAG run 用此默认值（覆盖 interim→final 的 ~1 工作日延迟）；
                                    # 每月 final 公布后通过 --vars '{lookback_days: 32}' 触发一次月度 re-run，
                                    # 确保上月所有 interim 行被最终 final 价格覆盖（见 §5.3、§6.2 末尾）
```

> 时区策略和 POC 匹配率阈值不放进 var——前者是文档约定（见 §5.3），后者直接硬编码在 `tests/poc_match_rate.sql` 里（无需运行时覆盖）。少一个 var 就少一处认知开销。
