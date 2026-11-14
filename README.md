# Grid Meter Lakehouse

为虚构泵站与工厂把累计电表读数转化为可核对的小时用电事实和站点日用电汇总。
采用 DuckDB + SQL + Parquet，实际保存原始 CSV、标准化数据、区间事实与日汇总；不依赖集群。
样例全部合成，口径是计量差分，不是把累计读数直接相加。详见 `docs/data-contract.md`。

## 安装

Python 3.11+，独立虚拟环境；DuckDB 1.4.2 固定版本，支持该版本 wheel 的 macOS/Linux 平台。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install -r requirements.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
```

初次安装需要网络；无云服务、真实账号、桌面依赖。运行和测试离线。

## 完整演示

在根目录运行，首次 `run/lake` 必须不存在。全量重放使用另一个新目录，已有目录会被拒绝。

```sh
.venv/bin/python -m meter_lake --output run/lake
.venv/bin/python -m meter_lake.query run/lake/warehouse.duckdb
.venv/bin/python -m meter_lake --output run/replay
.venv/bin/python -m unittest discover -s tests -v
```

样例 2 台表、6 个累计边界、4 个小时区间。North Works=2.5 kWh，Canal Pump=0.9 kWh，
合计 3.4 kWh；日期按区间起点 UTC 归属。原始输入故意乱序，SQL window 按时间排序后差分。
`run/lake/manifest.json` 保存源文件哈希、schema 版本和业务汇总；Parquet 可用 DuckDB 单独查询。
同一个输出目录重复运行返回非零，保护已有快照；改新目录应得到一致业务值。

## 流式全量构建

全量构建不再把读数整体载入 Python：原始 CSV 先落到暂存 bronze，校验、分批入库
（`--batch-rows`，默认 10000，必须为正整数）与 manifest 哈希都作用于同一份暂存字节。
Python 只保留维表和最多两个批次；重复键（含跨批次的 UTC 等价时刻）由 readings 主键
在 DuckDB 内拒绝。任一批次坏行或导出失败都不会发布输出，暂存目录自动清理。

```sh
.venv/bin/python -m meter_lake --output run/lake --batch-rows 5000
.venv/bin/python scripts/bench_streaming.py   # 10 倍行数差的分批内存基准
```

## 增量修订（v2）

已发布快照保持不变；把一份有序补丁应用到父快照（v1 全量快照或已有的 v2 快照），
在不存在的新目录中生成完整、自包含的 v2 快照：

```sh
.venv/bin/python -m meter_lake.incremental \
  --base run/lake \
  --changes data/patches/01-revise-history.csv \
  --output run/lake-v2
.venv/bin/python -m meter_lake.query run/lake-v2/warehouse.duckdb
```

Python 调用：`from meter_lake.incremental import revise`，
签名为 `revise(base, changes_path, output)`。补丁契约、依赖闭包、v2 分层与失败
重跑语义见 `docs/incremental-v2.md`。合成补丁示例位于 `data/patches/`，包括
仅表头补丁、补报小时、等值 replace 与删除。增量构建只撤销并重算变化边界邻接
闭包内的区间与受影响站点日，不调用全量 build，也不重跑全表 lag 差分。

## 月度站点电价比较

只读现有 v1/v2 快照，按 UTC 月份和站点比较一份或多份严格 JSON 套餐；不修改快照、
manifest 或 DuckDB。旧的全量构建、增量修订和 query 入口保持不变。

```sh
.venv/bin/python -m meter_lake.tariff \
  --snapshot run/lake-v2 --month 2026-01 --site "North Works" \
  --tariff data/tariffs/01-peak-aware.json \
  --tariff data/tariffs/02-flat-saver.json \
  --output run/north-works-2026-01.json
```

套餐由互斥的 0–24 点小时段单价、月累计 kWh 递增阶梯附加价（末档无限）、
每 kW 需量价和固定费组成。按区间起点 UTC 小时计价；电量费、逐档边际附加费、
站点同时小时合计 kWh 最大值乘需量价与固定费均用十进制精确计算，仅最后
ROUND_HALF_UP 到分，排名按未舍入总额和套餐 ID。月内本站每台表每小时恰有一条
事实才可比较；缺测按表列出合并后的 UTC 缺口并禁止排名，零值不算缺测，坏事实
拒绝整次比较。报告含逐表逐小时依据、全部并列峰时、阶梯分摊与来源哈希，可脱离
快照复算；写出失败保留旧报告，输入不变时重跑字节一致。
自带样例仅有 2 个小时区间，运行上述命令会如实列出整月缺口并拒绝排名；
完整月份的比较见 `tests/test_tariff.py` 的合成站点。契约、费用公式和报告字段见
`docs/tariff-comparison.md`。

## 文件与语义

- `meter_lake/ingest.py`：CSV、维表契约和读数分批流式校验。
- `meter_lake/transform.sql`：lag 差分、维度关联与日汇总。
- `meter_lake/pipeline.py`：分批入库、分层构建、业务校验、Parquet 和 manifest。
- `meter_lake/patch.py`：补丁解析、整批前置校验、最终计量约束与依赖闭包。
- `meter_lake/incremental/`：v2 增量修订引擎（`revise`）与 `python -m meter_lake.incremental` 入口。
- `meter_lake/query.py`：只读查询已构建数据库。
- `meter_lake/tariff/`：月度站点电价比较（`compare_month`）与 `python -m meter_lake.tariff` 入口。
- `data/`：自制数据和来源说明；`data/patches/`：合成补丁示例；`data/tariffs/`：合成电价套餐；`tests/`：计量语义、重复运行和失败测试。

源数据错误（未知表、重复时间、缺小时、读数回退、超过容量）拒绝整个批次，输出目录不发布。
基线是单写者全量构建；没有实现流处理、分区增量或 Schema 演进。
SQL 文件作为包资源随安装分发。依赖保留各自许可证；项目代码与自制数据为 MIT。
