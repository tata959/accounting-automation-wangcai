# 本地 R2R 自动化落地手册（D盘版）

## 0. 目标与范围
本手册实现两个自动化节点：
- N08 报表工厂：自动生成 Excel 多 Sheet 财务报告
- N09 Power BI 刷新器：审批通过后触发刷新（本地模拟）

并包含：
- 真实上市公司三表数据抓取（IBM）
- Python 清洗 + SQLite 建模（Star Schema）
- SQL / Python / Excel / Power BI 连接文件完整产出

## 1. 本地路径规范（全部在 D 盘）
根目录：`D:\R2R_Automation`

子目录：
- `D:\R2R_Automation\data_raw` 原始 JSON
- `D:\R2R_Automation\data_stage` 清洗后长表
- `D:\R2R_Automation\data_mart` SQLite 数据库
- `D:\R2R_Automation\reports` Excel 报表
- `D:\R2R_Automation\pbi` Power BI 刷新输入与触发文件
- `D:\R2R_Automation\scripts` Python 与 SQL 脚本
- `D:\R2R_Automation\logs` 搜索/过程日志

## 2. 数据源与真实性说明
- 数据来源：AlphaVantage 官方接口（上市公司财务报表）
- 公司样本：IBM（NYSE: IBM）
- 报表类型：Income Statement / Balance Sheet / Cash Flow

说明：`demo` API key 仅稳定开放 IBM，因此当前交付使用 IBM 单公司全流程演示；若你提供正式 API Key，可把脚本中的 `TICKERS` 扩展为多公司。

## 3. 执行步骤（命令级）
在 PowerShell 执行：

```powershell
python D:\R2R_Automation\scripts\01_extract_clean_load.py
python D:\R2R_Automation\scripts\02_generate_report_n08.py
python D:\R2R_Automation\scripts\03_powerbi_refresh_n09.py
python D:\R2R_Automation\scripts\04_set_approval_signal.py
python D:\R2R_Automation\scripts\03_powerbi_refresh_n09.py
```

步骤解释：
1. `01_extract_clean_load.py`：抓取三表 JSON（年-季），清洗成长表（将字符串格式的数字转换为浮点数；删除重复项；剔除异常值（例如金额超过 100 万亿的无效数据）；统一日期格式等），写入星型模型（star schema）。
2. `02_generate_report_n08.py`：从 SQL 抽取并生成 `Financial_Report_YYYY_MM.xlsx`。
3. 第一次 `03_powerbi_refresh_n09.py`：若未审批，应跳过刷新。
4. `04_set_approval_signal.py`：模拟财务经理审批通过。
5. 第二次 `03_powerbi_refresh_n09.py`：识别 Approved 后触发刷新模拟并输出 CSV。

## 4. Star Schema 设计（维度建模）
- 维表：
  - `dim_company`（公司信息）
  - `dim_date`（日期）
  - `dim_statement`（报表类型）
  - `dim_account`（存会计科目）
- 事实表：
  - `fact_financials`（具体金额，通过id关联维度表）
- 流程控制表：
  - `workflow_status`（记录当前账期（Period）的处理进度（如 "Pending"））
  - `pbi_refresh_log`（记录脚本每次运行的结果（成功或失败））
  - `run_log`（与 Power BI 等可视化工具对接的记录接口）

设计原因：
1. 三表字段多、口径异构，账户维度单独拆出可复用。
2. 日期维度支持环比/同比计算（N08/N09都要用）。
3. statement维度区分损益/资产负债/现金流，便于查询聚合。
4. workflow/pbi日志表保证可追溯。

## 5. N08 报表工厂逻辑
脚本：`02_generate_report_n08.py`

输出：`D:\R2R_Automation\reports\Financial_Report_2026_04.xlsx`

Sheet：
1. `P&L`
2. `BS`
3. `Expense_Analysis`
4. `Revenue_Profit_Variance`

列结构：
- `ticker`
- `period_key`
- `科目/指标`
- `本月数`
- `上月数`
- `预算数`
- `环比%`
- `同比%`
- `差异额`
- `备注`

指标选择原因：
1. `totalRevenue/costOfRevenue/grossProfit/netIncome`：最直接体现利润形成路径。
2. `totalAssets/totalLiabilities/totalShareholderEquity`：反映偿债与资本结构。
3. `operatingExpenses` 与费用明细：支持费用率分析。
4. 环比/同比：管理层最常用的结账后波动判断维度。

## 6. N09 刷新触发逻辑（精确）
脚本：`03_powerbi_refresh_n09.py`

触发条件（OR）：
- `D:\R2R_Automation\pbi\status.txt` 内容为 `Approved`
- 或 `workflow_status` 最新一条记录的 `status='Approved'`

判定伪逻辑：
```text
if status.txt == 'Approved' OR latest_db_status == 'Approved':
    trigger refresh
else:
    skip refresh
```

触发后动作：
1. 从 SQL 抽取 KPI 数据，输出 `pbi_dataset.csv`
2. 计算 `revenue_mom_pct`
3. 若 `|revenue_mom_pct| > 10%`，标记 `alert_flag=True`
4. 写入 `refresh_trigger.log`
5. 写入 `pbi_refresh_log` 与 `run_log`
6. 生成 `R2R_Local_Dataset.pbids`（Power BI Desktop 可直接打开）

## 7. 为什么这些指标和方法
1. 流动性与风险：
   - 使用 `current assets/current liabilities` 可进一步计算流动比率。
   - 因为月结审批要先确认短期偿债能力是否异常。
2. 盈利质量：
   - `gross margin/net margin/expense rate` 直接衡量盈利结构是否恶化。
3. EBITDA（建议）
   - 当前数据源可扩展到折旧摊销字段后计算 EBITDA。
   - 原因：剔除折旧/融资影响，更适合跨期经营比较。
4. 为什么 SQL + Python：
   - SQL 负责结构化存储和聚合；
   - Python 负责清洗、跨表计算、Excel 输出与触发控制；
   - 成本低、维护门槛低、财务/IT都能接手。

## 8. Power BI 本地接入步骤
1. 打开 `D:\R2R_Automation\pbi\R2R_Local_Dataset.pbids`
2. 在 Power BI Desktop 载入 `pbi_dataset.csv`
3. 创建指标卡：`revenue/cost/gross_profit/net_profit/expense_rate/revenue_mom_pct`
4. 设置条件格式：`alert_flag=True` 显示红色
5. 以后每次只需：审批 -> 运行 N09 -> 点击 Power BI 刷新

## 9. 关键产物清单
- 脚本：`D:\R2R_Automation\scripts\01_extract_clean_load.py`
- 脚本：`D:\R2R_Automation\scripts\02_generate_report_n08.py`
- 脚本：`D:\R2R_Automation\scripts\03_powerbi_refresh_n09.py`
- 脚本：`D:\R2R_Automation\scripts\04_set_approval_signal.py`
- SQL：`D:\R2R_Automation\scripts\schema_star.sql`
- 数据库：`D:\R2R_Automation\data_mart\r2r_finance.db`
- 清洗数据：`D:\R2R_Automation\data_stage\financial_long_clean.csv`
- 三表原始数据：`D:\R2R_Automation\data_raw\IBM_INCOME_STATEMENT.json` 等
- Excel：`D:\R2R_Automation\reports\Financial_Report_2026_04.xlsx`
- PBI数据集：`D:\R2R_Automation\pbi\pbi_dataset.csv`
- PBI连接文件：`D:\R2R_Automation\pbi\R2R_Local_Dataset.pbids`
- 审批信号：`D:\R2R_Automation\pbi\status.txt`
- 刷新日志：`D:\R2R_Automation\pbi\refresh_trigger.log`

## 10. 当前实测结果
- `fact_financials`: 5865 行
- `dim_account`: 63 行
- `dim_date`: 81 行
- N08：已生成 Excel 报表（成功）
- N09：Pending 时跳过、Approved 时触发（成功）
