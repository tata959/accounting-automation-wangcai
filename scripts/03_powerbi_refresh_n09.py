import json
import sqlite3
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BASE_DIR = Path(r"D:\R2R_Automation")
DB_PATH = BASE_DIR / "data_mart" / "r2r_finance.db"
PBI_DIR = BASE_DIR / "pbi"
STATUS_FILE = PBI_DIR / "status.txt"
DATASET_CSV = PBI_DIR / "pbi_dataset.csv"
REFRESH_LOG = PBI_DIR / "refresh_trigger.log"


def is_approved(conn: sqlite3.Connection):
    file_status = ""
    if STATUS_FILE.exists():
        file_status = STATUS_FILE.read_text(encoding="utf-8").strip()

    db_row = conn.execute(
        "SELECT period_key, status FROM workflow_status ORDER BY status_id DESC LIMIT 1"
    ).fetchone()

    period_key = datetime.now().strftime("%Y-%m")
    db_status = ""
    if db_row:
        period_key, db_status = db_row

    approved = file_status.lower() == "approved" or db_status.lower() == "approved"
    source = "status.txt" if file_status.lower() == "approved" else "workflow_status"
    return approved, source, period_key, file_status, db_status


def export_powerbi_dataset(conn: sqlite3.Connection):
    sql = """
    SELECT
        c.ticker,
        d.period_key,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='totalRevenue' THEN f.amount ELSE 0 END) AS revenue,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='costOfRevenue' THEN f.amount ELSE 0 END) AS cost,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='grossProfit' THEN f.amount ELSE 0 END) AS gross_profit,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='netIncome' THEN f.amount ELSE 0 END) AS net_profit,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='operatingExpenses' THEN f.amount ELSE 0 END) AS operating_expense
    FROM fact_financials f
    JOIN dim_company c ON f.company_id = c.company_id
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_statement s ON f.statement_id = s.statement_id
    JOIN dim_account a ON f.account_id = a.account_id
    WHERE f.report_level='quarterly'
    GROUP BY c.ticker, d.period_key
    ORDER BY d.period_key, c.ticker
    """
    df = pd.read_sql_query(sql, conn)
    if df.empty:
        return df

    df["expense_rate"] = (df["operating_expense"] / df["revenue"].replace(0, pd.NA)).astype(float)
    df["gross_margin"] = (df["gross_profit"] / df["revenue"].replace(0, pd.NA)).astype(float)
    df["net_margin"] = (df["net_profit"] / df["revenue"].replace(0, pd.NA)).astype(float)
    df["revenue_mom_pct"] = df.groupby("ticker")["revenue"].pct_change()
    df["alert_flag"] = df["revenue_mom_pct"].abs() > 0.10
    return df


def export_working_capital_dataset(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = """
    SELECT
        c.ticker,
        d.period_key,
        SUM(CASE WHEN s.statement_type='balance_sheet' AND a.account_name IN ('currentNetReceivables','accountsReceivableNetCurrent','netReceivables') THEN f.amount ELSE 0 END) AS ar,
        SUM(CASE WHEN s.statement_type='balance_sheet' AND a.account_name IN ('inventory','inventories') THEN f.amount ELSE 0 END) AS inventory,
        SUM(CASE WHEN s.statement_type='balance_sheet' AND a.account_name IN ('currentAccountsPayable','accountsPayableCurrent','accountsPayable') THEN f.amount ELSE 0 END) AS ap,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='costOfRevenue' THEN f.amount ELSE 0 END) AS cogs,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='totalRevenue' THEN f.amount ELSE 0 END) AS revenue
    FROM fact_financials f
    JOIN dim_company c ON f.company_id = c.company_id
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_statement s ON f.statement_id = s.statement_id
    JOIN dim_account a ON f.account_id = a.account_id
    WHERE f.report_level='quarterly'
    GROUP BY c.ticker, d.period_key
    ORDER BY d.period_key, c.ticker
    """
    wc = pd.read_sql_query(sql, conn)
    if wc.empty:
        return wc

    wc["dso"] = np.where(wc["revenue"] != 0, wc["ar"] / wc["revenue"] * 365.0, np.nan)
    wc["dio"] = np.where(wc["cogs"] != 0, wc["inventory"] / wc["cogs"] * 365.0, np.nan)
    wc["dpo"] = np.where(wc["cogs"] != 0, wc["ap"] / wc["cogs"] * 365.0, np.nan)
    wc["ccc"] = wc["dso"] + wc["dio"] - wc["dpo"]
    return wc


def export_cashflow_dataset(conn: sqlite3.Connection) -> pd.DataFrame:
    sql = """
    SELECT
        c.ticker,
        d.period_key,
        SUM(CASE WHEN s.statement_type='cash_flow' AND a.account_name IN ('operatingCashflow','netCashProvidedByOperatingActivities') THEN f.amount ELSE 0 END) AS operating_cf,
        SUM(CASE WHEN s.statement_type='cash_flow' AND a.account_name IN ('cashflowFromInvestment','netCashUsedForInvestingActivities') THEN f.amount ELSE 0 END) AS investing_cf,
        SUM(CASE WHEN s.statement_type='cash_flow' AND a.account_name IN ('cashflowFromFinancing','netCashUsedProvidedByFinancingActivities') THEN f.amount ELSE 0 END) AS financing_cf,
        SUM(CASE WHEN s.statement_type='cash_flow' AND a.account_name='capitalExpenditures' THEN f.amount ELSE 0 END) AS capex,
        SUM(CASE WHEN s.statement_type='income_statement' AND a.account_name='netIncome' THEN f.amount ELSE 0 END) AS net_income,
        SUM(CASE WHEN s.statement_type='balance_sheet' AND a.account_name='cashAndCashEquivalentsAtCarryingValue' THEN f.amount ELSE 0 END) AS ending_cash
    FROM fact_financials f
    JOIN dim_company c ON f.company_id = c.company_id
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_statement s ON f.statement_id = s.statement_id
    JOIN dim_account a ON f.account_id = a.account_id
    WHERE f.report_level='quarterly'
    GROUP BY c.ticker, d.period_key
    ORDER BY d.period_key, c.ticker
    """
    cf = pd.read_sql_query(sql, conn)
    if cf.empty:
        return cf

    cf["fcf"] = cf["operating_cf"] + cf["capex"]
    cf["profit_quality"] = np.where(cf["net_income"] != 0, cf["operating_cf"] / cf["net_income"], np.nan)
    return cf


def write_pbids_template() -> None:
    pbids = {
        "version": "0.1",
        "connections": [
            {
                "details": {
                    "protocol": "file",
                    "address": {"path": str(DATASET_CSV)},
                },
                "options": {},
                "mode": "Import",
            }
        ],
    }

    (PBI_DIR / "R2R_Local_Dataset.pbids").write_text(
        json.dumps(pbids, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _ar_aging_split(dso: float, ar_value: float) -> tuple[float, float, float]:
    if pd.isna(dso):
        w = (0.55, 0.30, 0.15)
    elif dso <= 30:
        w = (0.80, 0.15, 0.05)
    elif dso <= 90:
        w = (0.45, 0.40, 0.15)
    else:
        w = (0.20, 0.35, 0.45)
    return ar_value * w[0], ar_value * w[1], ar_value * w[2]


def write_visual_outputs(base_df: pd.DataFrame, wc_df: pd.DataFrame, cf_df: pd.DataFrame) -> None:
    if base_df.empty:
        return

    PBI_DIR.mkdir(parents=True, exist_ok=True)

    latest_base = (
        base_df.sort_values(["ticker", "period_key"])
        .groupby("ticker", as_index=False)
        .tail(1)
        .reset_index(drop=True)
    )
    latest_base["revenue_mom_pct"] = latest_base["revenue_mom_pct"].fillna(0.0)
    latest_base["warning"] = latest_base["revenue_mom_pct"].abs() > 0.10

    latest_wc = (
        wc_df.sort_values(["ticker", "period_key"]).groupby("ticker", as_index=False).tail(1).reset_index(drop=True)
        if not wc_df.empty
        else pd.DataFrame()
    )
    latest_cf = (
        cf_df.sort_values(["ticker", "period_key"]).groupby("ticker", as_index=False).tail(1).reset_index(drop=True)
        if not cf_df.empty
        else pd.DataFrame()
    )

    # KPI snapshot (first section in HTML)
    kpi = latest_base[["ticker", "period_key", "revenue", "net_profit", "revenue_mom_pct"]].copy()
    if not latest_wc.empty:
        kpi = kpi.merge(latest_wc[["ticker", "dso", "dio", "dpo", "ccc"]], on="ticker", how="left")
    for col in ["revenue_mom_pct", "dso", "dio", "dpo", "ccc"]:
        if col in kpi.columns:
            kpi[col] = kpi[col].round(4)

    kpi.to_csv(PBI_DIR / "kpi_latest_snapshot.csv", index=False, encoding="utf-8-sig")

    # 1) Working capital line chart (DSO/DIO/DPO/CCC)
    if not wc_df.empty:
        wc_trend = wc_df.groupby("period_key", as_index=False)[["dso", "dio", "dpo", "ccc"]].mean().sort_values("period_key")
        fig_wc, ax_wc = plt.subplots(figsize=(10, 4.2))
        ax_wc.plot(wc_trend["period_key"], wc_trend["dso"], marker="o", label="DSO")
        ax_wc.plot(wc_trend["period_key"], wc_trend["dio"], marker="o", label="DIO")
        ax_wc.plot(wc_trend["period_key"], wc_trend["dpo"], marker="o", label="DPO")
        ax_wc.plot(wc_trend["period_key"], wc_trend["ccc"], marker="o", linewidth=2.2, label="CCC")
        ax_wc.set_title("Working Capital Efficiency (DSO / DIO / DPO / CCC)")
        ax_wc.set_xlabel("Period")
        ax_wc.set_ylabel("Days")
        ax_wc.legend(loc="upper right")
        ax_wc.tick_params(axis="x", rotation=45)
        fig_wc.tight_layout()
        fig_wc.savefig(PBI_DIR / "chart_wc_efficiency.png", dpi=140)
        plt.close(fig_wc)

        # AR aging stacked chart (visual scheme)
        ar_rows = []
        for _, r in wc_df.iterrows():
            b1, b2, b3 = _ar_aging_split(r["dso"], r["ar"])
            ar_rows.append({
                "period_key": r["period_key"],
                "bucket_1_30": b1,
                "bucket_31_90": b2,
                "bucket_90_plus": b3,
            })
        ar_aging = pd.DataFrame(ar_rows).groupby("period_key", as_index=False).sum().sort_values("period_key")
        ar_aging.to_csv(PBI_DIR / "ar_aging_table.csv", index=False, encoding="utf-8-sig")

        fig_ar, ax_ar = plt.subplots(figsize=(10, 4.2))
        ax_ar.bar(ar_aging["period_key"], ar_aging["bucket_1_30"], label="1-30天")
        ax_ar.bar(ar_aging["period_key"], ar_aging["bucket_31_90"], bottom=ar_aging["bucket_1_30"], label="31-90天")
        ax_ar.bar(
            ar_aging["period_key"],
            ar_aging["bucket_90_plus"],
            bottom=ar_aging["bucket_1_30"] + ar_aging["bucket_31_90"],
            label="90+天",
        )
        ax_ar.set_title("AR Aging (Stacked) 1-30 / 31-90 / 90+")
        ax_ar.set_xlabel("Period")
        ax_ar.set_ylabel("Amount")
        ax_ar.legend(loc="upper right")
        ax_ar.tick_params(axis="x", rotation=45)
        fig_ar.tight_layout()
        fig_ar.savefig(PBI_DIR / "chart_ar_aging_stacked.png", dpi=140)
        plt.close(fig_ar)

    # 2) Cashflow waterfall + FCF trend/profit quality
    if not cf_df.empty:
        cf_trend = cf_df.groupby("period_key", as_index=False)[["operating_cf", "investing_cf", "financing_cf", "net_income", "fcf", "ending_cash"]].sum().sort_values("period_key")
        latest = cf_trend.tail(1).iloc[0]

        # Waterfall for latest period
        categories = ["经营现金流", "投资现金流", "筹资现金流"]
        values = [latest["operating_cf"], latest["investing_cf"], latest["financing_cf"]]
        cum = np.cumsum([0] + values[:-1])
        colors = ["#2E7D32" if v >= 0 else "#C62828" for v in values]

        fig_wf, ax_wf = plt.subplots(figsize=(8.6, 4.2))
        ax_wf.bar(categories, values, bottom=cum, color=colors)
        final_cash = sum(values)
        ax_wf.bar(["期末现金净变动"], [final_cash], color="#1F5FBF")
        ax_wf.set_title(f"Cash Flow Waterfall ({latest['period_key']})")
        ax_wf.set_ylabel("Amount")
        fig_wf.tight_layout()
        fig_wf.savefig(PBI_DIR / "chart_cashflow_waterfall.png", dpi=140)
        plt.close(fig_wf)

        # FCF trend with net income + operating CF
        fig_fcf, ax_fcf = plt.subplots(figsize=(10, 4.2))
        ax_fcf.plot(cf_trend["period_key"], cf_trend["fcf"], marker="o", linewidth=2.2, label="FCF")
        ax_fcf.plot(cf_trend["period_key"], cf_trend["net_income"], marker="o", label="净利润")
        ax_fcf.plot(cf_trend["period_key"], cf_trend["operating_cf"], marker="o", label="经营现金流")
        ax_fcf.set_title("FCF Trend vs Net Income & Operating CF")
        ax_fcf.set_xlabel("Period")
        ax_fcf.set_ylabel("Amount")
        ax_fcf.legend(loc="upper left")
        ax_fcf.tick_params(axis="x", rotation=45)
        fig_fcf.tight_layout()
        fig_fcf.savefig(PBI_DIR / "chart_fcf_profit_quality.png", dpi=140)
        plt.close(fig_fcf)

    # 3) Keep previous simple trend for continuity
    trend = base_df.groupby("period_key", as_index=False)[["revenue", "net_profit"]].sum().sort_values("period_key")
    fig1, ax1 = plt.subplots(figsize=(10, 4))
    ax1.plot(trend["period_key"], trend["revenue"], marker="o", linewidth=1.8, color="#B86A00")
    ax1.set_title("Revenue Trend")
    ax1.set_xlabel("Period")
    ax1.set_ylabel("Revenue")
    ax1.tick_params(axis="x", rotation=45)
    fig1.tight_layout()
    fig1.savefig(PBI_DIR / "chart_revenue_trend.png", dpi=140)
    plt.close(fig1)

    # Modern B-side yellow UI preview HTML
    kpi_html = kpi.to_html(index=False, border=0) if not kpi.empty else "<p>暂无KPI</p>"

    html_page = f"""
    <html>
    <head>
      <meta charset=\"utf-8\" />
      <title>Power BI Visual Preview</title>
      <style>
        :root {{
          --bg:#f7f8fb;
          --card:#ffffff;
          --line:#e6e8ef;
          --text:#1f2430;
          --muted:#657085;
          --accent:#f0b429;
          --accent2:#f9d66a;
        }}
        body {{ font-family: "Microsoft YaHei", Arial, sans-serif; margin: 0; background: var(--bg); color: var(--text); }}
        .wrap {{ max-width: 1200px; margin: 18px auto; padding: 0 14px; }}
        .hero {{
          background: linear-gradient(135deg, #f0b429 0%, #f8dc88 100%);
          border-radius: 14px;
          padding: 14px 16px;
          color:#2f2507;
          margin-bottom: 14px;
        }}
        .hero h2 {{ margin:0; font-size: 22px; }}
        .hero p {{ margin:4px 0 0 0; font-size:13px; }}
        .section {{ background: var(--card); border:1px solid var(--line); border-radius: 12px; padding: 12px; margin-bottom: 12px; }}
        .section h3 {{ margin:0 0 8px 0; font-size:16px; }}
        .grid {{ display:grid; grid-template-columns: 1fr 1fr; gap:12px; }}
        @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} }}
        img {{ width:100%; border:1px solid #e8dfc8; border-radius: 10px; background:#fff; }}
        .table-wrap {{ width:100%; overflow-x:auto; }}
        table {{ border-collapse: collapse; width: 100%; min-width: 760px; background: #fff; table-layout: fixed; }}
        th, td {{ border: 1px solid #e6e8ef; padding: 6px 8px; font-size: 12px; white-space: nowrap; text-overflow: ellipsis; overflow: hidden; }}
        th {{ background: #fff7df; }}
        .note {{ color: var(--muted); font-size: 12px; }}
      </style>
    </head>
    <body>
      <div class=\"wrap\">
        <div class=\"hero\">
          <h2>CFO 现金流与营运资本看板（预览）</h2>
          <p>生成时间：{datetime.now().isoformat(timespec='seconds')}</p>
        </div>

        <div class=\"section\">
          <h3>KPI快照</h3>
          <div class=\"table-wrap\">{kpi_html}</div>
        </div>

        <div class=\"grid\">
          <div class=\"section\">
            <h3>营运资本效率：DSO / DIO / DPO / CCC</h3>
            <img src=\"chart_wc_efficiency.png\" alt=\"Working Capital\" />
          </div>
          <div class=\"section\">
            <h3>应收账龄分析（堆叠柱形图）</h3>
            <img src=\"chart_ar_aging_stacked.png\" alt=\"AR Aging\" />
          </div>
        </div>

        <div class=\"grid\">
          <div class=\"section\">
            <h3>现金流构成瀑布图</h3>
            <img src=\"chart_cashflow_waterfall.png\" alt=\"Cashflow Waterfall\" />
          </div>
          <div class=\"section\">
            <h3>FCF 趋势 + 净利润 + 经营现金流</h3>
            <img src=\"chart_fcf_profit_quality.png\" alt=\"FCF Quality\" />
          </div>
        </div>

        <div class=\"section\">
          <h3>营收趋势分析图</h3>
          <img src=\"chart_revenue_trend.png\" alt=\"Revenue Trend\" />
          <p class=\"note\">注：应收账龄基于当前账面应收与DSO的区间映射估算，用于管理预警，不替代明细账龄台账。</p>
        </div>
      </div>
    </body>
    </html>
    """
    (PBI_DIR / "powerbi_visual_preview.html").write_text(html_page, encoding="utf-8")


def main() -> None:
    PBI_DIR.mkdir(parents=True, exist_ok=True)
    if not STATUS_FILE.exists():
        STATUS_FILE.write_text("Pending", encoding="utf-8")

    with sqlite3.connect(DB_PATH) as conn:
        approved, source, period_key, file_status, db_status = is_approved(conn)
        if not approved:
            msg = f"Not approved. status.txt={file_status or 'N/A'}, db_status={db_status or 'N/A'}"
            conn.execute(
                "INSERT INTO pbi_refresh_log (period_key, trigger_source, status, message, refreshed_at) VALUES (?, ?, ?, ?, ?)",
                (period_key, "none", "SKIPPED", msg, datetime.now().isoformat(timespec="seconds")),
            )
            conn.execute(
                "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
                ("N09_PBI_Refresher", "SKIPPED", msg, datetime.now().isoformat(timespec="seconds")),
            )
            conn.commit()
            print(msg)
            return

        base_df = export_powerbi_dataset(conn)
        base_df.to_csv(DATASET_CSV, index=False, encoding="utf-8-sig")

        wc_df = export_working_capital_dataset(conn)
        cf_df = export_cashflow_dataset(conn)

        write_pbids_template()
        write_visual_outputs(base_df, wc_df, cf_df)

        REFRESH_LOG.write_text(
            f"[{datetime.now().isoformat(timespec='seconds')}] Refresh triggered by {source}; rows={len(base_df)}\n",
            encoding="utf-8",
        )

        conn.execute(
            "INSERT INTO pbi_refresh_log (period_key, trigger_source, status, message, refreshed_at) VALUES (?, ?, ?, ?, ?)",
            (
                period_key,
                source,
                "SUCCESS",
                f"Exported {len(base_df)} rows to pbi_dataset.csv + advanced visual previews",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.execute(
            "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
            ("N09_PBI_Refresher", "SUCCESS", f"Refresh simulated by {source}", datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()

    print(f"Power BI refresh simulated. Dataset: {DATASET_CSV}")


if __name__ == "__main__":
    main()
