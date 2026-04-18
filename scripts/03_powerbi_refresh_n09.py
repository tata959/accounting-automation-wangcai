import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

BASE_DIR = Path(os.getenv("R2R_BASE_DIR", Path(__file__).resolve().parents[1]))
DB_PATH = BASE_DIR / "data_mart" / "r2r_finance.db"
PBI_DIR = BASE_DIR / "pbi"
DATASET_CSV = PBI_DIR / "pbi_dataset.csv"
REFRESH_LOG = PBI_DIR / "refresh_trigger.log"


def _runtime_threshold() -> tuple[float, float]:
    threshold_val = float(os.getenv("R2R_THRESHOLD_VAL", "10"))
    threshold_pct = threshold_val / 100.0
    return threshold_val, threshold_pct


def is_approved(conn: sqlite3.Connection):
    db_row = conn.execute(
        "SELECT period_key, status FROM workflow_status ORDER BY status_id DESC LIMIT 1"
    ).fetchone()

    period_key = datetime.now().strftime("%Y-%m")
    db_status = ""
    if db_row:
        period_key, db_status = db_row

    approved = db_status.lower() == "approved"
    return approved, period_key, db_status


def export_powerbi_dataset(conn: sqlite3.Connection, threshold_pct: float):
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

    rev_nonzero = df["revenue"].replace(0, np.nan)
    df["expense_rate"] = df["operating_expense"] / rev_nonzero
    df["gross_margin"] = df["gross_profit"] / rev_nonzero
    df["net_margin"] = df["net_profit"] / rev_nonzero
    df["revenue_mom_pct"] = df.groupby("ticker", sort=False)["revenue"].pct_change()
    df["threshold_pct"] = threshold_pct
    df["alert_flag"] = df["revenue_mom_pct"].abs() > threshold_pct
    df["alert_diff"] = df["revenue_mom_pct"].abs() - threshold_pct
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


def _save_chart_revenue_trend(base_df: pd.DataFrame):
    trend = (
        base_df.groupby("period_key", as_index=False)[["revenue", "net_profit"]]
        .sum()
        .sort_values("period_key")
        .tail(24)
    )
    if trend.empty:
        return

    fig, ax = plt.subplots(figsize=(9, 3.8))
    ax.plot(trend["period_key"], trend["revenue"], marker="o", linewidth=1.8, color="#B86A00", label="Revenue")
    ax.plot(trend["period_key"], trend["net_profit"], marker="o", linewidth=1.4, color="#2E7D32", label="Net Profit")
    ax.set_title("Revenue & Net Profit Trend")
    ax.set_xlabel("Period")
    ax.set_ylabel("Amount")
    ax.legend(loc="upper left")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(PBI_DIR / "chart_revenue_trend.png", dpi=105)
    plt.close(fig)


def write_visual_outputs(base_df: pd.DataFrame, wc_df: pd.DataFrame, threshold_val: float) -> None:
    if base_df.empty:
        return

    PBI_DIR.mkdir(parents=True, exist_ok=True)
    _save_chart_revenue_trend(base_df)

    latest_base = (
        base_df.sort_values(["ticker", "period_key"]).groupby("ticker", as_index=False).tail(1).reset_index(drop=True)
    )
    latest_base["revenue_mom_pct"] = latest_base["revenue_mom_pct"].fillna(0.0)

    kpi = latest_base[["ticker", "period_key", "revenue", "net_profit", "revenue_mom_pct", "alert_flag"]].copy()
    if not wc_df.empty:
        latest_wc = wc_df.sort_values(["ticker", "period_key"]).groupby("ticker", as_index=False).tail(1)
        kpi = kpi.merge(latest_wc[["ticker", "dso", "dio", "dpo", "ccc"]], on="ticker", how="left")

    for col in ["revenue_mom_pct", "dso", "dio", "dpo", "ccc"]:
        if col in kpi.columns:
            kpi[col] = pd.to_numeric(kpi[col], errors="coerce").round(4)

    kpi.to_csv(PBI_DIR / "kpi_latest_snapshot.csv", index=False, encoding="utf-8-sig")

    table_df = kpi.head(80).copy()
    if "revenue_mom_pct" in table_df.columns:
        table_df["revenue_mom_pct(%)"] = (table_df["revenue_mom_pct"] * 100).round(2)

    kpi_html = table_df.to_html(index=False, border=0) if not table_df.empty else "<p>暂无KPI</p>"

    html_page = f"""
    <html>
    <head>
      <meta charset=\"utf-8\" />
      <title>Power BI Visual Preview</title>
      <style>
        body {{ font-family: \"Microsoft YaHei\", Arial, sans-serif; margin:0; background:#f7f8fb; color:#1f2430; }}
        .wrap {{ max-width: 1200px; margin: 16px auto; padding: 0 12px; }}
        .hero {{ background: linear-gradient(135deg, #f0b429 0%, #f8dc88 100%); border-radius: 12px; padding: 12px 14px; color:#2f2507; margin-bottom: 12px; }}
        .section {{ background:#fff; border:1px solid #e6e8ef; border-radius: 12px; padding: 10px; margin-bottom:12px; }}
        .table-wrap {{ width:100%; overflow-x:auto; }}
        table {{ border-collapse: collapse; width: 100%; min-width: 720px; table-layout: fixed; }}
        th, td {{ border: 1px solid #e6e8ef; padding: 6px 8px; font-size: 12px; white-space: nowrap; text-overflow: ellipsis; overflow: hidden; }}
        th {{ background: #fff7df; }}
        img {{ width:100%; border:1px solid #e8dfc8; border-radius:10px; background:#fff; }}
      </style>
    </head>
    <body>
      <div class=\"wrap\">
        <div class=\"hero\">
          <h2 style=\"margin:0;\">CFO 快速看板（轻量预览）</h2>
          <p style=\"margin:4px 0 0 0; font-size:12px;\">生成时间：{datetime.now().isoformat(timespec='seconds')} | 波动阈值：{threshold_val:.2f}%</p>
        </div>
        <div class=\"section\">
          <h3 style=\"margin:0 0 8px 0;\">KPI 快照（最多 80 行）</h3>
          <div class=\"table-wrap\">{kpi_html}</div>
        </div>
        <div class=\"section\">
          <h3 style=\"margin:0 0 8px 0;\">营收趋势分析图</h3>
          <img src=\"chart_revenue_trend.png\" loading=\"lazy\" alt=\"Revenue Trend\" />
        </div>
      </div>
    </body>
    </html>
    """
    (PBI_DIR / "powerbi_visual_preview.html").write_text(html_page, encoding="utf-8")


def main() -> None:
    PBI_DIR.mkdir(parents=True, exist_ok=True)
    threshold_val, threshold_pct = _runtime_threshold()

    with sqlite3.connect(DB_PATH) as conn:
        approved, period_key, db_status = is_approved(conn)
        if not approved:
            msg = f"Not approved. db_status={db_status or 'N/A'}"
            conn.execute(
                "INSERT INTO pbi_refresh_log (period_key, trigger_source, status, message, refreshed_at) VALUES (?, ?, ?, ?, ?)",
                (period_key, "workflow_status", "SKIPPED", msg, datetime.now().isoformat(timespec="seconds")),
            )
            conn.execute(
                "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
                ("N09_PBI_Refresher", "SKIPPED", msg, datetime.now().isoformat(timespec="seconds")),
            )
            conn.commit()
            print(msg)
            return

        base_df = export_powerbi_dataset(conn, threshold_pct=threshold_pct)
        base_df.to_csv(DATASET_CSV, index=False, encoding="utf-8-sig")

        wc_df = export_working_capital_dataset(conn)

        write_pbids_template()
        write_visual_outputs(base_df, wc_df, threshold_val=threshold_val)

        REFRESH_LOG.write_text(
            f"[{datetime.now().isoformat(timespec='seconds')}] Refresh by workflow_status; rows={len(base_df)}; threshold={threshold_val}%\n",
            encoding="utf-8",
        )

        conn.execute(
            "INSERT INTO pbi_refresh_log (period_key, trigger_source, status, message, refreshed_at) VALUES (?, ?, ?, ?, ?)",
            (
                period_key,
                "workflow_status",
                "SUCCESS",
                f"Exported {len(base_df)} rows to pbi_dataset.csv; threshold={threshold_val}%",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.execute(
            "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
            ("N09_PBI_Refresher", "SUCCESS", "Refresh completed", datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()

    print(f"Power BI refresh simulated. Dataset: {DATASET_CSV}")


if __name__ == "__main__":
    main()
