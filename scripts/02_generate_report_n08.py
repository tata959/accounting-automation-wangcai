import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(r"D:\R2R_Automation")
DB_PATH = BASE_DIR / "data_mart" / "r2r_finance.db"
REPORT_DIR = BASE_DIR / "reports"


def query_df(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)


def build_financial_frames(conn: sqlite3.Connection):
    base = """
    SELECT c.ticker, d.fiscal_date, d.period_key, s.statement_type, a.account_name, f.amount
    FROM fact_financials f
    JOIN dim_company c ON f.company_id = c.company_id
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN dim_statement s ON f.statement_id = s.statement_id
    JOIN dim_account a ON f.account_id = a.account_id
    WHERE f.report_level = 'quarterly'
    """
    raw = query_df(conn, base)
    raw["fiscal_date"] = pd.to_datetime(raw["fiscal_date"])

    # P&L
    pl_accounts = ["totalRevenue", "costOfRevenue", "grossProfit", "operatingIncome", "netIncome"]
    pl = raw[(raw["statement_type"] == "income_statement") & (raw["account_name"].isin(pl_accounts))].copy()

    # BS
    bs_accounts = [
        "totalAssets",
        "totalLiabilities",
        "totalShareholderEquity",
        "cashAndCashEquivalentsAtCarryingValue",
        "totalCurrentAssets",
        "totalCurrentLiabilities",
    ]
    bs = raw[(raw["statement_type"] == "balance_sheet") & (raw["account_name"].isin(bs_accounts))].copy()

    # Expense
    exp_accounts = ["researchAndDevelopment", "sellingGeneralAndAdministrative", "operatingExpenses"]
    exp = raw[(raw["statement_type"] == "income_statement") & (raw["account_name"].isin(exp_accounts))].copy()

    # Revenue & Profit variance
    var_accounts = ["totalRevenue", "grossProfit", "netIncome"]
    var_df = raw[(raw["statement_type"] == "income_statement") & (raw["account_name"].isin(var_accounts))].copy()

    return pl, bs, exp, var_df


def enrich_for_report(df: pd.DataFrame, metric_col: str = "account_name") -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values(["ticker", metric_col, "fiscal_date"]).copy()
    df["上月数"] = df.groupby(["ticker", metric_col])["amount"].shift(1)
    df["预算数"] = df["amount"] * 1.03
    df["环比%"] = ((df["amount"] - df["上月数"]) / df["上月数"].abs()).replace([pd.NA, pd.NaT], pd.NA)
    df["同比基准"] = df.groupby(["ticker", metric_col])["amount"].shift(4)
    df["同比%"] = ((df["amount"] - df["同比基准"]) / df["同比基准"].abs()).replace([pd.NA, pd.NaT], pd.NA)
    df["差异额"] = df["amount"] - df["预算数"]
    df["备注"] = "Auto-generated"
    out = df.rename(columns={"amount": "本月数", metric_col: "科目/指标"})[
        ["ticker", "period_key", "科目/指标", "本月数", "上月数", "预算数", "环比%", "同比%", "差异额", "备注"]
    ]
    return out


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        pl, bs, exp, var_df = build_financial_frames(conn)

        pl_out = enrich_for_report(pl)
        bs_out = enrich_for_report(bs)
        exp_out = enrich_for_report(exp)
        var_out = enrich_for_report(var_df)

        stamp = datetime.now().strftime("%Y_%m")
        out_path = REPORT_DIR / f"Financial_Report_{stamp}.xlsx"

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            pl_out.to_excel(writer, sheet_name="P&L", index=False)
            bs_out.to_excel(writer, sheet_name="BS", index=False)
            exp_out.to_excel(writer, sheet_name="Expense_Analysis", index=False)
            var_out.to_excel(writer, sheet_name="Revenue_Profit_Variance", index=False)

        conn.execute(
            "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
            ("N08_ReportFactory", "SUCCESS", f"Generated report: {out_path.name}", datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()

    print(f"Report generated: {out_path}")


if __name__ == "__main__":
    main()
