import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(os.getenv("R2R_BASE_DIR", Path(__file__).resolve().parents[1]))
DB_PATH = BASE_DIR / "data_mart" / "r2r_finance.db"
REPORT_DIR = BASE_DIR / "reports"


def query_df(conn: sqlite3.Connection, sql: str) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn)


def _runtime_params() -> dict:
    last_year_avg_growth = float(os.getenv("R2R_LAST_YEAR_AVG_GROWTH", "0.03"))
    expected_growth = float(os.getenv("R2R_EXPECTED_GROWTH", str(last_year_avg_growth)))
    budget_amount_raw = os.getenv("R2R_BUDGET_AMOUNT", "").strip()
    budget_amount = float(budget_amount_raw) if budget_amount_raw else None
    weights = {}
    try:
        weights = json.loads(os.getenv("R2R_ACCOUNT_WEIGHTS", "{}"))
    except Exception:
        weights = {}
    return {
        "last_year_avg_growth": last_year_avg_growth,
        "expected_growth": expected_growth,
        "budget_amount": budget_amount,
        "weights": weights,
    }


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

    pl_accounts = ["totalRevenue", "costOfRevenue", "grossProfit", "operatingIncome", "netIncome"]
    pl = raw[(raw["statement_type"] == "income_statement") & (raw["account_name"].isin(pl_accounts))].copy()

    bs_accounts = [
        "totalAssets",
        "totalLiabilities",
        "totalShareholderEquity",
        "cashAndCashEquivalentsAtCarryingValue",
        "totalCurrentAssets",
        "totalCurrentLiabilities",
    ]
    bs = raw[(raw["statement_type"] == "balance_sheet") & (raw["account_name"].isin(bs_accounts))].copy()

    exp_accounts = ["researchAndDevelopment", "sellingGeneralAndAdministrative", "operatingExpenses"]
    exp = raw[(raw["statement_type"] == "income_statement") & (raw["account_name"].isin(exp_accounts))].copy()

    var_accounts = ["totalRevenue", "grossProfit", "netIncome"]
    var_df = raw[(raw["statement_type"] == "income_statement") & (raw["account_name"].isin(var_accounts))].copy()

    return pl, bs, exp, var_df


def enrich_for_report(df: pd.DataFrame, params: dict, metric_col: str = "account_name") -> pd.DataFrame:
    if df.empty:
        return df

    work = df.sort_values(["ticker", metric_col, "fiscal_date"]).copy()
    work["上月数"] = work.groupby(["ticker", metric_col])["amount"].shift(1)
    work["同比基准"] = work.groupby(["ticker", metric_col])["amount"].shift(4)

    weight_map = params.get("weights", {}) or {}
    work["weight"] = work[metric_col].map(lambda x: float(weight_map.get(str(x), 1.0)))

    growth_base = params["expected_growth"] if params["expected_growth"] is not None else params["last_year_avg_growth"]
    budget_base = work["上月数"].where(work["上月数"].notna(), work["amount"]).astype(float)
    if params.get("budget_amount") is not None:
        budget_base = pd.Series(float(params["budget_amount"]), index=work.index)

    work["预算数"] = budget_base * (1.0 + float(growth_base)) * work["weight"]

    prev = work["上月数"].abs().replace(0, pd.NA)
    yoy_base = work["同比基准"].abs().replace(0, pd.NA)
    work["环比%"] = (work["amount"] - work["上月数"]) / prev
    work["同比%"] = (work["amount"] - work["同比基准"]) / yoy_base

    work["差异额"] = work["amount"] - work["预算数"]
    work["备注"] = f"ParamSim(g={growth_base:.4f})"

    out = work.rename(columns={"amount": "本月数", metric_col: "科目/指标"})[
        ["ticker", "period_key", "科目/指标", "本月数", "上月数", "预算数", "环比%", "同比%", "差异额", "备注"]
    ]
    return out


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    params = _runtime_params()

    with sqlite3.connect(DB_PATH) as conn:
        pl, bs, exp, var_df = build_financial_frames(conn)

        pl_out = enrich_for_report(pl, params=params)
        bs_out = enrich_for_report(bs, params=params)
        exp_out = enrich_for_report(exp, params=params)
        var_out = enrich_for_report(var_df, params=params)

        stamp = datetime.now().strftime("%Y_%m")
        out_path = REPORT_DIR / f"Financial_Report_{stamp}.xlsx"

        with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
            pl_out.to_excel(writer, sheet_name="P&L", index=False)
            bs_out.to_excel(writer, sheet_name="BS", index=False)
            exp_out.to_excel(writer, sheet_name="Expense_Analysis", index=False)
            var_out.to_excel(writer, sheet_name="Revenue_Profit_Variance", index=False)

        conn.execute(
            "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
            (
                "N08_ReportFactory",
                "SUCCESS",
                f"Generated report: {out_path.name}; expected_growth={params['expected_growth']}",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()

    print(f"Report generated: {out_path}")


if __name__ == "__main__":
    main()
