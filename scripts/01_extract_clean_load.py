import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import requests

BASE_DIR = Path(os.getenv("R2R_BASE_DIR", Path(__file__).resolve().parents[1]))
RAW_DIR = BASE_DIR / "data_raw"#原始数据层
STAGE_DIR = BASE_DIR / "data_stage"#中间数据层
MART_DIR = BASE_DIR / "data_mart"#最终数据层
LOG_DIR = BASE_DIR / "logs"#运行日志
DB_PATH = MART_DIR / "r2r_finance.db"

TICKERS = [t.strip().upper() for t in os.getenv("R2R_TICKERS", "IBM").split(",") if t.strip()]
FUNCTIONS = {
    "INCOME_STATEMENT": "income_statement",
    "BALANCE_SHEET": "balance_sheet",
    "CASH_FLOW": "cash_flow",
}
API_KEY = os.getenv("ALPHAVANTAGE_API_KEY", "demo")


def ensure_dirs() -> None:
    for d in [RAW_DIR, STAGE_DIR, MART_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)

#从api接口抓取数据
def fetch_statement(ticker: str, function: str) -> Dict:
    url = "https://www.alphavantage.co/query"
    params = {"function": function, "symbol": ticker, "apikey": API_KEY}
    session = requests.Session()
    session.trust_env = False
    r = session.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    if "annualReports" not in data:
        raise ValueError(f"API response missing reports for {ticker} {function}: {data}")
    return data

#把原始json保存
def save_raw_json(ticker: str, function: str, data: Dict) -> Path:
    out = RAW_DIR / f"{ticker}_{function}.json"
    out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return out

#把json用pandas的dataframe转化成行列表格
def normalize_reports(ticker: str, statement_type: str, reports: List[Dict], report_level: str) -> pd.DataFrame:
    rows = []
    for rep in reports:
        fiscal_date = rep.get("fiscalDateEnding")
        currency = rep.get("reportedCurrency", "USD")
        for k, v in rep.items():
            if k in {"fiscalDateEnding", "reportedCurrency"}:
                continue
            try:
                amount = float(v)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "ticker": ticker,
                    "statement_type": statement_type,
                    "report_level": report_level,
                    "fiscal_date": fiscal_date,
                    "currency": currency,
                    "account_name": k,
                    "amount": amount,
                    "source_system": "AlphaVantage",
                    "load_time": datetime.now().isoformat(timespec="seconds"),
                }
            )
    return pd.DataFrame(rows)

#清洗数据（清除异常值、删除重复值）+时间处理（拆分成季度和年度）
def build_stage_dataset() -> pd.DataFrame:
    all_frames = []
    for ticker in TICKERS:
        for fn_api, st_name in FUNCTIONS.items():
            data = fetch_statement(ticker, fn_api)
            save_raw_json(ticker, fn_api, data)
            annual_df = normalize_reports(ticker, st_name, data.get("annualReports", []), "annual")
            quarterly_df = normalize_reports(ticker, st_name, data.get("quarterlyReports", []), "quarterly")
            all_frames.extend([annual_df, quarterly_df])

    stage_df = pd.concat(all_frames, ignore_index=True)
    stage_df["fiscal_date"] = pd.to_datetime(stage_df["fiscal_date"])
    stage_df = stage_df.dropna(subset=["fiscal_date", "account_name", "amount"])
    stage_df = stage_df[stage_df["amount"].abs() < 1e14]
    stage_df = stage_df.drop_duplicates(subset=["ticker", "statement_type", "report_level", "fiscal_date", "account_name"])

    out_csv = STAGE_DIR / "financial_long_clean.csv"
    stage_df.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return stage_df

#星型模型构建（sql代码部分）
def create_schema(conn: sqlite3.Connection) -> None:
    ddl = """
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS dim_company (
        company_id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT UNIQUE NOT NULL,
        company_name TEXT,
        currency TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY AUTOINCREMENT,
        fiscal_date TEXT UNIQUE NOT NULL,
        fiscal_year INTEGER,
        fiscal_quarter INTEGER,
        fiscal_month INTEGER,
        period_key TEXT
    );

    CREATE TABLE IF NOT EXISTS dim_statement (
        statement_id INTEGER PRIMARY KEY AUTOINCREMENT,
        statement_type TEXT UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS dim_account (
        account_id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_name TEXT UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS fact_financials (
        fact_id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        date_id INTEGER NOT NULL,
        statement_id INTEGER NOT NULL,
        account_id INTEGER NOT NULL,
        report_level TEXT NOT NULL,
        amount REAL NOT NULL,
        source_system TEXT,
        load_time TEXT,
        FOREIGN KEY(company_id) REFERENCES dim_company(company_id),
        FOREIGN KEY(date_id) REFERENCES dim_date(date_id),
        FOREIGN KEY(statement_id) REFERENCES dim_statement(statement_id),
        FOREIGN KEY(account_id) REFERENCES dim_account(account_id),
        UNIQUE(company_id, date_id, statement_id, account_id, report_level)
    );

    CREATE TABLE IF NOT EXISTS workflow_status (
        status_id INTEGER PRIMARY KEY AUTOINCREMENT,
        period_key TEXT NOT NULL,
        status TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        updated_by TEXT NOT NULL,
        comments TEXT
    );

    CREATE TABLE IF NOT EXISTS pbi_refresh_log (
        refresh_id INTEGER PRIMARY KEY AUTOINCREMENT,
        period_key TEXT NOT NULL,
        trigger_source TEXT NOT NULL,
        status TEXT NOT NULL,
        message TEXT,
        refreshed_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS run_log (
        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
        node_name TEXT NOT NULL,
        result TEXT NOT NULL,
        message TEXT,
        created_at TEXT NOT NULL
    );
    """
    conn.executescript(ddl)

#维度表-如果不存在，则新增，如存在就不重复插入（insert or ignore）
def upsert_dimensions_and_fact(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    cur = conn.cursor()

    for ticker, grp in df.groupby("ticker"):
        currency = grp["currency"].mode().iat[0] if not grp["currency"].mode().empty else "USD"#用众数确定该股票对应的主要货币，防止原始数据中因单行错误导致的货币混乱
        cur.execute(
            "INSERT OR IGNORE INTO dim_company (ticker, company_name, currency) VALUES (?, ?, ?)",
            (ticker, ticker, currency),
        )

    for st in sorted(df["statement_type"].unique()):
        cur.execute("INSERT OR IGNORE INTO dim_statement (statement_type) VALUES (?)", (st,))

    for acc in sorted(df["account_name"].unique()):
        cur.execute("INSERT OR IGNORE INTO dim_account (account_name) VALUES (?)", (acc,))
    
    #时间表（提取去重，转化拆分，生成 Period Key）
    date_df = df[["fiscal_date"]].drop_duplicates().copy()
    date_df["fiscal_year"] = date_df["fiscal_date"].dt.year
    date_df["fiscal_month"] = date_df["fiscal_date"].dt.month
    date_df["fiscal_quarter"] = ((date_df["fiscal_month"] - 1) // 3) + 1
    date_df["period_key"] = date_df["fiscal_date"].dt.strftime("%Y-%m")

    for _, r in date_df.iterrows():
        cur.execute(
            """
            INSERT OR IGNORE INTO dim_date (fiscal_date, fiscal_year, fiscal_quarter, fiscal_month, period_key)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                r["fiscal_date"].strftime("%Y-%m-%d"),
                int(r["fiscal_year"]),
                int(r["fiscal_quarter"]),
                int(r["fiscal_month"]),
                r["period_key"],
            ),
        )

#事实表
    company_map = dict(cur.execute("SELECT ticker, company_id FROM dim_company").fetchall())
    statement_map = dict(cur.execute("SELECT statement_type, statement_id FROM dim_statement").fetchall())
    account_map = dict(cur.execute("SELECT account_name, account_id FROM dim_account").fetchall())
    date_map = dict(cur.execute("SELECT fiscal_date, date_id FROM dim_date").fetchall())

    insert_sql = """
    INSERT OR REPLACE INTO fact_financials
    (company_id, date_id, statement_id, account_id, report_level, amount, source_system, load_time)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
#把文本替换成映射对应的维度表数字id，同时加入time和source方便追溯
    records = []
    for _, r in df.iterrows():
        date_key = r["fiscal_date"].strftime("%Y-%m-%d")
        records.append(
            (
                company_map[r["ticker"]],
                date_map[date_key],
                statement_map[r["statement_type"]],
                account_map[r["account_name"]],
                r["report_level"],
                float(r["amount"]),
                r["source_system"],
                r["load_time"],
            )
        )

    cur.executemany(insert_sql, records)
    conn.commit()

#根据period_key查看有无数据，没有就打pending
def seed_workflow_status(conn: sqlite3.Connection) -> None:
    period_key = datetime.now().strftime("%Y-%m")
    cur = conn.cursor()
    exists = cur.execute(
        "SELECT 1 FROM workflow_status WHERE period_key = ? ORDER BY status_id DESC LIMIT 1", (period_key,)
    ).fetchone()
    if not exists:
        cur.execute(
            "INSERT INTO workflow_status (period_key, status, updated_at, updated_by, comments) VALUES (?, ?, ?, ?, ?)",
            (period_key, "Pending", datetime.now().isoformat(timespec="seconds"), "system", "Initial load complete"),
        )
        conn.commit()

#日志归档-记录在哪个环节，有什么结果，时间等）
def write_run_log(conn: sqlite3.Connection, node_name: str, result: str, message: str) -> None:
    conn.execute(
        "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
        (node_name, result, message, datetime.now().isoformat(timespec="seconds")),
    )
    conn.commit()

#确认管道流程
def main() -> None:
    ensure_dirs()
    df = build_stage_dataset()
    with sqlite3.connect(DB_PATH) as conn:
        create_schema(conn)
        upsert_dimensions_and_fact(conn, df)
        seed_workflow_status(conn)
        write_run_log(conn, "N01_N02_N03", "SUCCESS", f"Loaded {len(df)} cleaned rows into star schema")
    print(f"Loaded rows: {len(df)}")
    print(f"SQLite DB: {DB_PATH}")


if __name__ == "__main__":
    main()


