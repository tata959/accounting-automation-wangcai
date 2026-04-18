import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
from datetime import datetime
from difflib import get_close_matches
from pathlib import Path
from typing import Tuple
from urllib.parse import parse_qs, urlparse

import pandas as pd
import requests

BASE_DIR = Path(os.getenv("R2R_BASE_DIR", Path(__file__).resolve().parent))
SCRIPTS_DIR = BASE_DIR / "scripts"
PBI_DIR = BASE_DIR / "pbi"
REPORT_DIR = BASE_DIR / "reports"
STAGE_DIR = BASE_DIR / "data_stage"
RAW_DIR = BASE_DIR / "data_raw"
DB_PATH = BASE_DIR / "data_mart" / "r2r_finance.db"
ACTION_LOG = BASE_DIR / "logs" / "commander_actions.log"
SCHEMA_SQL = SCRIPTS_DIR / "schema_star.sql"

SCRIPT_01 = SCRIPTS_DIR / "01_extract_clean_load.py"
SCRIPT_02 = SCRIPTS_DIR / "02_generate_report_n08.py"
SCRIPT_03 = SCRIPTS_DIR / "03_powerbi_refresh_n09.py"
SCRIPT_04 = SCRIPTS_DIR / "04_set_approval_signal.py"

REQUIRED_IMPORT_COLS = {
    "ticker",
    "statement_type",
    "report_level",
    "fiscal_date",
    "account_name",
    "amount",
}

STANDARD_ACCOUNT_MAP = {
    "营业收入": "totalRevenue",
    "主营业务收入": "totalRevenue",
    "销售收入": "totalRevenue",
    "revenue": "totalRevenue",
    "sales": "totalRevenue",
    "totalrevenue": "totalRevenue",
    "销售费用": "sellingGeneralAndAdministrative",
    "销售及管理费用": "sellingGeneralAndAdministrative",
    "sellingexpense": "sellingGeneralAndAdministrative",
    "sellinggeneralandadministrative": "sellingGeneralAndAdministrative",
    "营业成本": "costOfRevenue",
    "成本": "costOfRevenue",
    "costofrevenue": "costOfRevenue",
    "毛利润": "grossProfit",
    "grossprofit": "grossProfit",
    "净利润": "netIncome",
    "netincome": "netIncome",
    "营业费用": "operatingExpenses",
    "operatingexpense": "operatingExpenses",
    "operatingexpenses": "operatingExpenses",
}

_BATCH_LOCK = threading.Lock()
_STAGED_BATCHES: list[pd.DataFrame] = []
_STAGED_SOURCES: list[str] = []
_IMPORT_CONFIRMED = False

_RUNTIME_PARAMS = {
    "last_year_avg_growth": 0.03,
    "expected_growth": 0.03,
    "budget_amount": "",
    "threshold_val": 10.0,
    "weight_totalRevenue": 1.0,
    "weight_sellingGeneralAndAdministrative": 1.0,
}


def ensure_dirs() -> None:
    for d in [BASE_DIR, PBI_DIR, REPORT_DIR, STAGE_DIR, RAW_DIR, DB_PATH.parent, ACTION_LOG.parent]:
        d.mkdir(parents=True, exist_ok=True)


def log_action(command: str, result: str, detail: str) -> None:
    ensure_dirs()
    line = f"[{datetime.now().isoformat(timespec='seconds')}] cmd={command} result={result} detail={detail}\n"
    if ACTION_LOG.exists():
        existing = ACTION_LOG.read_text(encoding="utf-8")
        ACTION_LOG.write_text(existing + line, encoding="utf-8")
    else:
        ACTION_LOG.write_text(line, encoding="utf-8")


def _sanitize_float(value, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def set_runtime_params(params: dict) -> dict:
    if not isinstance(params, dict):
        return get_runtime_params()

    _RUNTIME_PARAMS["last_year_avg_growth"] = _sanitize_float(
        params.get("last_year_avg_growth", _RUNTIME_PARAMS["last_year_avg_growth"]),
        _RUNTIME_PARAMS["last_year_avg_growth"],
    )
    _RUNTIME_PARAMS["expected_growth"] = _sanitize_float(
        params.get("expected_growth", _RUNTIME_PARAMS["expected_growth"]),
        _RUNTIME_PARAMS["expected_growth"],
    )
    _RUNTIME_PARAMS["threshold_val"] = _sanitize_float(
        params.get("threshold_val", _RUNTIME_PARAMS["threshold_val"]),
        _RUNTIME_PARAMS["threshold_val"],
    )
    _RUNTIME_PARAMS["weight_totalRevenue"] = _sanitize_float(
        params.get("weight_totalRevenue", _RUNTIME_PARAMS["weight_totalRevenue"]),
        _RUNTIME_PARAMS["weight_totalRevenue"],
    )
    _RUNTIME_PARAMS["weight_sellingGeneralAndAdministrative"] = _sanitize_float(
        params.get(
            "weight_sellingGeneralAndAdministrative",
            _RUNTIME_PARAMS["weight_sellingGeneralAndAdministrative"],
        ),
        _RUNTIME_PARAMS["weight_sellingGeneralAndAdministrative"],
    )
    budget_amount = str(params.get("budget_amount", _RUNTIME_PARAMS["budget_amount"]) or "").strip()
    _RUNTIME_PARAMS["budget_amount"] = budget_amount
    return get_runtime_params()


def get_runtime_params() -> dict:
    return dict(_RUNTIME_PARAMS)


def _runtime_env_overrides(extra: dict | None = None) -> dict:
    weights = {
        "totalRevenue": _RUNTIME_PARAMS["weight_totalRevenue"],
        "sellingGeneralAndAdministrative": _RUNTIME_PARAMS["weight_sellingGeneralAndAdministrative"],
    }
    env = {
        "R2R_LAST_YEAR_AVG_GROWTH": str(_RUNTIME_PARAMS["last_year_avg_growth"]),
        "R2R_EXPECTED_GROWTH": str(_RUNTIME_PARAMS["expected_growth"]),
        "R2R_THRESHOLD_VAL": str(_RUNTIME_PARAMS["threshold_val"]),
        "R2R_ACCOUNT_WEIGHTS": json.dumps(weights, ensure_ascii=False),
    }
    if _RUNTIME_PARAMS["budget_amount"]:
        env["R2R_BUDGET_AMOUNT"] = str(_RUNTIME_PARAMS["budget_amount"])
    if extra:
        env.update(extra)
    return env


def run_script(script_path: Path, env_overrides: dict | None = None) -> Tuple[bool, str]:
    if not script_path.exists():
        return False, f"未找到脚本: {script_path.name}"

    run_env = os.environ.copy()
    run_env.update(_runtime_env_overrides(env_overrides))

    proc = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        cwd=str(BASE_DIR),
        env=run_env,
    )
    ok = proc.returncode == 0
    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    msg = out if ok else (err or out or f"脚本执行失败: {script_path.name}")
    return ok, msg


def get_latest_report_path() -> Path | None:
    reports = sorted(REPORT_DIR.glob("Financial_Report_*.xlsx"))
    return reports[-1] if reports else None


def get_status() -> dict:
    db_status = "Pending"
    period_key = datetime.now().strftime("%Y-%m")
    if DB_PATH.exists():
        try:
            with sqlite3.connect(DB_PATH) as conn:
                row = conn.execute(
                    "SELECT period_key, status FROM workflow_status ORDER BY status_id DESC LIMIT 1"
                ).fetchone()
                if row:
                    period_key, db_status = row[0], (row[1] or "Pending")
        except Exception:
            pass

    effective = "Approved" if str(db_status).lower() == "approved" else "Pending"
    return {
        "db_status": db_status,
        "effective": effective,
        "period_key": period_key,
    }


def summarize_result() -> str:
    row_count = 0
    latest_mom = None

    if DB_PATH.exists():
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT COUNT(*) FROM fact_financials").fetchone()
            row_count = int(row[0]) if row else 0

    pbi_dataset = PBI_DIR / "pbi_dataset.csv"
    if pbi_dataset.exists():
        df = pd.read_csv(pbi_dataset)
        if "revenue_mom_pct" in df.columns:
            s = df["revenue_mom_pct"].dropna()
            if not s.empty:
                latest_mom = float(s.iloc[-1])

    if latest_mom is None:
        return f"流程完成：已处理 {row_count} 条财务记录。"

    return f"流程完成：已处理 {row_count} 条财务记录，最新收入环比 {latest_mom * 100:.2f}%。"


def _create_schema_if_needed(conn: sqlite3.Connection) -> None:
    if SCHEMA_SQL.exists():
        conn.executescript(SCHEMA_SQL.read_text(encoding="utf-8"))
    else:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS dim_company (company_id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT UNIQUE NOT NULL, company_name TEXT, currency TEXT);
            CREATE TABLE IF NOT EXISTS dim_date (date_id INTEGER PRIMARY KEY AUTOINCREMENT, fiscal_date TEXT UNIQUE NOT NULL, fiscal_year INTEGER, fiscal_quarter INTEGER, fiscal_month INTEGER, period_key TEXT);
            CREATE TABLE IF NOT EXISTS dim_statement (statement_id INTEGER PRIMARY KEY AUTOINCREMENT, statement_type TEXT UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS dim_account (account_id INTEGER PRIMARY KEY AUTOINCREMENT, account_name TEXT UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS fact_financials (fact_id INTEGER PRIMARY KEY AUTOINCREMENT, company_id INTEGER NOT NULL, date_id INTEGER NOT NULL, statement_id INTEGER NOT NULL, account_id INTEGER NOT NULL, report_level TEXT NOT NULL, amount REAL NOT NULL, source_system TEXT, load_time TEXT, UNIQUE(company_id, date_id, statement_id, account_id, report_level));
            CREATE TABLE IF NOT EXISTS workflow_status (status_id INTEGER PRIMARY KEY AUTOINCREMENT, period_key TEXT NOT NULL, status TEXT NOT NULL, updated_at TEXT NOT NULL, updated_by TEXT NOT NULL, comments TEXT);
            CREATE TABLE IF NOT EXISTS pbi_refresh_log (refresh_id INTEGER PRIMARY KEY AUTOINCREMENT, period_key TEXT NOT NULL, trigger_source TEXT NOT NULL, status TEXT NOT NULL, message TEXT, refreshed_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS run_log (run_id INTEGER PRIMARY KEY AUTOINCREMENT, node_name TEXT NOT NULL, result TEXT NOT NULL, message TEXT, created_at TEXT NOT NULL);
            """
        )


def _normalize_account_name(account_name: str) -> str:
    raw = str(account_name or "").strip()
    if not raw:
        return "totalRevenue"

    key = re.sub(r"[\s_\-（）()]+", "", raw).lower()
    if key in STANDARD_ACCOUNT_MAP:
        return STANDARD_ACCOUNT_MAP[key]

    candidates = list(STANDARD_ACCOUNT_MAP.keys())
    matched = get_close_matches(key, candidates, n=1, cutoff=0.72)
    if matched:
        return STANDARD_ACCOUNT_MAP[matched[0]]

    return raw


def _normalize_general_dataframe(df: pd.DataFrame, source_name: str, default_ticker: str = "MANUAL") -> pd.DataFrame:
    work = df.copy()
    cols = list(work.columns)
    lower_map = {c: str(c).lower() for c in cols}

    def pick(keywords: list[str]) -> str | None:
        for c in cols:
            lc = lower_map[c]
            if any(k in lc for k in keywords):
                return c
        return None

    date_col = pick(["fiscal_date", "date", "timestamp", "month", "period", "日期", "月份", "期间", "会计期间"])
    amount_col = pick(["amount", "revenue", "sales", "income", "close", "price", "金额", "收入", "值", "数值"])
    account_col = pick(["account", "item", "科目", "项目", "指标"])
    ticker_col = pick(["ticker", "symbol", "股票", "代码"])

    if date_col is None:
        date_col = cols[0] if cols else None

    if amount_col is None:
        numeric_candidates = [c for c in cols if pd.to_numeric(work[c], errors="coerce").notna().sum() > 0]
        amount_col = numeric_candidates[0] if numeric_candidates else None

    if date_col is None or amount_col is None:
        return pd.DataFrame(columns=list(REQUIRED_IMPORT_COLS) + ["currency"])

    inferred_ticker = str(default_ticker)
    if not ticker_col and source_name.startswith(("URL:", "API:")):
        try:
            parsed = urlparse(source_name.split(":", 1)[1])
            symbol = (parse_qs(parsed.query).get("symbol", [""])[0] or "").strip().upper()
            if symbol:
                inferred_ticker = symbol
        except Exception:
            pass

    out = pd.DataFrame(index=work.index)
    out["ticker"] = work[ticker_col].astype(str) if ticker_col else inferred_ticker
    out["statement_type"] = "income_statement"
    out["report_level"] = "quarterly"
    out["fiscal_date"] = pd.to_datetime(work[date_col], errors="coerce")
    out["account_name"] = work[account_col].astype(str) if account_col else "totalRevenue"
    out["amount"] = pd.to_numeric(work[amount_col], errors="coerce")
    out["currency"] = "USD"

    out = out.dropna(subset=["fiscal_date", "amount"])
    if out.empty:
        return out

    out["account_name"] = out["account_name"].map(_normalize_account_name)
    out["source_system"] = source_name
    out["load_time"] = datetime.now().isoformat(timespec="seconds")
    return out


def _load_dataframe_to_db(df: pd.DataFrame, source_name: str) -> Tuple[bool, str]:
    missing = REQUIRED_IMPORT_COLS - set(df.columns)
    if missing:
        auto_df = _normalize_general_dataframe(df, source_name)
        if auto_df.empty:
            return False, f"文件缺少必要字段且无法自动识别：{', '.join(sorted(missing))}"
        work = auto_df
    else:
        work = df.copy()

    work["fiscal_date"] = pd.to_datetime(work["fiscal_date"], errors="coerce")
    work["amount"] = pd.to_numeric(work["amount"], errors="coerce")
    work = work.dropna(subset=["fiscal_date", "amount", "ticker", "statement_type", "report_level", "account_name"])
    if work.empty:
        return False, "导入后无有效数据，请检查文件内容。"

    work["account_name"] = work["account_name"].map(_normalize_account_name)

    if "currency" not in work.columns:
        work["currency"] = "USD"
    work["currency"] = work["currency"].fillna("USD")
    work["source_system"] = source_name
    work["load_time"] = datetime.now().isoformat(timespec="seconds")

    ensure_dirs()
    STAGE_DIR.mkdir(parents=True, exist_ok=True)
    stage_file = STAGE_DIR / "manual_import_standardized.csv"
    work.to_csv(stage_file, index=False, encoding="utf-8-sig")

    with sqlite3.connect(DB_PATH) as conn:
        _create_schema_if_needed(conn)
        cur = conn.cursor()
        conn.execute("BEGIN")
        try:
            for ticker, grp in work.groupby("ticker"):
                currency = grp["currency"].mode().iat[0] if not grp["currency"].mode().empty else "USD"
                cur.execute(
                    "INSERT OR IGNORE INTO dim_company (ticker, company_name, currency) VALUES (?, ?, ?)",
                    (str(ticker), str(ticker), str(currency)),
                )

            for st in sorted(work["statement_type"].astype(str).unique()):
                cur.execute("INSERT OR IGNORE INTO dim_statement (statement_type) VALUES (?)", (st,))

            for acc in sorted(work["account_name"].astype(str).unique()):
                cur.execute("INSERT OR IGNORE INTO dim_account (account_name) VALUES (?)", (acc,))

            date_df = work[["fiscal_date"]].drop_duplicates().copy()
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

            company_map = dict(cur.execute("SELECT ticker, company_id FROM dim_company").fetchall())
            statement_map = dict(cur.execute("SELECT statement_type, statement_id FROM dim_statement").fetchall())
            account_map = dict(cur.execute("SELECT account_name, account_id FROM dim_account").fetchall())
            date_map = dict(cur.execute("SELECT fiscal_date, date_id FROM dim_date").fetchall())

            insert_sql = """
            INSERT OR REPLACE INTO fact_financials
            (company_id, date_id, statement_id, account_id, report_level, amount, source_system, load_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """

            recs = []
            for _, r in work.iterrows():
                dkey = r["fiscal_date"].strftime("%Y-%m-%d")
                recs.append(
                    (
                        company_map[str(r["ticker"])],
                        date_map[dkey],
                        statement_map[str(r["statement_type"])],
                        account_map[str(r["account_name"])],
                        str(r["report_level"]),
                        float(r["amount"]),
                        str(r["source_system"]),
                        str(r["load_time"]),
                    )
                )

            cur.executemany(insert_sql, recs)
            cur.execute(
                "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
                (
                    "Manual_Import",
                    "SUCCESS",
                    f"Imported {len(work)} rows from {source_name}",
                    datetime.now().isoformat(timespec="seconds"),
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    return True, f"导入成功：共 {len(work)} 条记录，已更新到账务数据。"


def _read_input_file(file_path: Path) -> tuple[bool, pd.DataFrame | None, str]:
    if not file_path.exists():
        return False, None, f"未找到文件：{file_path}"

    try:
        if file_path.suffix.lower() in [".xlsx", ".xls"]:
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
    except Exception as e:
        return False, None, f"文件读取失败：{e}"
    return True, df, "OK"


def stage_dataframe(df: pd.DataFrame, source_name: str) -> Tuple[bool, str]:
    global _IMPORT_CONFIRMED
    normalized = _normalize_general_dataframe(df, source_name=source_name)
    if normalized.empty and REQUIRED_IMPORT_COLS.issubset(set(df.columns)):
        normalized = df.copy()
        normalized["fiscal_date"] = pd.to_datetime(normalized["fiscal_date"], errors="coerce")
        normalized["amount"] = pd.to_numeric(normalized["amount"], errors="coerce")
        normalized = normalized.dropna(subset=["fiscal_date", "amount"])
        normalized["account_name"] = normalized["account_name"].map(_normalize_account_name)

    if normalized.empty:
        return False, "暂存失败：无法识别有效日期/金额列。"

    with _BATCH_LOCK:
        _STAGED_BATCHES.append(normalized)
        _STAGED_SOURCES.append(source_name)
        _IMPORT_CONFIRMED = False
        total_rows = sum(len(x) for x in _STAGED_BATCHES)
        batch_cnt = len(_STAGED_BATCHES)
    return True, f"已暂存成功：第 {batch_cnt} 批，当前累计 {total_rows} 行。"


def stage_import_file(file_path: Path) -> Tuple[bool, str]:
    ok, df, msg = _read_input_file(file_path)
    if not ok or df is None:
        return False, msg
    return stage_dataframe(df, source_name=f"Staged:{file_path.name}")


def get_staged_summary() -> dict:
    with _BATCH_LOCK:
        return {
            "batch_count": len(_STAGED_BATCHES),
            "row_count": int(sum(len(x) for x in _STAGED_BATCHES)),
            "sources": list(_STAGED_SOURCES),
            "confirmed": bool(_IMPORT_CONFIRMED),
        }


def reset_staged_imports() -> None:
    global _IMPORT_CONFIRMED
    with _BATCH_LOCK:
        _STAGED_BATCHES.clear()
        _STAGED_SOURCES.clear()
        _IMPORT_CONFIRMED = False


def confirm_staged_import() -> Tuple[bool, str]:
    global _IMPORT_CONFIRMED
    with _BATCH_LOCK:
        if not _STAGED_BATCHES:
            return False, "暂无可确认批次，请先上传文件。"
        merged = pd.concat(_STAGED_BATCHES, ignore_index=True)
        sources = list(_STAGED_SOURCES)

    ok, msg = _load_dataframe_to_db(merged, source_name=f"BatchConfirm:{'|'.join(sources)}")
    if not ok:
        return False, msg

    with _BATCH_LOCK:
        _STAGED_BATCHES.clear()
        _STAGED_SOURCES.clear()
        _IMPORT_CONFIRMED = True

    return True, f"{msg} 已完成批次确认并锁定流程。"


def _has_fact_data() -> bool:
    if not DB_PATH.exists():
        return False
    try:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute("SELECT COUNT(*) FROM fact_financials").fetchone()
            return bool(row and int(row[0]) > 0)
    except Exception:
        return False


def _is_flow_unlocked() -> bool:
    return _IMPORT_CONFIRMED or _has_fact_data()


def import_csv_file(file_path: Path) -> Tuple[bool, str]:
    ok, df, msg = _read_input_file(file_path)
    if not ok or df is None:
        return False, msg

    ret = _load_dataframe_to_db(df, source_name=f"ManualFile:{file_path.name}")
    if ret[0]:
        global _IMPORT_CONFIRMED
        _IMPORT_CONFIRMED = True
    return ret


def import_csv_from_url(url: str) -> Tuple[bool, str]:
    try:
        s = requests.Session()
        s.trust_env = False
        resp = s.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        return False, f"URL 数据获取失败：{e}"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = RAW_DIR / f"url_import_{stamp}.csv"
    out_file.write_bytes(resp.content)

    try:
        df = pd.read_csv(out_file)
    except Exception as e:
        return False, f"URL 数据不是可识别的 CSV：{e}"

    ok, msg = stage_dataframe(df, source_name=f"URL:{url}")
    if ok:
        return True, f"{msg} 来源文件：{out_file}。请点击“确认完成导入”后入库。"
    return False, msg


def import_json_from_api_url(url: str) -> Tuple[bool, str]:
    try:
        s = requests.Session()
        s.trust_env = False
        resp = s.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return False, f"API 数据获取失败或不是有效 JSON：{e}"

    if isinstance(data, dict) and isinstance(data.get("data"), list):
        rows = data["data"]
    elif isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        rows = [data]
    else:
        return False, "JSON 结构无法识别，请提供对象或数组结构。"

    df = pd.DataFrame(rows)
    if df.empty:
        return False, "JSON 中没有可导入的数据行。"

    ok, msg = stage_dataframe(df, source_name=f"API:{url}")
    if ok:
        return True, f"{msg} 请点击“确认完成导入”后入库。"
    return False, msg


def run_extract_for_ticker(ticker: str) -> Tuple[bool, str]:
    t = ticker.strip().upper()
    if not re.fullmatch(r"[A-Z.\-]{1,10}", t):
        return False, "股票代码格式不正确，例如 IBM、AAPL。"

    ok, msg = run_script(SCRIPT_01, env_overrides={"R2R_TICKERS": t})
    if ok:
        global _IMPORT_CONFIRMED
        _IMPORT_CONFIRMED = True
        return True, f"云端取数完成：已按股票代码 {t} 更新账务数据。"
    return False, f"股票代码 {t} 取数失败：{msg[:260]}"


def cloud_ingest(input_text: str) -> Tuple[bool, str]:
    text = input_text.strip()
    if not text:
        return False, "请输入股票代码、ERP账套名称或API地址。"

    if re.match(r"^https?://", text, flags=re.IGNORECASE):
        lower_text = text.lower()
        if "datatype=csv" in lower_text or lower_text.endswith(".csv"):
            ok_csv, msg_csv = import_csv_from_url(text)
            if ok_csv:
                return ok_csv, f"云端 URL CSV 已进入暂存池。{msg_csv}"
            return False, f"URL CSV 导入失败：{msg_csv}"

        if text.lower().endswith(".json"):
            return import_json_from_api_url(text)

        ok_json, msg_json = import_json_from_api_url(text)
        if ok_json:
            return ok_json, f"云端 API JSON 已进入暂存池。{msg_json}"

        ok_csv, msg_csv = import_csv_from_url(text)
        if ok_csv:
            return ok_csv, f"云端 URL CSV 已进入暂存池。{msg_csv}"

        return False, f"URL 导入失败：JSON尝试({msg_json})；CSV尝试({msg_csv})"

    if re.fullmatch(r"[A-Za-z.\-]{1,10}", text):
        return run_extract_for_ticker(text)

    return False, f"已识别 ERP账套名称“{text}”。请上传该账套导出的 CSV/Excel，或提供可访问 API 地址。"


def suggest_mapping(columns: list[str]) -> dict:
    cols = [str(c) for c in columns]
    lower = {c: c.lower() for c in cols}

    def pick(keys: list[str]) -> str | None:
        for c in cols:
            if any(k in lower[c] for k in keys):
                return c
        return None

    return {
        "fiscal_date": pick(["date", "month", "period", "日期", "月份", "期间"]),
        "amount": pick(["sales", "revenue", "amount", "收入", "金额", "值"]),
        "account_name": pick(["account", "item", "科目", "项目", "指标", "费用"]),
        "ticker": pick(["ticker", "symbol", "股票", "代码"]),
    }


def standardize_with_mapping(df: pd.DataFrame, mapping: dict, default_ticker: str = "MANUAL") -> pd.DataFrame:
    m_date = mapping.get("fiscal_date")
    m_amt = mapping.get("amount")
    m_acc = mapping.get("account_name")
    m_ticker = mapping.get("ticker")

    out = pd.DataFrame(index=df.index)
    out["ticker"] = df[m_ticker].astype(str) if m_ticker in df.columns else str(default_ticker)
    out["statement_type"] = "income_statement"
    out["report_level"] = "quarterly"
    out["fiscal_date"] = pd.to_datetime(df[m_date], errors="coerce") if m_date in df.columns else pd.NaT
    out["account_name"] = df[m_acc].astype(str) if m_acc in df.columns else "totalRevenue"
    out["amount"] = pd.to_numeric(df[m_amt], errors="coerce") if m_amt in df.columns else pd.NA
    out["currency"] = "USD"

    out = out.dropna(subset=["fiscal_date", "amount"])
    if not out.empty:
        out["account_name"] = out["account_name"].map(_normalize_account_name)
    return out


def run_data_with_repair() -> Tuple[bool, str]:
    ok, _ = run_script(SCRIPT_01)
    if ok:
        return True, "账务数据已准备完成。"

    retry_ok, retry_msg = run_script(SCRIPT_01)
    if retry_ok:
        return True, "首次抓取失败，已自动重试并成功完成数据准备。"

    return False, f"数据准备失败，已自动重试一次仍未成功。错误信息：{retry_msg[:280]}"


def confirm_approval(updated_by: str = "web_ui", comments: str = "Approved by web action") -> Tuple[bool, str]:
    ensure_dirs()
    period_key = datetime.now().strftime("%Y-%m")
    with sqlite3.connect(DB_PATH) as conn:
        _create_schema_if_needed(conn)
        conn.execute(
            "INSERT INTO workflow_status (period_key, status, updated_at, updated_by, comments) VALUES (?, ?, ?, ?, ?)",
            (period_key, "Approved", datetime.now().isoformat(timespec="seconds"), updated_by, comments),
        )
        conn.execute(
            "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
            ("Approval_Signal_API", "SUCCESS", "workflow_status set to Approved by API", datetime.now().isoformat(timespec="seconds")),
        )
        conn.commit()
    return True, "审批状态已更新为 Approved。"


def handle_start_close() -> str:
    if not _is_flow_unlocked():
        return "流程已锁定：请先上传并点击“确认完成导入”。"

    ok, prep_msg = run_data_with_repair()
    if not ok:
        return prep_msg

    status = get_status()
    if status["effective"] == "Pending":
        ok_n08, msg_n08 = run_script(SCRIPT_02)
        if not ok_n08:
            return f"数据准备已完成，但初步报表生成失败：{msg_n08[:260]}"
        latest_report = get_latest_report_path()
        report_path = str(latest_report) if latest_report else "(未找到报表文件)"
        return f"{prep_msg} 已生成初步报表。文件路径：{report_path}。当前为待审批，请审批后点击“审批通过，更新看板”。"

    ok_n09, msg_n09 = run_script(SCRIPT_03)
    if not ok_n09:
        return f"数据准备已完成，但看板更新失败：{msg_n09[:260]}"
    return summarize_result()


def handle_status() -> str:
    status = get_status()
    if status["effective"] == "Approved":
        return "当前审批状态：已通过。你可以点击“审批通过，更新看板”发布数据。"
    return "当前审批状态：待审批。请先审批，再点击“审批通过，更新看板”。"


def handle_generate_report() -> str:
    if not _is_flow_unlocked():
        return "流程已锁定：请先上传并点击“确认完成导入”。"

    ok, msg = run_script(SCRIPT_02)
    if not ok:
        return f"报表生成失败：{msg[:260]}"
    latest_report = get_latest_report_path()
    report_path = str(latest_report) if latest_report else "(未找到报表文件)"
    return f"初步报表已生成。文件路径：{report_path}。"


def handle_publish() -> str:
    if not _is_flow_unlocked():
        return "流程已锁定：请先上传并点击“确认完成导入”。"

    ok_sig, msg_sig = confirm_approval(updated_by="web_button", comments="Approved by publish action")
    if not ok_sig:
        return f"审批信号设置失败：{msg_sig[:240]}"

    ok_ref, msg_ref = run_script(SCRIPT_03)
    if not ok_ref:
        return f"审批已写入，但看板更新失败：{msg_ref[:260]}"

    summary = summarize_result()
    alert_text = ""
    dataset = PBI_DIR / "pbi_dataset.csv"
    threshold = _sanitize_float(_RUNTIME_PARAMS.get("threshold_val", 10.0), 10.0) / 100.0
    if dataset.exists():
        df = pd.read_csv(dataset)
        if "revenue_mom_pct" in df.columns:
            hit = pd.to_numeric(df["revenue_mom_pct"], errors="coerce").abs() > threshold
            if hit.fillna(False).any():
                alert_text = f" 已检测到收入波动超过 {threshold * 100:.0f}%，请重点复核。"

    return f"数据已发布到看板。{summary}{alert_text}"


def handle_import_file_command(command: str) -> str:
    m = re.search(r"导入\s+(.+)$", command)
    if not m:
        return "请按“导入 文件路径”输入，例如：导入 D:\\R2R_Automation\\sample.csv"
    path = Path(m.group(1).strip().strip('"'))
    ok, msg = import_csv_file(path)
    return msg if ok else msg


def handle_import_url_command(command: str) -> str:
    m = re.search(r"导入网址\s+(.+)$", command)
    if not m:
        return "请按“导入网址 URL”输入，例如：导入网址 https://example.com/finance.csv"
    url = m.group(1).strip()
    ok, msg = import_csv_from_url(url)
    return msg if ok else msg


def route_command(command: str) -> str:
    c = command.strip()
    if not c:
        return "请输入指令，例如：开始月度结账、查看审批状态、审批通过，更新看板。"

    if c in {"帮助", "help", "?"}:
        return "你可以这样做：1. 开始月度结账 2. 查看审批状态 3. 审批通过，更新看板 4. 导入数据"

    if c in {"开始月度结账", "开始结账"}:
        return handle_start_close()

    if c in {"查看审批状态", "审批状态"}:
        return handle_status()

    if c in {"生成本月初步报表", "生成报表", "检查费用"}:
        return handle_generate_report()

    if c in {"审批通过，更新看板", "审批通过", "发布数据", "刷新报表"}:
        return handle_publish()

    if c.startswith("云端取数 "):
        text = c.replace("云端取数", "", 1).strip()
        ok, msg = cloud_ingest(text)
        return msg if ok else msg

    if c.startswith("导入网址 "):
        return handle_import_url_command(c)

    if c.startswith("导入 "):
        return handle_import_file_command(c)

    if c.startswith("获取 ") and "数据" in c:
        return handle_start_close()

    if c in {"退出", "exit", "quit"}:
        return "exit"

    return "我还不理解这个指令。建议点击：开始月度结账 / 查看审批状态 / 审批通过，更新看板"


def interactive_loop() -> None:
    print("旺财已启动。")
    print("可用指令：开始月度结账、查看审批状态、生成本月初步报表、审批通过，更新看板、云端取数 <股票代码|ERP|URL>、导入 <文件路径>、退出")
    while True:
        cmd = input("\n你：").strip()
        result = route_command(cmd)
        if result == "exit":
            print("旺财：已结束本次操作。")
            break
        print(f"旺财：{result}")
        log_action(cmd, "OK", result)


def main() -> None:
    parser = argparse.ArgumentParser(description="R2R Finance Commander")
    parser.add_argument("command", nargs="?", help="单次执行指令，例如：开始月度结账")
    args = parser.parse_args()

    if args.command:
        result = route_command(args.command)
        if result == "exit":
            print("旺财：已结束本次操作。")
            return
        print(result)
        log_action(args.command, "OK", result)
    else:
        interactive_loop()


if __name__ == "__main__":
    main()
