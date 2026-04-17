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
