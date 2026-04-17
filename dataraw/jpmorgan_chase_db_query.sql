-----------------------------------------
-- William Lorenzo
-- 01/19/25
-- JPMorgan Chase Financial Data Analysis
-- MySQL Workbench 8.0
-----------------------------------------

CREATE DATABASE chase_financials;

USE chase_financials;

CREATE TABLE income_statement (
	period_end DATE,
    revenue DECIMAL(15, 2),
    interest_income DECIMAL(15, 2),
    interest_expense DECIMAL(15, 2),
    net_income DECIMAL(15, 2),
    PRIMARY KEY (period_end)
);
    
CREATE TABLE balance_sheet(
	date DATE,
    total_assets DECIMAL(15, 2),
    total_liabilities DECIMAL(15, 2),
    stockholders_equity DECIMAL(15, 2),
    PRIMARY KEY (date)
);

CREATE TABLE cash_flow (
	period_end DATE,
    operating_cash_flow DECIMAL(15, 2),
    investing_cash_flow DECIMAL(15, 2),
    financing_cash_flow DECIMAL(15, 2),
    net_cash_flow DECIMAL(15, 2),
    PRIMARY KEY (period_end)
);

-- Unpacking Income Statement
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/jpmorgan_chase_income_statement.csv'
INTO TABLE income_statement
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Unpacking Balance Sheet
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/jpmorgan_chase_balance_sheet.csv'
INTO TABLE balance_sheet
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Unpacking Cash Flow
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/jpmorgan_chase_cash_flow.csv'
INTO TABLE cash_flow
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Using SQL queries to analyze the data

-- Revenue Growth Trend
SELECT period_end, revenue,
	COALESCE((revenue - LAG(revenue) OVER (ORDER BY period_end)) / LAG(revenue) OVER (ORDER BY period_end) * 100, 0) AS revenue_growth
FROM income_statement;

-- Debt-to-Equity Ratio
SELECT date, total_liabilities / stockholders_equity AS debt_to_equity_ratio
FROM balance_sheet;

-- Net Cash Flow Analysis
SELECT period_end, net_cash_flow
FROM cash_flow
WHERE net_cash_flow < 0;