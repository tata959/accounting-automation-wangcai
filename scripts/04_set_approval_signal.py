import os
import sqlite3
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(os.getenv("R2R_BASE_DIR", Path(__file__).resolve().parents[1]))
DB_PATH = BASE_DIR / "data_mart" / "r2r_finance.db"


def main():
    with sqlite3.connect(DB_PATH) as conn:
        period_key = datetime.now().strftime("%Y-%m")
        conn.execute(
            "INSERT INTO workflow_status (period_key, status, updated_at, updated_by, comments) VALUES (?, ?, ?, ?, ?)",
            (period_key, "Approved", datetime.now().isoformat(timespec="seconds"), "finance_manager", "Monthly close approved"),
        )
        conn.execute(
            "INSERT INTO run_log (node_name, result, message, created_at) VALUES (?, ?, ?, ?)",
            (
                "Approval_Signal",
                "SUCCESS",
                "workflow_status set to Approved",
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()

    print("Approval signal written: workflow_status=Approved")


if __name__ == "__main__":
    main()
