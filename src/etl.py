from __future__ import annotations

import os
import sys
import sqlite3
from pathlib import Path
import pandas as pd

# import integrity (works in tests and when run as a script)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
try:
    from src.integrity import check_duplicates, check_ranges
except Exception:
    from integrity import check_duplicates, check_ranges

SCHEMA = ROOT / "db" / "schema.sql"

def get_db_path() -> Path:
    # read fresh each call so tests can set RT_DB_PATH after import
    return Path(os.getenv("RT_DB_PATH", str(ROOT / "db" / "rt_mining.db")))

def get_raw_dir() -> Path:
    # read fresh each call so tests can set RT_DATA_DIR after import
    return Path(os.getenv("RT_DATA_DIR", str(ROOT / "data" / "raw")))

def ensure_db() -> None:
    db = get_db_path()
    with sqlite3.connect(db) as con:
        con.executescript(Path(SCHEMA).read_text())
    print("✅ Database schema created at", db)

def load_table(con: sqlite3.Connection, name: str, df: pd.DataFrame, if_exists: str = "replace") -> None:
    df.to_sql(name, con, if_exists=if_exists, index=False)
    print(f"→ loaded {name}: {len(df)} rows")

def load_all() -> None:
    ensure_db()
    db = get_db_path()
    raw = get_raw_dir()

    with sqlite3.connect(db) as con:
        # dim_equipment
        dim_equipment = pd.read_csv(raw / "equipment_metadata.csv")
        load_table(con, "dim_equipment", dim_equipment)

        # fact_telemetry + Data Quality
        tele = pd.read_csv(raw / "telemetry.csv", parse_dates=["timestamp"])
        dups = check_duplicates(tele, ["timestamp", "equipment_id"])
        rng = check_ranges(
            tele,
            {
                "throughput_tph": (0, 1000),
                "power_kw": (0, 5000),
                "temperature_c": (-10, 120),
                "pressure_kpa": (0, 500),
                "status": (0, 1),
            },
        )

        if not dups.empty:
            (
                dups.assign(
                    table_name="fact_telemetry",
                    record_key=dups.astype(str).agg("|".join, axis=1),
                    check_name="duplicate_key",
                    check_result="FAIL",
                    severity="HIGH",
                )[["table_name", "record_key", "check_name", "check_result", "severity"]]
                .to_sql("data_quality", con, if_exists="append", index=False)
            )

        if not rng.empty:
            (
                rng[["timestamp", "equipment_id"]]
                .astype(str)
                .drop_duplicates()
                .assign(
                    table_name="fact_telemetry",
                    record_key=lambda d: d.astype(str).agg("|".join, axis=1),
                    check_name="out_of_range",
                    check_result="FAIL",
                    severity="MEDIUM",
                )[["table_name", "record_key", "check_name", "check_result", "severity"]]
                .to_sql("data_quality", con, if_exists="append", index=False)
            )

        tele["timestamp"] = tele["timestamp"].astype(str)
        load_table(con, "fact_telemetry", tele)

        load_table(con, "fact_downtime", pd.read_csv(raw / "downtime_events.csv"))
        load_table(con, "fact_lab_assays", pd.read_csv(raw / "lab_assays.csv"))
        load_table(con, "fact_power_price", pd.read_csv(raw / "power_prices.csv"))
        load_table(con, "benchmarks", pd.read_csv(raw / "benchmarks.csv"))

if __name__ == "__main__":
    load_all()
