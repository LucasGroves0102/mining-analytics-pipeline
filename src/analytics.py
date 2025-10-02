from __future__ import annotations
from pathlib import Path
import os, sqlite3
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

def get_db_path() -> Path:
    # Read fresh each call so tests can set RT_DB_PATH after import
    return Path(os.getenv("RT_DB_PATH", str(ROOT / "db" / "rt_mining.db")))

def _read(sql: str, params: dict | tuple = ()):
    with sqlite3.connect(get_db_path()) as con:
        return pd.read_sql_query(sql, con, params=params)

def daily_kpis(date_str: str) -> pd.DataFrame:
    """
    Per-equipment KPIs for a given YYYY-MM-DD:
      - utilization_pct
      - total_tonnage_t
      - total_energy_kwh
      - specific_energy_kwhpt
      - avg_throughput_tph
      - meets_min_throughput (vs benchmarks)
      - meets_max_specific_energy (vs benchmarks)
    """
    tele = _read("""
        SELECT t.*, b.min_throughput_tph, b.max_specific_energy_kwhpt
        FROM fact_telemetry t
        LEFT JOIN benchmarks b USING (equipment_id)
        WHERE date(substr(timestamp,1,10)) = date(:d)
        ORDER BY equipment_id, timestamp
    """, {"d": date_str})

    if tele.empty:
        return pd.DataFrame(columns=[
            "equipment_id","utilization_pct","total_tonnage_t","total_energy_kwh",
            "specific_energy_kwhpt","avg_throughput_tph","meets_min_throughput","meets_max_specific_energy"
        ])

    # 5-minute cadence
    hours = 5.0 / 60.0
    tele["tons_this_interval"] = tele["throughput_tph"] * hours
    tele["kwh_this_interval"]  = tele["power_kw"] * hours

    agg = tele.groupby(["equipment_id","min_throughput_tph","max_specific_energy_kwhpt"], as_index=False).agg(
        intervals=("status","size"),
        run_intervals=("status", lambda s: (s==1).sum()),
        total_tonnage_t=("tons_this_interval","sum"),
        total_energy_kwh=("kwh_this_interval","sum"),
        avg_throughput_tph=("throughput_tph","mean"),
    )

    agg["utilization_pct"] = 100.0 * agg["run_intervals"] / agg["intervals"]
    agg["specific_energy_kwhpt"] = agg["total_energy_kwh"] / agg["total_tonnage_t"].replace(0, float("nan"))
    agg["meets_min_throughput"] = agg["avg_throughput_tph"] >= agg["min_throughput_tph"]
    agg["meets_max_specific_energy"] = agg["specific_energy_kwhpt"] <= agg["max_specific_energy_kwhpt"]

    return agg[[
        "equipment_id","utilization_pct","total_tonnage_t","total_energy_kwh",
        "specific_energy_kwhpt","avg_throughput_tph","meets_min_throughput","meets_max_specific_energy"
    ]].sort_values("equipment_id").reset_index(drop=True)

def downtime_summary(date_str: str) -> pd.DataFrame:
    """Downtime rollup for the day."""
    dt = _read("""
        SELECT equipment_id, start_ts, end_ts, duration_min, reason
        FROM fact_downtime
        WHERE date(substr(start_ts,1,10)) = date(:d)
        ORDER BY equipment_id, start_ts
    """, {"d": date_str})
    if dt.empty:
        return pd.DataFrame(columns=["equipment_id","events","total_downtime_min"])
    return (dt.groupby("equipment_id", as_index=False)
              .agg(events=("duration_min","size"), total_downtime_min=("duration_min","sum"))
              .sort_values("equipment_id"))

if __name__ == "__main__":
    day = "2025-08-01"
    print("\n=== KPIs ===")
    print(daily_kpis(day).to_string(index=False))
    print("\n=== Downtime ===")
    print(downtime_summary(day).to_string(index=False))
