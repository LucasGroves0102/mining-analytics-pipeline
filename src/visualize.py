from __future__ import annotations
from pathlib import Path 
import sqlite3
import pandas as pd 
import matplotlib.pyplot as plt 

ROOT = Path(__file__).resolve().parent.parent
DB = ROOT / "db" / "rt_mining.db"
OUT = ROOT / "outputs"
OUT.mkdir(parents=True, exist_ok=True)

def _read(sql: str, params: dict | tuple = ()):
    with sqlite3.connect(DB) as con:
        return pd.read_sql_query(sql, con, params=params)

def fetch_timeseries(day: str, equipment_id: str) -> pd.DataFrame:
    df = _read("""
        SELECT timestamp, equipment_id, throughput_tph, power_kw, status
        FROM fact_telemetry
        WHERE date(substr(timestamp,1,10)) = date(:d)
          AND equipment_id = :eid
        ORDER BY timestamp
    """, {"d": day, "eid": equipment_id})
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    # interval = 5 min
    hours = 5.0/60.0
    df["tons"] = df["throughput_tph"] * hours
    df["kwh"]  = df["power_kw"] * hours
    # rolling specific energy (kWh/t) over 1 hour (12 samples)
    roll = 12
    df["roll_spec_energy_kwhpt"] = (df["kwh"].rolling(roll, min_periods=1).sum()
                                     / df["tons"].rolling(roll, min_periods=1).sum().replace(0, pd.NA))
    return df

def plot_throughput_power(df: pd.DataFrame, day: str, equipment_id: str):
    if df.empty:
        print("No data to plot.")
        return
    fig, ax1 = plt.subplots(figsize=(10,4))
    ax1.plot(df["timestamp"], df["throughput_tph"], label="Throughput (tph)")
    ax1.set_xlabel("Time")
    ax1.set_ylabel("Throughput (tph)")
    ax1.tick_params(axis='x', rotation=30)

    ax2 = ax1.twinx()
    ax2.plot(df["timestamp"], df["power_kw"], label="Power (kW)")
    ax2.set_ylabel("Power (kW)")

    fig.suptitle(f"{equipment_id} — Throughput vs Power — {day}")
    fig.tight_layout()
    out = OUT / f"{day}_{equipment_id}_throughput_power.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print("Saved:", out)

def plot_specific_energy(df: pd.DataFrame, day: str, equipment_id: str):
    if df.empty:
        print("No data to plot.")
        return
    fig, ax = plt.subplots(figsize=(10,4))
    ax.plot(df["timestamp"], df["roll_spec_energy_kwhpt"], label="Rolling Specific Energy (kWh/t)")
    ax.set_xlabel("Time")
    ax.set_ylabel("kWh per ton")
    ax.tick_params(axis='x', rotation=30)
    fig.suptitle(f"{equipment_id} — Rolling Specific Energy — {day}")
    fig.tight_layout()
    out = OUT / f"{day}_{equipment_id}_specific_energy.png"
    fig.savefig(out, dpi=140)
    plt.close(fig)
    print("Saved:", out)

def save_daily_summary(day: str):
    df = _read("""
        SELECT equipment_id,
               AVG(throughput_tph) AS avg_tph,
               SUM(throughput_tph)*(5.0/60.0) AS total_tons,
               SUM(power_kw)*(5.0/60.0)       AS total_kwh
        FROM fact_telemetry
        WHERE date(substr(timestamp,1,10)) = date(:d)
        GROUP BY equipment_id
        ORDER BY equipment_id
    """, {"d": day})
    if df.empty:
        return
    df["specific_energy_kwhpt"] = df["total_kwh"] / df["total_tons"].replace(0, pd.NA)
    out = OUT / f"{day}_daily_summary.csv"
    df.to_csv(out, index=False)
    print("Saved:", out)

def main(day: str = "2025-08-01", equipment_ids: list[str] | None = None):
    if equipment_ids is None:
        eq = _read("SELECT equipment_id FROM dim_equipment ORDER BY equipment_id")
        equipment_ids = eq["equipment_id"].tolist()

    for eid in equipment_ids:
        ts = fetch_timeseries(day, eid)
        plot_throughput_power(ts, day, eid)
        plot_specific_energy(ts, day, eid)

    save_daily_summary(day)

if __name__ == "__main__":
    main()