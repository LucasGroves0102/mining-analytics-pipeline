from __future__ import annotations
from pathlib import Path 
import numpy as np 
import pandas as pd 
from datetime import datetime, timedelta

ROOT = Path(__file__).resolve().parent.parent 
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)
np.random.seed(7)


equip = pd.DataFrame([
    {"equipment_id":"CR-01","area":"Crusher","commissioned_date":"2020-01-01","nameplate_tph":450},
    {"equipment_id":"SAG-01","area":"SAG_Mill","commissioned_date":"2020-01-01","nameplate_tph":300}
])
equip.to_csv(RAW/"equipment_metadata.csv", index=False)


# telemtry: 1 day, 5-min cadence
start = datetime(2025, 8, 1, 0, 0, 0)
times = [start + timedelta(minutes=5*i) for i in range(288)]
rows = []

for eid, area, base_tph in [("CR-01","Crusher",380), ("SAG-01","SAG_Mill",260)]:
    nameplate = int(equip.loc[equip.equipment_id==eid,"nameplate_tph"].iloc[0])
    for t in times:
        diur = np.sin(2*np.pi*((t.hour*60+t.minute)/1440))
        tph = np.clip(base_tph*(1+0.05*diur)+np.random.normal(0,12), 0, nameplate*1.05)
        pkw = np.clip((400 if area=="Crusher" else 1200) + 3.0*tph + np.random.normal(0,15), 0, None)
        rows.append({
            "timestamp": t.isoformat(),
            "equipment_id": eid,
            "area": area,
            "throughput_tph": round(float(tph),3),
            "power_kw": round(float(pkw),3),
            "temperature_c": round(35+5*diur+np.random.normal(0,1.2),3),
            "pressure_kpa": round(200+20*diur+np.random.normal(0,4),3),
            "status": 1
        })
telemetry = pd.DataFrame(rows)
telemetry.to_csv(RAW/"telemetry.csv", index=False)

# --- downtime (one short event)
pd.DataFrame([{
    "equipment_id":"CR-01",
    "start_ts": (start + timedelta(hours=6)).isoformat(),
    "end_ts":   (start + timedelta(hours=6, minutes=30)).isoformat(),
    "duration_min": 30.0,
    "reason":"Maintenance"
}]).to_csv(RAW/"downtime_events.csv", index=False)

# --- lab assays (daily)
pd.DataFrame([{
    "date":"2025-08-01",
    "ore_grade_pct": 0.52,
    "moisture_pct": 0.09,
    "bond_work_index_kwhpt": 14.8
}]).to_csv(RAW/"lab_assays.csv", index=False)

# --- power price
pd.DataFrame([{"date":"2025-08-01","usd_per_mwh": 57.25}]).to_csv(RAW/"power_prices.csv", index=False)

# --- benchmarks
pd.DataFrame([
    {"equipment_id":"CR-01","target_utilization_pct":92.0,"max_specific_energy_kwhpt":15.0,"min_throughput_tph":270},
    {"equipment_id":"SAG-01","target_utilization_pct":92.0,"max_specific_energy_kwhpt":22.0,"min_throughput_tph":180},
]).to_csv(RAW/"benchmarks.csv", index=False)

print("created data written to", RAW)