import os
from pathlib import Path
import pandas as pd
from src import etl, analytics

def seed_and_load(tmp_path, monkeypatch):
    # minimal data
    raw = tmp_path / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"equipment_id":"CR-01","area":"Crusher","commissioned_date":"2020-01-01","nameplate_tph":450}]).to_csv(raw/"equipment_metadata.csv", index=False)

    # twelve 5-min intervals = 1 hour
    rows = []
    for i in range(12):
        rows.append({"timestamp":f"2025-08-01T00:{i:02d}:00","equipment_id":"CR-01","area":"Crusher","throughput_tph":360,"power_kw":1500,"temperature_c":40,"pressure_kpa":200,"status":1})
    pd.DataFrame(rows).to_csv(raw/"telemetry.csv", index=False)

    pd.DataFrame([{"equipment_id":"CR-01","start_ts":"2025-08-01T06:00:00","end_ts":"2025-08-01T06:30:00","duration_min":30.0,"reason":"Maintenance"}]).to_csv(raw/"downtime_events.csv", index=False)
    pd.DataFrame([{"date":"2025-08-01","ore_grade_pct":0.52,"moisture_pct":0.09,"bond_work_index_kwhpt":14.8}]).to_csv(raw/"lab_assays.csv", index=False)
    pd.DataFrame([{"date":"2025-08-01","usd_per_mwh":57.25}]).to_csv(raw/"power_prices.csv", index=False)
    pd.DataFrame([{"equipment_id":"CR-01","target_utilization_pct":92.0,"max_specific_energy_kwhpt":15.0,"min_throughput_tph":270}]).to_csv(raw/"benchmarks.csv", index=False)

    db_file = tmp_path / "rt_test.db"
    monkeypatch.setenv("RT_DATA_DIR", str(raw))
    monkeypatch.setenv("RT_DB_PATH", str(db_file))
    etl.load_all()
    return db_file

def test_daily_kpis(tmp_path, monkeypatch):
    seed_and_load(tmp_path, monkeypatch)
    kpis = analytics.daily_kpis("2025-08-01")
    expect_cols = {
        "equipment_id","utilization_pct","total_tonnage_t","total_energy_kwh",
        "specific_energy_kwhpt","avg_throughput_tph","meets_min_throughput","meets_max_specific_energy"
    }
    assert expect_cols.issubset(set(kpis.columns))
    assert len(kpis) == 1
    # sanity: utilization should be 100 for fully running hour
    assert 99.0 <= kpis.loc[0,"utilization_pct"] <= 100.0
    # specific energy positive
    assert kpis.loc[0,"specific_energy_kwhpt"] > 0

def test_downtime_summary(tmp_path, monkeypatch):
    seed_and_load(tmp_path, monkeypatch)
    dt = analytics.downtime_summary("2025-08-01")
    assert set(dt.columns) == {"equipment_id","events","total_downtime_min"}
    assert dt.loc[0,"events"] >= 1
    assert dt.loc[0,"total_downtime_min"] >= 30.0
