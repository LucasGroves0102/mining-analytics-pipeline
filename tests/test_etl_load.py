import os
import sqlite3
from pathlib import Path
import pandas as pd
from src import etl

def write_csvs(base: Path):
    raw = base / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    pd.DataFrame([
        {"equipment_id":"CR-01","area":"Crusher","commissioned_date":"2020-01-01","nameplate_tph":450},
    ]).to_csv(raw/"equipment_metadata.csv", index=False)

    # two 5-min rows, with one deliberate duplicate to trigger DQ
    tele = pd.DataFrame([
        {"timestamp":"2025-08-01T00:00:00","equipment_id":"CR-01","area":"Crusher","throughput_tph":380,"power_kw":1600,"temperature_c":40,"pressure_kpa":200,"status":1},
        {"timestamp":"2025-08-01T00:05:00","equipment_id":"CR-01","area":"Crusher","throughput_tph":-1,"power_kw":1600,"temperature_c":40,"pressure_kpa":200,"status":1}, # out-of-range
        {"timestamp":"2025-08-01T00:00:00","equipment_id":"CR-01","area":"Crusher","throughput_tph":380,"power_kw":1600,"temperature_c":40,"pressure_kpa":200,"status":1}, # duplicate
    ])
    tele.to_csv(raw/"telemetry.csv", index=False)

    pd.DataFrame([{
        "equipment_id":"CR-01","start_ts":"2025-08-01T06:00:00","end_ts":"2025-08-01T06:30:00","duration_min":30.0,"reason":"Maintenance"
    }]).to_csv(raw/"downtime_events.csv", index=False)

    pd.DataFrame([{
        "date":"2025-08-01","ore_grade_pct":0.5,"moisture_pct":0.08,"bond_work_index_kwhpt":14.5
    }]).to_csv(raw/"lab_assays.csv", index=False)

    pd.DataFrame([{
        "date":"2025-08-01","usd_per_mwh":55.0
    }]).to_csv(raw/"power_prices.csv", index=False)

    pd.DataFrame([{
        "equipment_id":"CR-01","target_utilization_pct":92.0,"max_specific_energy_kwhpt":15.0,"min_throughput_tph":270
    }]).to_csv(raw/"benchmarks.csv", index=False)

def test_etl_end_to_end(tmp_path, monkeypatch):
    # temp raw dir + temp DB file
    data_dir = tmp_path / "data"
    db_file = tmp_path / "rt_test.db"

    write_csvs(data_dir)

    # point ETL to our temp locations
    monkeypatch.setenv("RT_DATA_DIR", str(data_dir / "raw"))
    monkeypatch.setenv("RT_DB_PATH", str(db_file))

    # run load
    etl.load_all()

    assert db_file.exists()

    with sqlite3.connect(db_file) as con:
        # tables exist & have rows
        cnt = lambda t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        assert cnt("dim_equipment") == 1
        assert cnt("fact_telemetry") == 3
        assert cnt("fact_downtime") == 1
        assert cnt("fact_lab_assays") == 1
        assert cnt("fact_power_price") == 1
        assert cnt("benchmarks") == 1

        # DQ should have 2 entries: one duplicate, one out_of_range
        dq_rows = cnt("data_quality")
        assert dq_rows >= 2
