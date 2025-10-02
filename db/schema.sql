PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS dim_equipment
(
    equipment_id TEXT PRIMARY KEY,
    area TEXT NOT NULL,
    commissioned_data TEXT,
    nameplate_tph INTEGER
);

CREATE TABLE IF NOT EXISTS fact_telemetry
(
    timestamp TEXT NOT NULL,
    equipment_id TEXT NOT NULL,
    area TEXT NOT NULL,
    throughput_tph REAL,
    power_kew REAL,
    temperature_c REAL,
    pressure_kpa REAL,
    status INTEGER,
    load_test TEXt DEFAULT (datetime('now')),
    PRIMARY KEY (timestamp, equipment_id),
    FOREIGN KEY (equipment_id) REFERENCES dim_equipment(equipment_id)
);

CREATE TABLE IF NOT EXISTS fact_downtime 
(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    equipment_id TEXT NOT NULL,
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL,
    duration_mind REAL,
    reason TEXT,
    load_ts TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (equipment_id) REFERENCES dim_equipment(equipment_id)
);


CREATE TABLE IF NOT EXISTS fact_lab_assays 
(
    date TEXT PRIMARY KEY,
    ore_grade_pct REAL,
    moisture_pct REAL,
    bond_work_index_kwhpt REAL
);


CREATE TABLE IF NOT EXISTS fact_power_price
(
    date TEXT PRIMARY KEY,
    usd_per_mwh REAL
);

CREATE TABLE IF NOT EXISTS benchmarks
(
    equipment_id TEXT PRIMARY KEY,
    target_utilization_pct REAL,
    max_specific_energy_kwhpt REAL,
    min_throughput_tph REAL,
    FOREIGN KEY (equipment_id) REFERENCES dim_equipment(equipment_id)
);

CREATE TABLE IF NOT EXISTS data_quality 
(
    dq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT,
    record_key TEXT,
    check_name TEXT,
    check_result TEXT,
    severity TEXT,
    created_ts TEXT DEFAULT (datetime('now'))
);