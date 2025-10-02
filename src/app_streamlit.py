from __future__ import annotations
from pathlib import Path
import os, sqlite3
import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parent.parent

def get_db_path() -> Path:
    # Reads fresh so tests or shells can override with RT_DB_PATH
    return Path(os.getenv("RT_DB_PATH", str(ROOT / "db" / "rt_mining.db")))

@st.cache_data(show_spinner=False)
def q(sql: str, params: dict | tuple = ()):
    with sqlite3.connect(get_db_path()) as con:
        return pd.read_sql_query(sql, con, params=params)

st.set_page_config(page_title="Mining Ops Dashboard", layout="wide")
st.title("⛏️ Mining Operations Dashboard")

# --- Data presence checks
try:
    dates = q("SELECT DISTINCT substr(timestamp,1,10) AS day FROM fact_telemetry ORDER BY day")
except Exception as e:
    st.error("Database not found or schema missing. Run:\n\n`python src/generate_data.py && python src/etl.py`")
    st.stop()

if dates.empty:
    st.warning("No telemetry in DB. Run:\n\n`python src/generate_data.py && python src/etl.py`")
    st.stop()

# --- Sidebar filters
day = st.sidebar.selectbox("Date", dates["day"].tolist(), index=len(dates)-1)

eq = q("SELECT equipment_id, area FROM dim_equipment ORDER BY equipment_id")
all_equip = eq["equipment_id"].tolist()
default_pick = all_equip[:] if all_equip else []
equip = st.sidebar.multiselect("Equipment", all_equip, default=default_pick)

if not equip:
    st.info("Select at least one equipment on the left.")
    st.stop()

# Build parameter placeholders safely
placeholders = ",".join(["?"] * len(equip))

# --- KPI table
kpi = q(
    f"""
    SELECT t.equipment_id,
           AVG(t.throughput_tph) AS avg_tph,
           SUM(t.throughput_tph)*(5.0/60.0) AS total_tons,
           SUM(t.power_kw)*(5.0/60.0)       AS total_kwh,
           SUM(CASE WHEN t.status=1 THEN 1 ELSE 0 END)*100.0/COUNT(*) AS utilization_pct,
           b.min_throughput_tph,
           b.max_specific_energy_kwhpt
    FROM fact_telemetry t
    LEFT JOIN benchmarks b USING (equipment_id)
    WHERE date(substr(t.timestamp,1,10))=date(?)
      AND t.equipment_id IN ({placeholders})
    GROUP BY t.equipment_id, b.min_throughput_tph, b.max_specific_energy_kwhpt
    ORDER BY t.equipment_id
    """,
    (day, *equip),
)

if kpi.empty:
    st.info("No data for the chosen filters.")
    st.stop()

kpi["specific_energy_kwhpt"] = kpi["total_kwh"] / kpi["total_tons"].replace(0, pd.NA)

# --- KPI header metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Equipments", len(kpi))
c2.metric("Avg Utilization %", f"{kpi['utilization_pct'].mean():.2f}")
c3.metric("Total Tons", f"{kpi['total_tons'].sum():.2f}")
c4.metric("Avg kWh/t", f"{kpi['specific_energy_kwhpt'].mean():.2f}")

st.dataframe(kpi, use_container_width=True)
st.divider()

# --- Time series for a selected equipment
pick = st.selectbox("Timeseries Equipment", kpi["equipment_id"].tolist(), index=0)

ts = q(
    """
    SELECT timestamp, equipment_id, throughput_tph, power_kw, status
    FROM fact_telemetry
    WHERE date(substr(timestamp,1,10))=date(:d) AND equipment_id=:eid
    ORDER BY timestamp
    """,
    {"d": day, "eid": pick},
)
ts["timestamp"] = pd.to_datetime(ts["timestamp"])

left, right = st.columns(2)
with left:
    fig1 = px.line(ts, x="timestamp", y="throughput_tph", title=f"{pick} — Throughput (tph)")
    st.plotly_chart(fig1, use_container_width=True)
with right:
    fig2 = px.line(ts, x="timestamp", y="power_kw", title=f"{pick} — Power (kW)")
    st.plotly_chart(fig2, use_container_width=True)

# Rolling kWh/t (1h = 12 intervals of 5 min)
hours = 5.0 / 60.0
ts["tons"] = ts["throughput_tph"] * hours
ts["kwh"] = ts["power_kw"] * hours
ts["roll_kwhpt"] = (
    ts["kwh"].rolling(12, min_periods=3).sum()
    / ts["tons"].rolling(12, min_periods=3).sum().replace(0, pd.NA)
)

fig3 = px.line(ts, x="timestamp", y="roll_kwhpt", title=f"{pick} — Rolling Specific Energy (kWh/t, 1h)")
st.plotly_chart(fig3, use_container_width=True)

# --- Downtime day summary
dt = q(
    """
    SELECT equipment_id,
           COUNT(*) AS events,
           SUM(duration_min) AS total_downtime_min
    FROM fact_downtime
    WHERE date(substr(start_ts,1,10))=date(:d)
    GROUP BY equipment_id
    ORDER BY equipment_id
    """,
    {"d": day},
)

st.subheader("Downtime (day)")
st.dataframe(dt if not dt.empty else pd.DataFrame(columns=["equipment_id", "events", "total_downtime_min"]), use_container_width=True)


