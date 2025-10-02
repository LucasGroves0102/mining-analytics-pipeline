Mining Analytics Pipeline and Dashboard

Tools: Python, SQL, pandas, matplotlib, pytest, Streamlit, Plotly

Data Model:
- fact_telemtry: throughput, power, status ever 5 min
- fact_downtime: start/end, duration, reason
- fact_lab_assays: ore grade, moisture, bond work index
- factor_power_price: $/MWh
- benchmakrs: utilization %, min throughput, max kWh/t
- data_quality: duplicate/out-of-range audit logs

  Next additions:
    Airflow/cron for scheduled ETL.

    Dockerize with Postgres backend.

    Power BI / Tableau dashboards.

    Anomaly detection with z-scores.
