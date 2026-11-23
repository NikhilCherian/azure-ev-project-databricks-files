# VoltGrid EV Intelligence — Databricks Layer

> This repository is a **sub-component of the main Azure project**: [`azure-ev-intelligence-project`](https://github.com/NikhilCherian/azure-ev-intelligence-project). It holds the Databricks notebooks, Delta Live Tables pipelines, and Unity Catalog grant scripts that the ADF pipelines in the main repo orchestrate. Start with the main repo's README for the full architecture, source list, and diagrams — this one covers just what lives in here.

## What's in here

```
.
├── control-table-setup.sql.dbquery.ipynb   # Registers sources in ev_catalog.ctl.pipeline_control
├── grants_adf_sp.sql                       # Unity Catalog grants for the ADF service principal
├── grants_gold_sp.sql                      # Unity Catalog grants for Gold-layer access
├── grants_silver_sp.ipynb                  # Unity Catalog grants for Silver-layer access
│
├── silver/                                 # Silver-layer transformation notebooks (one per source)
│   ├── nb_silver_customers.ipynb, nb_silver_chargers.ipynb, nb_silver_stations.ipynb, …
│   └── nb_silver_pipeline_audit.ipynb      # Pipeline run observability
│
├── gold/                                   # Gold-layer dimension, fact & mart notebooks
│   ├── nb_gold_dim_charger.ipynb, nb_gold_dim_customer.ipynb, nb_gold_dim_station.ipynb, nb_gold_dim_tariff.ipynb
│   ├── nb_gold_fact_charging_session.ipynb, nb_gold_fact_payments.ipynb
│   └── nb_gold_mart_revenue_by_geo_month.ipynb
│
├── streaming/
│   ├── eventhub/                           # Vehicle telemetry streaming via Azure Event Hub
│   └── kafka/                              # Equivalent streaming pipeline via Kafka
│
├── delta-live-tables/                      # DLT version of the EV charging pipeline
├── dlt-pipeline-ev-vechicle-data-from-event-hub/   # DLT: Event Hub → Bronze → Silver → Gold
├── dlt-pipeline-ev-vehicle-data-from-bronze/       # DLT: Bronze → Silver → Gold (batch source)
│
├── nb_list_active_sources.ipynb            # Reads active rows from the control table
├── nb_read_control_table.ipynb             # Control-table inspection utility
└── nb_update_watermark.ipynb               # Updates last_watermark after a successful load
```

## Where this fits in the pipeline

The ADF pipelines in the main repo call into this repo's notebooks at three points:

1. **`act_list_sources`** (master pipeline) → `nb_list_active_sources.ipynb`, reading `ev_catalog.ctl.pipeline_control`
2. **`pl_silver_dynamic_foreach`** → loops through `silver/*.ipynb`
3. **`pl_gold_dynamic_foreach`** → loops through `gold/*.ipynb`

Streaming notebooks under `streaming/` and the `delta-live-tables`/`dlt-pipeline-*` folders run independently of the batch ADF orchestration, driven by Event Hub/Kafka directly.

See the [main repo](https://github.com/NikhilCherian/azure-ev-intelligence-project) for the full architecture diagram, data source list, and Gold-layer star schema.

