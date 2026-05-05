# Fintech Fraud Detection Data Platform

End-to-end fraud analytics pipeline using PaySim, GCP, Snowflake, dbt and GitHub Actions.

## Snowflake
| Concepto  | Qué es         |
| --------- | -------------- |
| Warehouse | Compute engine |
| Database  | Base de datos  |
| Schema    | Carpeta lógica |
| Table     | Tabla          |
| Role      | Permisos       |

export AIRFLOW_HOME=/Users/seleneandrade/Documents/portfolio/paysim-fraud-analytics-platform/airflow
export $(grep -v '^#' ../.env | xargs)

# Paysim Fraud Analytics Platform Improvement Plan

## Objective

Transform the existing portfolio repository into a flagship analytics engineering project suitable for recruiters, hiring managers, and technical interviews.

---

## Phase 1 — Presentation & Positioning (Highest ROI)

### 1. Rewrite README.md

Include:

* Project summary
* Business problem (fraud detection)
* Architecture diagram
* Tech stack
* Data model overview
* How to run locally
* Example outputs / screenshots
* KPIs measured
* Future roadmap

### 2. Add Visual Assets

Create `/docs` folder:

* architecture.png
* dbt_lineage.png
* dashboard.png
* pipeline_flow.png

### 3. Improve Repository Branding

* Professional repo name
* Consistent badges
* Clear folder naming
* Clean commit history moving forward

---

## Phase 2 — Engineering Structure

### Recommended Repository Layout

```text
paysim-fraud-analytics-platform/
├── README.md
├── Dockerfile                        ← Astronomer runtime
├── requirements.txt
│
├── ingestion/                        ← Python ELT pipeline (Spark + GCS + Snowflake)
│   ├── clients/                      ← GCP, Snowflake, Spark session builders
│   ├── config/                       ← Settings & environment loader
│   ├── core/raw/                     ← GCPDataReader: reads from GCS, writes to Snowflake
│   ├── jobs/
│   │   ├── landing/                  ← upload_batch_to_gcs.py
│   │   └── raw/                      ← raw_transactions.py (main Spark job)
│   ├── jars/                         ← GCS Hadoop connector
│   └── data_parameters.py            ← Schema definitions & column mappings
│
├── transform/                        ← dbt transformation layer
│   ├── models/
│   │   ├── staging/                  ← stg_transactions (clean, typed, no business logic)
│   │   ├── dimensions/               ← dim_accounts, dim_dates, dim_transaction_types
│   │   └── facts/                    ← fct_fraud_events, fct_balance_movements, agg_account_balances
│   ├── snapshots/dimensions/         ← SCD Type 2 on dim_accounts & dim_transaction_types
│   ├── tests/generic/                ← assert_no_negative_amounts custom test
│   ├── macros/                       ← generate_schema_name
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── orchestration/                    ← Airflow (Astronomer)
│   ├── dags/                         ← Pipeline DAGs
│   ├── include/
│   └── airflow_settings.yaml
│
├── infra/
│   ├── snowflake/                    ← DDL, roles, warehouse setup
│   └── terraform/                    ← (planned) IaC for GCS buckets & SA
│
├── docs/
│   └── notebooks/                    ← Exploratory analysis
│
├── dataset/                          ← Local PaySim CSV batches
├── secrets/                          ← gitignored credentials
└── .github/workflows/                ← CI/CD (planned)
```

### dbt Model Layout

```text
models/
├── staging/
├── intermediate/
└── marts/
    ├── core/
    └── fraud/
```

Replace `kitchen/` with industry-standard naming unless required internally.

---

## Phase 3 — Data Modeling Improvements

### Core Models

* dim_accounts
  n- dim_transaction_types
* fct_transactions
* fct_fraud_events
* agg_daily_fraud_metrics

### Add Strong Tests

* unique
* not_null
* accepted_values
* relationships
* freshness

### Incremental Optimization

* Use robust watermark logic
* Avoid duplicate merge windows
* Add clustering/partitioning notes

---

## Phase 4 — Orchestration & Automation

### Replace Demo DAGs

Create real DAG:
`daily_fraud_pipeline.py`

Steps:

1. Ingest source data
2. Load raw layer
3. dbt run
4. dbt test
5. Publish metrics/dashboard refresh
6. Notify failures

### CI/CD with GitHub Actions

Pipeline:

```yaml
dbt deps
dbt seed
dbt run
dbt test
sqlfluff lint
```

---

## Phase 5 — Analytics Layer

### Build Dashboard (Power BI / Tableau / Streamlit)

Show:

* Fraud rate over time
* Fraud by transaction type
* Prevented loss amount
* Top risky accounts
* Flag precision / recall (if modeled)

### Add Business Storytelling

Explain:

* What fraud patterns were found
* Operational recommendations
* Risk reduction opportunities

---

## Phase 6 — Senior-Level Enhancements

### Add Observability

* Row count drift checks
* Freshness alerts
* Failed test notifications

### Cost & Performance Section

* Warehouse sizing assumptions
* Incremental strategy reasoning
* Query optimization notes

### Security / Governance

* Environment variables
* Secrets management
* Role-based schemas

---

## Hiring-Optimized Keywords

Use in README and resume:

* Analytics Engineering
* ELT Pipelines
* dbt
* Snowflake / Databricks
* Airflow
* Dimensional Modeling
* Incremental Models
* CI/CD
* Data Quality
* Fraud Analytics
* KPI Design

---

## Suggested 7-Day Execution Plan

### Day 1

Rewrite README + visuals.

### Day 2

Refactor repo structure.

### Day 3

Improve dbt layers + tests.

### Day 4

Build production Airflow DAG.

### Day 5

Create dashboard.

### Day 6

Add CI/CD + linting.

### Day 7

Final polish, screenshots, LinkedIn/GitHub launch.

---

## Final Goal

Position this project as:

> Production-grade fraud analytics platform built with modern data stack tools.

One polished flagship project is more valuable than multiple unfinished repositories.


Tengo suficiente contexto. Aquí está el plan:
 
Plan: Estructura de Pipeline de Producción para Portfolio
Este plan convierte el proyecto actual en un pipeline de datos end-to-end de apariencia profesional. El foco está en: reemplazar el DAG de ejemplo con orquestación real, añadir una capa de staging en dbt, incorporar pruebas de calidad de datos, y documentar la arquitectura para máximo impacto ante reclutadores técnicos.
 
Paso 1 — Reemplazar exampledag.py con DAGs reales en dags/
Crear dos DAGs con Airflow TaskFlow API:
dag_ingest_transactions.py — schedule @hourly o cada 30 min. Tareas en orden:
check_landing_files — sensor que detecta nuevos archivos en GCS landing/incoming/
run_spark_ingestion — llama src/jobs/raw/raw_transactions.py vía BashOperator o PythonOperator
validate_raw_counts — consulta Snowflake para verificar que se cargaron filas (data quality gate)
trigger_dbt_pipeline → dispara el segundo DAG via TriggerDagRunOperator
dag_transform_warehouse.py — triggered por el primero. Tareas:
dbt_run_staging — kitchen_transactions y nuevos modelos staging
dbt_run_dimensions — dim_* models
dbt_run_facts — fct_* models
dbt_test — ejecuta dbt test al final
notify_success — log o alerta de finalización
Usar el Cosmos operator de Astronomer para integrar dbt nativamente en Airflow (muy bien visto en portfolios).
 
Paso 2 — Reorganizar y completar src/
Cambios estructurales:
Renombrar raw_core/gcp_data_reader.py → src/core/gcp_data_reader.py (quitar el prefijo redundante)
Convertir move_batch.py (actualmente en el root) en src/jobs/landing/upload_batch_to_gcs.py — un job reutilizable que sube archivos de dataset/batches_output/ al bucket GCS
Añadir src/jobs/landing/__init__.py
Añadir src/utils/schema_validator.py — valida el schema del DataFrame Spark contra DataParameters antes de escribir a Snowflake
Añadir src/utils/logger.py — wrapper centralizado de loguru con contexto de job
Estructura objetivo de src/:
src/
  clients/         ← sin cambios
  config/          ← sin cambios
  core/            ← renombrado desde raw_core/
  jobs/
    landing/       ← nuevo: upload_batch_to_gcs.py
    raw/           ← existente: raw_transactions.py
  utils/           ← nuevo: schema_validator.py, logger.py
  data_parameters.py
 
Paso 3 — Completar la capa dbt en paysim_dbt/models/
Añadir capa staging/ (actualmente falta — el kitchen/ hace demasiado):
stg_transactions.sql — selección limpia desde source('raw', 'raw_transactions'), cast de tipos, sin lógica de negocio
stg_transactions.yml — con not_null y accepted_values tests en columnas clave (type, isFraud)
Mejorar tests en los modelos existentes (dimensions/, facts/):
Añadir .yml para cada modelo que no lo tenga con: not_null, unique, relationships (FK checks entre facts y dims)
Añadir tests/generic/ con un test custom de dbt: assert_no_negative_amounts.sql
Añadir mart/ o reporting/ layer (opcional pero impactante):
mart_fraud_summary.sql — agregaciones por tipo de transacción y período, listo para consumo por BI
 
Paso 4 — Calidad de datos en dos niveles
Nivel Spark (pre-carga): en GCPDataReader, añadir método validate_schema() que compara el schema inferido del CSV contra el schema esperado definido en DataParameters; si falla, mover archivos a landing/error/ (ya existe el path)
Nivel dbt (post-transformación): dbt test como tarea explícita en el DAG — si falla, el DAG falla y se registra en los logs de Airflow
Nivel Snowflake: añadir snowflake_core/snowflake_config.sql una constraint NOT NULL y comentarios en las tablas clave
 
Paso 5 — README y documentación de arquitectura
El README debe incluir (en este orden para impacto máximo):
Banner/título con badges: Python version, dbt, Airflow, Snowflake
Architecture diagram — imagen de flujo: GCS landing → Spark → Snowflake RAW → dbt staging → dbt dimensions/facts → mart; crear con Excalidraw o draw.io, guardar como docs/architecture.png
Data flow narrative — qué hace cada capa y por qué (demuestra razonamiento de diseño)
Setup instructions — cómo configurar secrets, Astronomer, Snowflake
Sección "Design decisions" — por qué Spark sobre Pandas, por qué kitchen/staging layers, por qué SCD2 en snapshots
 
Paso 6 — CI/CD con GitHub Actions
Para un proyecto solo, lo mínimo efectivo es un workflow en .github/workflows/ci.yml con:
Trigger: push a main y PRs
Jobs:
lint — ruff o flake8 sobre src/ y dags/
test-python — pytest tests/ (unit tests de GCPDataReader mockeando GCS)
dbt-compile — dbt compile --profiles-dir config/ en modo dry-run (sin conexión real, usando dbt con target: ci)
Añadir profiles.yml con un target ci que use variables de entorno GitHub Secrets
 
Consideraciones adicionales
Cosmos vs BashOperator para dbt: Cosmos crea un task por modelo dbt en el grafo de Airflow (más visible y profesional); BashOperator es más simple — ¿cuál prefieres según el tiempo disponible?
Tests de Python: ¿Quieres que los unit tests cubran solo GCPDataReader (lo más crítico) o también los clients de Snowflake/GCS con mocks?
Mart layer: Añadir mart_fraud_summary.sql conecta el pipeline con un caso de uso analítico concreto (detección de fraude), lo que da más narrativa al portfolio — ¿vale la pena incluirlo aunque no haya BI tool conectada?