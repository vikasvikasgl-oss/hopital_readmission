# Hospital Readmission Analytics and Prediction Using Public Data

**Course:** Data Engineering and MLOps  
**Project:** Project 5 - Hospital Readmission Analytics and Prediction  
**Primary Dataset:** UCI Diabetes 130-US Hospitals (Anonymized Clinical Records)  
**Orchestration:** Apache Airflow  
**Storage & Warehousing:** PostgreSQL & Dimensional Star Schema  
**Visualization:** Streamlit Interactive Analytical Dashboard  

---

## 1. Project Overview & Multi-Source Segregation

In compliance with the data engineering guidelines:
- **At least 3 genuinely different data sources** are segregated in landing storage before transformation.
- Ingestion metadata (`source_name`, `ingestion_timestamp`, `batch_id`) is tracked on every record.
- Patient-safe surrogate keys are generated via SHA-256 hashing to adhere to HIPAA/privacy standards.

| Source Name | Ingestion Type | Format | Features Provided | Destination Raw Table |
|---|---|---|---|---|
| **Source 1: UCI 130-US Encounters** | Batch Ingestion | CSV | Encounter IDs, admission/discharge IDs, diagnoses, lab counts, medications, readmission status | `raw_clinical_encounters` |
| **Source 2: Patient Registry API** | REST API Connector | JSON | Demographics, zip prefix, socioeconomic risk index, insurance tier, language | `raw_patient_api` |
| **Source 3: Institutional ICD-9 Mapping** | DB / Reference | CSV / SQL | ICD-9 diagnostic codes, descriptions, clinical category, chronic disease flag | `raw_icd9_lookup` |

---

## 2. Architecture & Data Flow

```
                      [ MULTI-SOURCE INGESTION ]
   [UCI Encounters CSV]     [Patient Registry API JSON]     [ICD-9 Reference DB]
            │                           │                           │
            ▼                           ▼                           ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                      SEGREGATED RAW LANDING ZONE                            │
 │        raw_clinical_encounters | raw_patient_api | raw_icd9_lookup          │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │
                                        ▼ (Apache Airflow ETL DAG)
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                    VALIDATION & DATA QUALITY AUDIT                          │
 │         - Negative Length of Stay (LOS) Correction                          │
 │         - Expired Patient Readmission Clinical Conflict Overrides           │
 │         - Missing API Joins Handled & Logged -> data_quality_error_log      │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                 CORE DATA WAREHOUSE (STAR SCHEMA)                           │
 │                                                                             │
 │  [Dim_Patient] ──────────┐                                                  │
 │  [Dim_Diagnosis] ────────┼───────► [Fact_Hospital_Encounters]               │
 │  [Dim_Admission_Source] ─┤                                                  │
 │  [Dim_Discharge_Disp] ───┘                                                  │
 └──────────────────────────────────────┬──────────────────────────────────────┘
                                        │
                                        ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                     ANALYTICAL DATA MARTS & DASHBOARD                       │
 │      mart_readmission_summary | mart_high_risk_cohorts                      │
 │                                      │                                      │
 │                                      ▼                                      │
 │                       STREAMLIT ANALYTICS DASHBOARD                         │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Directory Structure

```text
├── dags/
│   └── hospital_readmission_etl_dag.py     # Apache Airflow DAG with task dependencies
├── database/
│   └── init/
│       ├── 01_raw_schema.sql               # Segregated raw schemas
│       ├── 02_staging_schema.sql           # Staging layer with surrogate keys
│       ├── 03_core_and_star_schema.sql     # Fact and Dimensions
│       └── 04_data_mart_and_quality.sql    # Analytical marts & quality error log
├── data/
│   ├── raw/                                # Segregated landing files
│   └── hospital_management.db             # Populated SQLite database
├── dashboard/
│   └── app.py                              # Streamlit 5-view analytics dashboard
├── scripts/
│   ├── etl_pipeline.py                     # Python ETL engine with data quality checks
│   ├── test_airflow_dag.py                 # Standalone Airflow DAG simulator
│   └── query_database.py                   # Verification & table query inspection tool
├── docker-compose.yml                      # Stack config for Airflow & PostgreSQL
└── README.md
```

---

## 4. How to Run and Test

### Option A: Direct Local Execution (Zero Docker Dependencies)

1. **Run the complete ETL Pipeline:**
   ```bash
   python scripts/etl_pipeline.py
   ```
2. **Simulate the Apache Airflow DAG:**
   ```bash
   python scripts/test_airflow_dag.py
   ```
3. **Inspect the Populated Star Schema Database:**
   ```bash
   python scripts/query_database.py
   ```
4. **Launch the Streamlit Analytics Dashboard:**
   ```bash
   streamlit run dashboard/app.py
   ```

---

### Option B: Apache Airflow & PostgreSQL Stack via Docker

1. **Start the PostgreSQL & Airflow stack:**
   ```bash
   # Copy .env.example to .env, then set unique passwords and generate AIRFLOW_FERNET_KEY.
   docker compose up -d
   ```
2. **Access Airflow UI:**
   - URL: `http://localhost:8080`
   - Sign in with `AIRFLOW_ADMIN_USERNAME` and `AIRFLOW_ADMIN_PASSWORD` from `.env`.
   - Trigger DAG: `hospital_readmission_etl_pipeline`
