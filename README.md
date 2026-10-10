# hospital_emergency_databricks

Lakeflow pipeline for hospital ER visits, ZeroBus ingest of patient rows from a Databricks App, and Lakehouse Federation to Postgres doctors.

Push to `main` runs **tests only**. Resource deploy is a **manual** GitHub Action and does **not** start the Databricks job.

## Layout

| Path | Role |
| --- | --- |
| `databricks.yml` | Asset Bundle: targets `dev` / `prod` |
| `hospital_ingestion_pipeline/` | Lakeflow transformations (bronze → silver → gold) |
| `ZEROBUS_patients_ingest.py` | ZeroBus load into `{catalog}.hospital_bronze.patients_bronze` |
| `notebooks/ensure_foreign_catalog.py` | Postgres connection + foreign catalog `hospital_db` |
| `csv-api-app_hospital/` | Databricks App that streams CSV rows |
| `.github/workflows/pipeline_functions_test.yml` | pytest on push to `main` |
| `.github/workflows/deploy.yml` | Manual `workflow_dispatch` deploy |

Unity Catalog `{catalog}.hospital_bronze|silver|gold` must already exist (or the deploying SP must be allowed to create schemas). Catalog itself is not created by the bundle.

## CI vs CD

- **CI:** `push` to `main` → GitHub Actions *Pipeline tests* (pytest). No Databricks deploy.
- **CD:** GitHub Actions *Deploy Databricks bundle*, only via **Run workflow** (`workflow_dispatch`). Input `target`: `dev` or `prod`.
- Deploy runs `databricks bundle validate` then `databricks bundle deploy`. It does **not** run `databricks bundle run`.

After deploy, start data refresh yourself in Databricks: job `hospital_ingest_and_refresh`.

That job:

1. `ensure_foreign_catalog` — `CREATE CONNECTION` / `CREATE FOREIGN CATALOG` for doctors
2. `zerobus_patients` — ingest from the CSV app via ZeroBus (parallel with 1)
3. `run_pipeline` — Lakeflow pipeline (depends on both)

## GitHub Environments

Create Environments named exactly `dev` and `prod`.

On **prod**, enable **Required reviewers** so a deploy waits for approval.

### Secrets (each environment)

| Secret | Purpose |
| --- | --- |
| `DATABRICKS_CLIENT_ID` | Service principal application ID |
| `DATABRICKS_CLIENT_SECRET` | Service principal secret |

### Variables (each environment)

| Variable | `dev` | `prod` |
| --- | --- | --- |
| `DATABRICKS_HOST` | `https://adb-7405618361895520.0.azuredatabricks.net` | prod workspace URL (required) |
| `CATALOG_NAME` | optional (`dbr_dev` is in the bundle) | required, e.g. `dbr_prod` |
| `ZEROBUS_WORKSPACE_ID` | optional | required numeric workspace id |
| `ZEROBUS_REGION` | optional | required, e.g. `eastus` |
| `CSV_API_APP_NAME` | optional (`csv-api-app`) | required, e.g. `csv-api-app-prod` |
| `CSV_API_APP_URL` | optional | optional; if empty, derived as `https://{app}-{workspace_id}.0.azure.databricksapps.com/api/stream` |

The SP needs rights to deploy jobs, pipelines, apps, schemas, and volumes in the target workspace, plus access to secret scope `scope_blech`.

## How to deploy

UI: Actions → **Deploy Databricks bundle** → **Run workflow** → choose `dev` or `prod`.

CLI:

```bash
gh workflow run deploy.yml -f target=dev
gh workflow run deploy.yml -f target=prod
```

Prod waits on Environment approval before the job runs.

## Manual job run (after deploy)

Workspace UI: Workflows → `hospital_ingest_and_refresh` → Run now.

Or locally, after CLI auth (this is **not** part of CD):

```bash
databricks bundle run hospital_ingest_and_refresh -t dev
databricks bundle run hospital_ingest_and_refresh -t prod \
  --var "workspace_url=$DATABRICKS_HOST" \
  --var "catalog_name=$CATALOG_NAME" \
  --var "zerobus_workspace_id=$ZEROBUS_WORKSPACE_ID" \
  --var "zerobus_region=$ZEROBUS_REGION" \
  --var "csv_api_app_name=$CSV_API_APP_NAME" \
  --var "csv_api_app_url=$CSV_API_APP_URL"
```

## Local tests

```bash
pip install "pyspark>=4.0,<4.1" pytest
python -m pytest -v
```
