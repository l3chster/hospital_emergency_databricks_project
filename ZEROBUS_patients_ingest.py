# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# dependencies = [
#   "databricks-zerobus-ingest-sdk",
# ]
# ///
# MAGIC %md
# MAGIC ### Preparing the Environment

# COMMAND ----------

# MAGIC %pip install databricks-zerobus-ingest-sdk
# MAGIC %restart_python

# COMMAND ----------

import json
import logging
from zerobus.sdk.sync import ZerobusSdk
from zerobus.sdk.shared import RecordType, StreamConfigurationOptions, TableProperties, ZerobusException
from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration

# COMMAND ----------

def _param(name):
    try:
        val = dbutils.widgets.get(name)
        if val:
            return val.strip()
    except Exception:
        pass
    raise ValueError(f"Brak wymaganego parametru: '{name}'. Przekaż go przez Job lub utwórz widget.")


DATABRICKS_WORKSPACE_ID = _param("zerobus_workspace_id")
DATABRICKS_WORKSPACE_URL = _param("workspace_url").rstrip("/")
DATABRICKS_REGION = _param("zerobus_region")
APP_NAME = _param("csv_api_app_name")
CATALOG = _param("catalog_name")
SCHEMA = "hospital_bronze"
TABLE = "patients_bronze"

ZEROBUS_INGEST_URL = (
    f"https://{DATABRICKS_WORKSPACE_ID}.zerobus.{DATABRICKS_REGION}.azuredatabricks.net"
)

csv_api_app_url = _param("csv_api_app_url")    

CLIENT_ID = dbutils.secrets.get(scope="scope_blech", key="sp-databricks-adls-appid")
CLIENT_SECRET = dbutils.secrets.get(scope="scope_blech", key="sp-databricks-adls-appkey")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.hospital_silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.hospital_gold")

spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.{TABLE}
        (
        payload VARIANT,
        timestamp_bronze STRING
        )
        """)

# COMMAND ----------

spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO " + "`" + CLIENT_ID + "`;").collect()
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO " + "`" + CLIENT_ID + "`;").collect()
spark.sql(f"GRANT MODIFY, SELECT ON TABLE {CATALOG}.{SCHEMA}.{TABLE} TO " + "`" + CLIENT_ID + "`;").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implementing Sync Client

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

server_endpoint = ZEROBUS_INGEST_URL
workspace_url = DATABRICKS_WORKSPACE_URL

table_name = f"{CATALOG}.{SCHEMA}.{TABLE}"
client_id = CLIENT_ID
client_secret = CLIENT_SECRET

sdk = ZerobusSdk(server_endpoint, unity_catalog_url=workspace_url)
table_properties = TableProperties(table_name)
options = StreamConfigurationOptions(record_type=RecordType.JSON)

# COMMAND ----------

w = WorkspaceClient()

def get_audience_token() -> str:
    app_client_id = w.apps.get(APP_NAME).oauth2_app_client_id
    token_url = f"{DATABRICKS_WORKSPACE_URL}/oidc/v1/token"

    notebook_token = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook().getContext().apiToken().get()
    )

    data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": notebook_token,
            "subject_token_type": "urn:databricks:params:oauth:token-type:personal-access-token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": "all-apis",
            "audience": app_client_id,
    }

    resp = requests.post(url=token_url, data=data, timeout=10)
    resp.raise_for_status()
    return resp.json()["access_token"]


# COMMAND ----------

stream = sdk.create_stream(client_id, client_secret, table_properties, options)

headers = {"Authorization": f"Bearer {get_audience_token()}"}
url = csv_api_app_url
BATCH_SIZE = 50

items = []
failed_total = []

def send_batch(stream, items):
    failed = []
    for record in items:
        try:
            line = json.dumps(record, ensure_ascii=False)
            stream.ingest_record_offset({"payload": line, "timestamp_bronze": datetime.now(timezone.utc).isoformat()})
        except ZerobusException as e:
            print(f"Record sending error: {e}")
            failed.append(record)
    return failed

try:
    with requests.get(url, headers=headers, stream=True, timeout=(5, 60)) as r:
        r.raise_for_status()
        r.encoding = "utf-8"

        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue

            try:
                items.append(json.loads(line))
            except json.JSONDecodeError:
                print(f"Incorrect line has been skipped.")
                continue

            if len(items) >= BATCH_SIZE:
                failed_total += send_batch(stream, items)
                print("Data ingested")
                items = []


finally:
    try:
        if items:
            failed_total += send_batch(stream, items)
            print("Data ingested")
        stream.flush()
        print(f"Finished. Failed records: {len(failed_total)}")
    except Exception as e:
        print(f"Final flush failed, {len(items)} records may be lost: {e}")
    finally:
        stream.close()
