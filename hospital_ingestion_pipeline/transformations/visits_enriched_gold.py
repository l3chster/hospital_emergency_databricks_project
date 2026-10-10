from pyspark import pipelines as dp
from transformations.src.visits_enriched import transform_visits_enriched

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")

SILVER = f"{catalog_name}.hospital_silver"
GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.visits_enriched")
def visits_enriched():
    patients_df = spark.read.table(f"{SILVER}.patient_streaming_silver")
    doctors_df = spark.read.table(f"{SILVER}.doctors_silver")
    return transform_visits_enriched(patients_df, doctors_df)
