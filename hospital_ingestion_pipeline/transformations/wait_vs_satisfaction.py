from pyspark import pipelines as dp
from transformations.src.wait_vs_satisfaction import transform_wait_vs_satisfaction

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")

GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.wait_vs_satisfaction")
def wait_vs_satisfaction():
    return transform_wait_vs_satisfaction(spark.read.table(f"{GOLD}.visits_enriched"))
