from pyspark import pipelines as dp
from transformations.src.coordinator_impact import transform_coordinator_impact

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")

GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.coordinator_impact")
def coordinator_impact():
    return transform_coordinator_impact(spark.read.table(f"{GOLD}.visits_enriched"))
