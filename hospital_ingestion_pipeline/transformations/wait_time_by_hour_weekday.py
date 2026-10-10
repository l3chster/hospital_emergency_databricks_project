from pyspark import pipelines as dp
from transformations.src.wait_time_by_hour_weekday import transform_wait_time_by_hour_weekday

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")

GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.wait_time_by_hour_weekday")
def wait_time_by_hour_weekday():
    return transform_wait_time_by_hour_weekday(spark.read.table(f"{GOLD}.visits_enriched"))
