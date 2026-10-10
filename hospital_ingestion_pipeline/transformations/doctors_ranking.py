from pyspark import pipelines as dp
from transformations.src.doctor_performance import transform_doctor_performance

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")

GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.doctor_performance")
def doctor_performance():
    return transform_doctor_performance(spark.read.table(f"{GOLD}.visits_enriched"))
