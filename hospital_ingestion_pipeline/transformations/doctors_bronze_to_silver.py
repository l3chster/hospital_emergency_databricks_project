from pyspark import pipelines as dp
from transformations.src.doctors_silver import DOCTOR_VALIDATIONS, transform_doctors_silver

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")


@dp.materialized_view(name=f"{catalog_name}.hospital_silver.doctors_silver")
@dp.expect_or_drop("existing doctor_id", "doctor_id IS NOT NULL")
@dp.expect_all(DOCTOR_VALIDATIONS)
def doctors_silver():
    return transform_doctors_silver(spark.read.table("hospital_db.public.doctors_info_bronze"))
