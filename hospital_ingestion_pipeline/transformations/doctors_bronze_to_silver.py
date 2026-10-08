from pyspark import pipelines as dp
from pyspark.sql.functions import col, initcap, trim, upper

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")

doctor_validations = {
    "doctor_id format": "doctor_id RLIKE '^DOC-[0-9]+$'",
    "last name not null": "doctor_last_name IS NOT NULL",
    "experience non-negative": "years_of_experience IS NULL OR years_of_experience >= 0",
}

@dp.materialized_view(name=f"{catalog_name}.hospital_silver.doctors_silver")
@dp.expect_or_drop("existing doctor_id", "doctor_id IS NOT NULL")
@dp.expect_all(doctor_validations)
def doctors_silver():
    return (
        spark.read.table("hospital_db.public.doctors_info_bronze")
        .withColumn("doctor_id", upper(trim(col("doctor_id"))))
        .withColumn("doctor_first_name", initcap(trim(col("doctor_first_name"))))
        .withColumn("doctor_last_name", initcap(trim(col("doctor_last_name"))))
        .withColumn("department", initcap(trim(col("department"))))
    )