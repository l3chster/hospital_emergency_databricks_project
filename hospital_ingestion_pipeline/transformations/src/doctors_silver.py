from pyspark.sql import DataFrame
from pyspark.sql.functions import col, initcap, trim, upper

DOCTOR_VALIDATIONS = {
    "doctor_id format": "doctor_id RLIKE '^DOC-[0-9]+$'",
    "last name not null": "doctor_last_name IS NOT NULL",
    "experience non-negative": "years_of_experience IS NULL OR years_of_experience >= 0",
}


def transform_doctors_silver(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("doctor_id", upper(trim(col("doctor_id"))))
        .withColumn("doctor_first_name", initcap(trim(col("doctor_first_name"))))
        .withColumn("doctor_last_name", initcap(trim(col("doctor_last_name"))))
        .withColumn("department", initcap(trim(col("department"))))
    )
