from pyspark import pipelines as dp
from pyspark.sql.functions import (
    coalesce, col, current_timestamp, expr, initcap, lit, lower,
    substring, try_to_timestamp, upper, when,
)
from pyspark.sql.types import IntegerType

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")

# payload (JSON) -> silver table
PAYLOAD_FIELDS = {
    "patient_id": "patient_id",
    "patient_admission_date": "admission_datetime",
    "patient_first_inital": "first_initial",
    "patient_last_name": "last_name",
    "patient_gender": "gender",
    "patient_age": "age",
    "patient_race": "race",
    "department_referral": "department_referral",
    "patient_admission_flag": "admission_flag",
    "patient_satisfaction_score": "satisfaction_score",
    "patient_waittime": "wait_time",
    "patients_cm": "coordinator_manager_flag",
    "doctor_id": "doctor_id",
    "file": "source_file",
    "row": "source_row",
}

validations = {
    "age check": "age IS NULL OR (age >= 0 AND age < 120)",
    "satisfaction score check": "satisfaction_score IS NULL OR (satisfaction_score BETWEEN 0 AND 10)",
    "wait time check": "wait_time IS NULL OR wait_time >= 0",
    "doctor id exists": "doctor_id IS NOT NULL",
}

DATE_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "dd-MM-yyyy HH:mm",
    "dd-MM-yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm",
    "dd/MM/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "dd-MM-yyyy",
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
]


@dp.view
@dp.expect_or_drop("existing patient_id", "patient_id IS NOT NULL")
@dp.expect_all(validations)
def patient_silver_clean():

    df = spark.readStream.table(f"{catalog_name}.hospital_bronze.patients_bronze")

    extracted = df.select(
        *[
            expr(f"try_variant_get(payload, '$[\"{src}\"]', 'string')").alias(dst)
            for src, dst in PAYLOAD_FIELDS.items()
        ],
        col("timestamp_bronze"),
    )    

    return (
        extracted
        .replace(["N/A", "Unknown", "None", "", " "], None)
        .withColumn(
            "gender",
            when(lower(col("gender")).startswith("m"), "male")
            .when(lower(col("gender")).startswith("f"), "female")
            .otherwise(lit(None)),
        )
        .withColumn(
            "admission_flag",
            when(
                lower(col("admission_flag")).startswith("y")
                | lower(col("admission_flag")).startswith("t")
                | (col("admission_flag") == "1"),
                True,
            )
            .when(
                lower(col("admission_flag")).startswith("n")
                | lower(col("admission_flag")).startswith("f")
                | (col("admission_flag") == "0"),
                False,
            )
            .otherwise(lit(None)),
        )
        .withColumn("first_initial", upper(substring(col("first_initial"), 1, 1)))
        .withColumn("last_name", initcap(col("last_name")))
        .withColumn("race", initcap(col("race")))
        .withColumn("department_referral", initcap(col("department_referral")))
        .withColumn("age", expr("TRY_CAST(age AS DOUBLE)").cast(IntegerType()))
        .withColumn("satisfaction_score", expr("TRY_CAST(satisfaction_score AS DOUBLE)").cast(IntegerType()))
        .withColumn("wait_time", expr("TRY_CAST(wait_time AS DOUBLE)").cast(IntegerType()))
        .withColumn("coordinator_manager_flag", expr("TRY_CAST(coordinator_manager_flag AS BOOLEAN)"))
        .withColumn("source_row", expr("TRY_CAST(source_row AS INT)"))
        .withColumn(
            "admission_datetime",
            coalesce(*[try_to_timestamp(col("admission_datetime"), lit(f)) for f in DATE_FORMATS]),
        )
        .withColumn("bronze_timestamp", try_to_timestamp(col("timestamp_bronze")))
        .drop("timestamp_bronze")
        .withColumn("silver_timestamp", current_timestamp())
    )


dp.create_streaming_table(
    name=f"{catalog_name}.hospital_silver.patient_streaming_silver",
    comment="transformed streaming data",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "false",   # saves data if full refresh
        "delta.autoOptimize.optimizeWrite": "true",
    },
)


dp.create_auto_cdc_flow(
    target=f"{catalog_name}.hospital_silver.patient_streaming_silver",
    source="patient_silver_clean",
    keys=["patient_id"],
    sequence_by=col("bronze_timestamp"),
    stored_as_scd_type="1",
)