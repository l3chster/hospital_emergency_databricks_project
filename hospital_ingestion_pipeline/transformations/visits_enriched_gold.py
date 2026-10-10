from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg, coalesce, col, concat_ws, count, date_format, dayofweek, format_string,
    hour, lit, round as sround, sha2, to_date, trunc, when,
)
 
catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
 
SILVER = f"{catalog_name}.hospital_silver"
GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.visits_enriched")
def visits_enriched():
    p = spark.read.table(f"{SILVER}.patient_streaming_silver")
    d = spark.read.table(f"{SILVER}.doctors_silver")
 
    return (
        p.join(d, "doctor_id", "left")
        .select(            
            "visit_id",
            "admission_datetime",
            "age", "gender", "race",
            "admission_flag", "satisfaction_score", "wait_time",
            coalesce(col("department_referral"), lit("No referral")).alias("department_referral"),
            col("coordinator_manager_flag").cast("int").alias("coordinator_involved"),
            "doctor_id",
            "doctor_first_name", 
            "doctor_last_name",
            "department",
            "subspecialty",
            "university", 
            "years_of_experience",
        )
        .withColumn("admission_month", trunc(to_date(col("admission_datetime")), "month"))
        .withColumn("admission_hour", hour("admission_datetime"))
        .withColumn("weekday_num", dayofweek("admission_datetime"))
        .withColumn("weekday", date_format("admission_datetime", "E"))
        .withColumn(
            "age_group",
            when(col("age") < 18, "0-17")
            .when(col("age") < 40, "18-39")
            .when(col("age") < 65, "40-64")
            .otherwise("65+"),
        )
        .withColumn(
            "wait_bucket_in_mins",
            when(col("wait_time") <= 15, "00-15")
            .when(col("wait_time") <= 30, "16-30")
            .when(col("wait_time") <= 45, "31-45")
            .otherwise("46+"),
        )
        .withColumn(
            "experience_bucket_in_years",
            when(col("years_of_experience").isNull(), None)
            .when(col("years_of_experience") <= 10, "0-10")
            .when(col("years_of_experience") <= 15, "11-15")
            .otherwise("16+"),
        )
    )