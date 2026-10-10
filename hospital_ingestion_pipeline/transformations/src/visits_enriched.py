from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    date_format,
    dayofweek,
    hour,
    lit,
    to_date,
    trunc,
    when,
)


def transform_visits_enriched(patients_df: DataFrame, doctors_df: DataFrame) -> DataFrame:
    return (
        patients_df.join(doctors_df, "doctor_id", "left")
        .select(
            "visit_id",
            "admission_datetime",
            "age",
            "gender",
            "race",
            "admission_flag",
            "satisfaction_score",
            "wait_time",
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
            .when(col("age") >= 65, "65+")
            .otherwise(None),
        )
        .withColumn(
            "wait_bucket_in_mins",
            when(col("wait_time") <= 15, "00-15")
            .when(col("wait_time") <= 30, "16-30")
            .when(col("wait_time") <= 45, "31-45")
            .when(col("wait_time") >= 46, "46+")
            .otherwise(None),
        )
        .withColumn(
            "experience_bucket_in_years",
            when(col("years_of_experience").isNull(), None)
            .when(col("years_of_experience") <= 10, "0-10")
            .when(col("years_of_experience") <= 15, "11-15")
            .otherwise("16+"),
        )
    )
