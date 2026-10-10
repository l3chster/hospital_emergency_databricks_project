from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, round as sround


def transform_doctor_performance(visits_df: DataFrame) -> DataFrame:
    return visits_df.groupBy(
        "doctor_id",
        "doctor_first_name",
        "doctor_last_name",
        "department",
        "subspecialty",
        "university",
        "years_of_experience",
    ).agg(
        count("*").alias("visits"),
        sround(avg("wait_time"), 1).alias("avg_wait_time"),
        sround(avg("satisfaction_score"), 2).alias("avg_satisfaction"),
        count("satisfaction_score").alias("satisfaction_responses"),
        sround(100 * avg(col("admission_flag").cast("int")), 1).alias("admission_rate_pct"),
        sround(100 * avg("coordinator_involved"), 1).alias("coordinator_usage_pct"),
    )
