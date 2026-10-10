from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, round as sround


def transform_department_monthly_stats(visits_df: DataFrame) -> DataFrame:
    return visits_df.groupBy("department_referral", "admission_month").agg(
        count("*").alias("visits"),
        sround(avg("wait_time"), 1).alias("avg_wait_time"),
        sround(avg("satisfaction_score"), 2).alias("avg_satisfaction"),
        count("satisfaction_score").alias("satisfaction_responses"),
        sround(100 * avg(col("admission_flag").cast("int")), 1).alias("admission_rate_pct"),
        sround(100 * avg("coordinator_involved"), 1).alias("coordinator_usage_pct"),
        sround(avg("age"), 1).alias("avg_age"),
    )
