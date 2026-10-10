from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, round as sround


def transform_wait_vs_satisfaction(visits_df: DataFrame) -> DataFrame:
    return visits_df.groupBy("wait_bucket_in_mins").agg(
        count("*").alias("visits"),
        sround(avg("satisfaction_score"), 2).alias("avg_satisfaction"),
        count("satisfaction_score").alias("satisfaction_responses"),
        sround(100 * avg(col("admission_flag").cast("int")), 1).alias("admission_rate_pct"),
    )
