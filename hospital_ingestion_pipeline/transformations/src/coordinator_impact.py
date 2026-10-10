from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, round as sround, when


def transform_coordinator_impact(visits_df: DataFrame) -> DataFrame:
    return (
        visits_df.withColumn(
            "coordinator",
            when(col("coordinator_involved") == True, "with_coordinator").otherwise(
                "without_coordinator"
            ),
        )
        .groupBy("age_group", "coordinator")
        .agg(
            count("*").alias("visits"),
            sround(avg("wait_time"), 1).alias("avg_wait_time"),
            sround(avg("satisfaction_score"), 2).alias("avg_satisfaction"),
            sround(100 * avg(col("admission_flag").cast("int")), 1).alias("admission_rate_pct"),
        )
    )
