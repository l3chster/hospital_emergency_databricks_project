from pyspark import pipelines as dp
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg, coalesce, col, concat_ws, count, date_format, dayofweek, format_string,
    hour, lit, round as sround, sha2, to_date, trunc, when,
)
 
catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
 
SILVER = f"{catalog_name}.hospital_silver"
GOLD = f"{catalog_name}.hospital_gold"

@dp.materialized_view(name=f"{GOLD}.department_monthly_stats")
def department_monthly_stats():
    v = spark.read.table(f"{GOLD}.visits_enriched")
    return (
        v.groupBy("department_referral", "admission_month")
        .agg(
            count("*").alias("visits"),
            sround(avg("wait_time"), 1).alias("avg_wait_time"),
            sround(avg("satisfaction_score"), 2).alias("avg_satisfaction"),
            count("satisfaction_score").alias("satisfaction_responses"),
            sround(100 * avg(col("admission_flag").cast("int")), 1).alias("admission_rate_pct"),   # % of taken people
            sround(100 * avg("coordinator_involved"), 1).alias("coordinator_usage_pct"),
            sround(avg("age"), 1).alias("avg_age"),
        )
    )