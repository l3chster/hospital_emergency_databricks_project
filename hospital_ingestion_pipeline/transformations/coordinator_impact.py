from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg, coalesce, col, concat_ws, count, date_format, dayofweek, format_string,
    hour, lit, round as sround, sha2, to_date, trunc, when,
)
 
catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
 
SILVER = f"{catalog_name}.hospital_silver"
GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.coordinator_impact")
def coordinator_impact():
    v = spark.read.table(f"{GOLD}.visits_enriched")
    return (
        v.withColumn(
            "coordinator",
            when(col("coordinator_involved") == True, "with_coordinator")
            .otherwise("without_coordinator"),
        )
        .groupBy("age_group", "coordinator")
        .agg(
            count("*").alias("visits"),
            sround(avg("wait_time"), 1).alias("avg_wait_time"),
            sround(avg("satisfaction_score"), 2).alias("avg_satisfaction"),            
            sround(100 * avg(col("admission_flag").cast("int")), 1).alias("admission_rate_pct")
        )
    )