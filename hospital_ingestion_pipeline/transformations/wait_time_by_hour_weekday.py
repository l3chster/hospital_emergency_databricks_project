from pyspark import pipelines as dp
from pyspark.sql.functions import (
    avg, coalesce, col, concat_ws, count, date_format, dayofweek, format_string,
    hour, lit, round as sround, sha2, to_date, trunc, when,
)
 
catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
 
SILVER = f"{catalog_name}.hospital_silver"
GOLD = f"{catalog_name}.hospital_gold"


@dp.materialized_view(name=f"{GOLD}.wait_time_by_hour_weekday")
def wait_time_by_hour_weekday():
    v = spark.read.table(f"{GOLD}.visits_enriched")
    return (
        v.groupBy("weekday_num", "weekday", "admission_hour")
        .agg(
            count("*").alias("visits"),
            sround(avg("wait_time"), 1).alias("avg_wait_time"),
        )
    )