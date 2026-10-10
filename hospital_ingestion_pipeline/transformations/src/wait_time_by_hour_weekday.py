from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, count, round as sround


def transform_wait_time_by_hour_weekday(visits_df: DataFrame) -> DataFrame:
    return visits_df.groupBy("weekday_num", "weekday", "admission_hour").agg(
        count("*").alias("visits"),
        sround(avg("wait_time"), 1).alias("avg_wait_time"),
    )
