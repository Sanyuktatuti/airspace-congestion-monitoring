# spark/batch_test_metrics.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window

# reuse your streaming schema & pure functions
from spark.spark_stream_processor import schema
from spark.risk_model import (
    compute_risk_score,
    compute_acceleration,
    compute_turn_rate,
    compute_alt_stability,
    compute_dt_last_contact,
    compute_altitude_delta
)

if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("BatchTestMetrics")
            # avoid bind errors on macOS
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host",        "127.0.0.1")
            .getOrCreate()
    )

    # 1) Load the wrapper JSONL so we get two columns: "key" (string) and "value" (struct)
    wrapped = spark.read.json("data/test_samples.jsonl")

    # 2) Expand the nested struct
    flights = wrapped.select("value.*")\
                     .selectExpr(*[f"`{f.name}` as `{f.name}`" for f in schema.fields])

    # 3) Apply exactly the same six metrics you use in streaming
    flight_win = Window.partitionBy("icao24").orderBy("fetch_time")

    scored = flights \
      .withColumn("risk_score",
        compute_risk_score(col("velocity"), col("vertical_rate"), col("on_ground"))
      ) \
      .withColumn("acceleration",
        compute_acceleration(col("velocity"), col("fetch_time"), flight_win)
      ) \
      .withColumn("turn_rate",
        compute_turn_rate(col("true_track"), col("fetch_time"), flight_win)
      ) \
      .withColumn("alt_stability_idx",
        compute_alt_stability(col("baro_altitude"), flight_win, lookback=5)
      ) \
      .withColumn("dt_last_contact",
        compute_dt_last_contact(col("fetch_time"), col("last_contact"))
      ) \
      .withColumn("altitude_delta",
        compute_altitude_delta(col("geo_altitude"), col("baro_altitude"))
      )

    # 4) Show results
    scored.select(
        "icao24", "fetch_time", "risk_score",
        "acceleration", "turn_rate",
        "alt_stability_idx", "dt_last_contact",
        "altitude_delta"
    ).show(truncate=False)

    spark.stop()