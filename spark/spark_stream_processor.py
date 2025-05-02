# spark/spark_stream_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr,
    to_json, struct,
    floor, window, avg, count,
    lag, stddev, when, lit, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, ArrayType, MapType, TimestampType
)
from pyspark.sql.window import Window

# -------------------------
# 1°) Schema & Constants
# -------------------------
schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("sensors", ArrayType(LongType())),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", StringType()),
    StructField("fetch_time", LongType()),
    StructField("aircraft", MapType(StringType(), StringType()))
])

# degrees per grid cell for spatial aggregates
GRID_SIZE = 1.0

# -------------------------
# 2°) Spark Setup
# -------------------------
if __name__ == "__main__":
    spark = (
        SparkSession.builder
            .appName("AirspaceCongestionStreaming")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read raw stream from Kafka
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "flight-stream") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON payload into columns
    flights = raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # -------------------------
    # A) Per-flight Derived Metrics
    # -------------------------
    # 1) Risk score (existing)
    scored = flights.withColumn(
        "risk_score",
        expr(
            "(CASE WHEN NOT on_ground AND velocity IS NOT NULL "
            "THEN LEAST(velocity/250.0,1.0) ELSE 0 END) + "
            "(CASE WHEN vertical_rate IS NOT NULL "
            "THEN LEAST(ABS(vertical_rate)/10.0,1.0) ELSE 0 END)"
        )
    )

    # Set up window spec by flight ID for delta computations
    flight_win = Window.partitionBy("icao24").orderBy("fetch_time")

    # 2) Acceleration: (v_t - v_{t-1}) / delta_t
    scored = scored.withColumn(
        "prev_vel", lag("velocity").over(flight_win)
    ).withColumn(
        "acceleration",
        (col("velocity") - col("prev_vel")) / \
        ((col("fetch_time") - lag("fetch_time").over(flight_win)) / 1000)
    )

    # 3) Turn rate: (track_t - track_{t-1}) / delta_t
    scored = scored.withColumn(
        "prev_track", lag("true_track").over(flight_win)
    ).withColumn(
        "turn_rate",
        (col("true_track") - col("prev_track")) / \
        ((col("fetch_time") - lag("fetch_time").over(flight_win)) / 1000)
    )

    # 4) Altitude stability: rolling STD of baro_altitude (window of last 6 observations)
    stability_win = flight_win.rowsBetween(-5, 0)
    scored = scored.withColumn(
        "alt_stability_idx",
        stddev("baro_altitude").over(stability_win)
    )

    # 5) Time-since-last-contact: (fetch_time - last_contact)/1000
    scored = scored.withColumn(
        "dt_last_contact",
        (col("fetch_time") - col("last_contact")) / 1000
    )

    # 6) Altitude delta: geo_altitude - baro_altitude
    scored = scored.withColumn(
        "altitude_delta",
        col("geo_altitude") - col("baro_altitude")
    )

    # -------------------------
    # B) Trajectory-level Features (skeleton placeholders)
    # -------------------------
    # TODO: group by icao24, maintain state or sliding window for:
    #   - rolling risk trend
    #   - path clustering
    #   - ETA prediction
    # Use mapGroupsWithState or FlatMapGroupsWithState for implementation

    # -------------------------
    # C) Spatial Aggregates Enhancements
    # -------------------------
    # Convert fetch_time → Timestamp for windowing
    with_ts = scored.withColumn("event_ts", expr("CAST(fetch_time/1000 AS TIMESTAMP)"))

    # Compute grid bins
    binned = with_ts \
        .withColumn("lat_bin", floor((col("latitude") + 90.0) / GRID_SIZE)) \
        .withColumn("lon_bin", floor((col("longitude") + 180.0) / GRID_SIZE)) \
        .withColumn("alt_band", when(col("baro_altitude") < 10000, lit("low"))
        .when(col("baro_altitude") > 30000, lit("high"))
           .otherwise(lit("mid"))
        )

    # Base aggregation (existing)
    agg = binned \
        .withWatermark("event_ts", "5 seconds") \
        .groupBy(
            window(col("event_ts"), "10 seconds"),
            col("lat_bin"), col("lon_bin")
        ) \
        .agg(
            avg("risk_score").alias("avg_risk"),
            count("icao24").alias("flight_count")
        )

    # TODO: kernel-density heatmaps, proximity alerts

    # -------------------------
    # D) Temporal Analytics (skeleton)
    # -------------------------
    # TODO: compare current window vs. previous windows for trend/peak detection
    # TODO: anomaly scoring (e.g., Poisson model deviation)

    # -------------------------
    # E) Enrichment & Context (skeleton)
    # -------------------------
    # TODO: reverse-geocode bins → regions
    # TODO: airport-centric metrics (inbound/outbound counts)
    # TODO: weather join (SIGMETs, warnings)

    # -------------------------
    # F) Machine-learning Features (skeleton)
    # -------------------------
    # TODO: build per-flight feature vectors for online ML
    # TODO: streaming k-means or anomaly detection per cell

    # -------------------------
    # OUTPUT STREAMS
    # -------------------------
    # 1) Per-flight metrics → Kafka / console
    per_flight_out = scored.select(
        to_json(struct(
            "icao24", "fetch_time", "risk_score",
            "acceleration", "turn_rate",
            "alt_stability_idx", "dt_last_contact",
            "altitude_delta"
        )).alias("value")
    )
    per_flight_out.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start()
    per_flight_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-metrics") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-metrics") \
        .start()

    # 2) Spatial aggregates → Kafka / console
    agg_out = agg.select(
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "lat_bin", "lon_bin", "avg_risk", "flight_count"
        )).alias("value")
    )
    agg_out.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", False) \
        .start()
    agg_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-aggregates") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-aggregates-v2") \
        .start()

    spark.streams.awaitAnyTermination()
