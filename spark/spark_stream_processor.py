# spark/spark_stream_processor.py

# -------------------------
# Airspace Congestion Streaming Processor with Enhanced Commentary
# -------------------------
# This module connects Kafka → Spark Structured Streaming,
# applies per-flight risk & derived metrics (via risk_model.py),
# computes spatial aggregates, and writes results back to Kafka and console.

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_json, struct,
    floor, window, avg, count,
    when, lit, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, ArrayType, MapType
)
from pyspark.sql.window import Window

# Import pure-function metric calculators
from spark.risk_model import (
    compute_risk_score,
    compute_acceleration,
    compute_turn_rate,
    compute_alt_stability,
    compute_dt_last_contact,
    compute_altitude_delta
)

# -------------------------
# 1°) Schema & Constants
# -------------------------
# Define the expected JSON schema of incoming messages from Kafka "flight-stream".
# Each field in StructType maps to a JSON key and enforces type safety in Spark.
schema = StructType([
    StructField("icao24", StringType()),       # unique aircraft identifier (hex)
    StructField("callsign", StringType()),     # flight callsign (string, may include spaces)
    StructField("origin_country", StringType()),# country of origin for the flight
    StructField("time_position", LongType()),  # timestamp when position last updated (s since epoch)
    StructField("last_contact", LongType()),   # timestamp of last contact (s since epoch)
    StructField("longitude", DoubleType()),    # longitude in degrees East
    StructField("latitude", DoubleType()),     # latitude in degrees North
    StructField("baro_altitude", DoubleType()),# barometric altitude in meters
    StructField("on_ground", BooleanType()),   # true if aircraft on ground
    StructField("velocity", DoubleType()),     # ground speed in m/s
    StructField("true_track", DoubleType()),   # heading in degrees (0=N, clockwise)
    StructField("vertical_rate", DoubleType()),# vertical speed in m/s (positive climb)
    StructField("sensors", ArrayType(LongType())), # optional sensor IDs list
    StructField("geo_altitude", DoubleType()), # GPS-based altitude in meters
    StructField("squawk", StringType()),       # transponder code
    StructField("spi", BooleanType()),         # special purpose indicator
    StructField("position_source", StringType()), # source of position data
    StructField("fetch_time", LongType()),     # ingestion timestamp (ms since epoch)
    StructField("aircraft", MapType(StringType(), StringType())) # static metadata map
])

# Grid size in degrees for spatial aggregation.
# A 1.0° cell in lat/lon covers roughly ~111km at equator per degree.
GRID_SIZE = 1.0

if __name__ == "__main__":
    # -------------------------
    # 2°) SparkSession Setup
    # -------------------------
    # Build a SparkSession with Kafka support via spark-sql-kafka package.
    spark = (
        SparkSession.builder
            .appName("AirspaceCongestionStreaming")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
            )
            # Force driver to bind to localhost for Kafka metadata
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")  # suppress verbose INFO logs

    # -------------------------
    # 3°) Read Raw Stream
    # -------------------------
    # Subscribe to Kafka topic 'flight-stream'.  Each record's value is a JSON string.
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "flight-stream") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the JSON payload into typed columns according to `schema`.
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
            window(col("event_ts"), "10 seconds"),  # tumbling window
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


    # 8.3) Block until termination of streaming queries
    spark.streams.awaitAnyTermination()