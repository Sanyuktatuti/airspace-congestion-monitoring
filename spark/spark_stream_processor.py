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
    # 4°) Per-Flight Metrics
    # -------------------------
    # 4.1) Risk Score: combines speed and climb/descent risk into [0,2]
    scored = flights.withColumn(
        "risk_score",
        compute_risk_score(
            col("velocity"),
            col("vertical_rate"),
            col("on_ground")
        )
    )

    # Define time-ordered window per flight (ICAO) for lag & rolling stats.
    flight_win = Window.partitionBy("icao24").orderBy("fetch_time")

    # 4.2) Acceleration a_t = (v_t - v_{t-1}) / Δt  [m/s²]
    scored = scored.withColumn(
        "acceleration",
        compute_acceleration(
            col("velocity"),
            col("fetch_time"),
            flight_win
        )
    )

    # 4.3) Turn Rate ω_t = (θ_t - θ_{t-1}) / Δt  [deg/s]
    scored = scored.withColumn(
        "turn_rate",
        compute_turn_rate(
            col("true_track"),
            col("fetch_time"),
            flight_win
        )
    )

    # 4.4) Altitude Stability Index = rolling stddev(baro_altitude) over last 6 samples [m]
    scored = scored.withColumn(
        "alt_stability_idx",
        compute_alt_stability(
            col("baro_altitude"),
            flight_win,
            lookback=5  # includes current + previous 5 rows
        )
    )

    # 4.5) Time Since Last Contact Δt_contact = (fetch_time - last_contact)/1000  [s]
    scored = scored.withColumn(
        "dt_last_contact",
        compute_dt_last_contact(
            col("fetch_time"),
            col("last_contact")
        )
    )

    # 4.6) Altitude Delta = geo_altitude - baro_altitude  [m]
    scored = scored.withColumn(
        "altitude_delta",
        compute_altitude_delta(
            col("geo_altitude"),
            col("baro_altitude")
        )
    )

    # -------------------------
    # 5°) Trajectory-Level (placeholders)
    # -------------------------
    # TODO: risk trend, path clustering, ETA via mapGroupsWithState

    # -------------------------
    # 6°) Spatial Aggregates
    # -------------------------
    # 6.1) Convert fetch_time (ms) → Timestamp for windowing operations
    with_ts = scored.withColumn(
        "event_ts",
        expr("CAST(fetch_time/1000 AS TIMESTAMP)")  # convert ms→s then to Timestamp
    )

    # 6.2) Compute grid cell indices:
    #      lat_bin = floor((latitude + 90) / GRID_SIZE)
    #      lon_bin = floor((longitude + 180) / GRID_SIZE)
    #      alt_band categorizes altitudes into low/mid/high bands
    binned = with_ts \
        .withColumn(
            "lat_bin",
            floor((col("latitude") + 90.0) / GRID_SIZE)
        ) \
        .withColumn(
            "lon_bin",
            floor((col("longitude") + 180.0) / GRID_SIZE)
        ) \
        .withColumn(
            "alt_band",
            when(col("baro_altitude") < 10000, lit("low"))   # below 10k m
            .when(col("baro_altitude") > 30000, lit("high")) # above 30k m
            .otherwise(lit("mid"))                             # between
        )

    # 6.3) Aggregate per 10s tumbling window + spatial cell
    # Watermark of 5s prevents unbounded state for late data
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

    # -------------------------
    # 7°) Temporal / Context / ML (TODO)
    # -------------------------
    # Trend detection, anomaly scoring, reverse-geocode, weather joins,
    # streaming k-means for cell-level clustering, etc.

    # -------------------------
    # 8°) Output Streams
    # -------------------------
    # 8.1) Per-flight metrics → console & Kafka topic 'flight-metrics'
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

    # 8.2) Spatial aggregates → console & Kafka topic 'flight-aggregates'
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