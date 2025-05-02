# spark/spark_stream_processor.py
# -------------------------
# Airspace Congestion Streaming Processor with Detailed Comments
# -------------------------

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
# Define the expected schema of incoming JSON messages from Kafka.
# Each StructField maps a JSON property to a Spark SQL column and type.
schema = StructType([
    StructField("icao24", StringType()),        # unique aircraft ICAO hex code
    StructField("callsign", StringType()),      # flight callsign (may include padding)
    StructField("origin_country", StringType()),# country of origin
    StructField("time_position", LongType()),   # UNIX timestamp of position report
    StructField("last_contact", LongType()),    # UNIX timestamp of last contact
    StructField("longitude", DoubleType()),     # degrees East
    StructField("latitude", DoubleType()),      # degrees North
    StructField("baro_altitude", DoubleType()), # barometric altitude (m)
    StructField("on_ground", BooleanType()),    # true if on ground
    StructField("velocity", DoubleType()),      # ground speed (m/s)
    StructField("true_track", DoubleType()),    # heading in degrees
    StructField("vertical_rate", DoubleType()), # vertical speed (m/s)
    StructField("sensors", ArrayType(LongType())), # optional sensor IDs
    StructField("geo_altitude", DoubleType()),  # GPS-derived altitude (m)
    StructField("squawk", StringType()),        # transponder code
    StructField("spi", BooleanType()),          # special purpose indicator
    StructField("position_source", StringType()),# source of position data
    StructField("fetch_time", LongType()),      # timestamp when record was fetched
    StructField("aircraft", MapType(StringType(), StringType())) # static metadata
])

# For spatial binning: size of grid cell in degrees
GRID_SIZE = 1.0

# -------------------------
# 2°) Spark Setup
# -------------------------
if __name__ == "__main__":
    # Initialize SparkSession with Kafka connector package
    spark = (
        SparkSession.builder
            .appName("AirspaceCongestionStreaming")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
            # Ensure driver binds to localhost (for Kafka metadata)
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")  # reduce log verbosity

    # ── Read raw stream from Kafka ──────────────────────────────────────────
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "flight-stream") \
        .option("startingOffsets", "earliest") \
        .load()

    # ── Parse JSON payload into DataFrame columns ─────────────────────────
    flights = raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # -------------------------
    # A) Per-flight Derived Metrics
    # -------------------------
    # 1) Risk score (two-part: speed + climb/descent)
    #    risk_speed = min(velocity/250, 1.0)
    #    risk_climb = min(|vertical_rate|/10, 1.0)
    #    total risk_score = risk_speed + risk_climb
    scored = flights.withColumn(
        "risk_score",
        expr(
            "(CASE WHEN NOT on_ground AND velocity IS NOT NULL "
            "THEN LEAST(velocity/250.0,1.0) ELSE 0 END) + "
            "(CASE WHEN vertical_rate IS NOT NULL "
            "THEN LEAST(ABS(vertical_rate)/10.0,1.0) ELSE 0 END)"
        )
    )

    # Define a sliding window over records for each flight (ICAO24), ordered by fetch_time
    flight_win = Window.partitionBy("icao24").orderBy("fetch_time")

    # 2) Acceleration: a_t = (v_t - v_{t-1}) / Δt
    #    Δt computed in seconds from fetch_time difference (ms -> s conversion)
    scored = scored.withColumn(
        "prev_vel", lag("velocity").over(flight_win)
    ).withColumn(
        "acceleration",
        (col("velocity") - col("prev_vel")) /
        ((col("fetch_time") - lag("fetch_time").over(flight_win)) / 1000)
    )

    # 3) Turn rate: ω_t = (track_t - track_{t-1}) / Δt (deg/s)
    scored = scored.withColumn(
        "prev_track", lag("true_track").over(flight_win)
    ).withColumn(
        "turn_rate",
        (col("true_track") - col("prev_track")) /
        ((col("fetch_time") - lag("fetch_time").over(flight_win)) / 1000)
    )

    # 4) Altitude stability index: rolling stddev of baro_altitude over last 6 records
    #    Use rowsBetween(-5,0) to include current + previous 5 rows
    stability_win = flight_win.rowsBetween(-5, 0)
    scored = scored.withColumn(
        "alt_stability_idx",
        stddev("baro_altitude").over(stability_win)
    )

    # 5) Time since last contact: Δt_contact = (fetch_time - last_contact) / 1000 (s)
    scored = scored.withColumn(
        "dt_last_contact",
        (col("fetch_time") - col("last_contact")) / 1000
    )

    # 6) Altitude delta: difference between GPS (geo_altitude) and barometer
    scored = scored.withColumn(
        "altitude_delta",
        col("geo_altitude") - col("baro_altitude")
    )

    # -------------------------
    # B) Trajectory-level Features (placeholders)
    # -------------------------
    # TODO: for each flight (group by icao24), maintain streaming state to compute:
    #   • rolling risk trend (e.g., last N-sec average or max)
    #   • map-matching / clustering of path segments
    #   • ETA prediction by fitting simple linear model on heading & speed
    # Spark functions: mapGroupsWithState / flatMapGroupsWithState

    # -------------------------
    # C) Spatial Aggregates Enhancements
    # -------------------------
    # Convert fetch_time (ms) → Timestamp for time-based window aggregations
    with_ts = scored.withColumn(
        "event_ts",    # cast to Spark TimestampType
        expr("CAST(fetch_time/1000 AS TIMESTAMP)")
    )

    # Assign each flight to a lat/lon grid cell + altitude band
    binned = with_ts \
        .withColumn(
            "lat_bin",
            floor((col("latitude") + 90.0) / GRID_SIZE)  # index 0 at -90° latitude
        ) \
        .withColumn(
            "lon_bin",
            floor((col("longitude") + 180.0) / GRID_SIZE) # index 0 at -180° longitude
        ) \
        .withColumn(
            "alt_band",
            when(col("baro_altitude") < 10000, lit("low"))  # <10k ft
            .when(col("baro_altitude") > 30000, lit("high")) # >30k ft
            .otherwise(lit("mid"))                             # between 10k–30k ft
        )

    # Base 10s tumbling window aggregation per cell
    agg = binned \
        .withWatermark("event_ts", "5 seconds") \
        .groupBy(
            window(col("event_ts"), "10 seconds"),  # 10s interval
            col("lat_bin"), col("lon_bin")           # spatial bins
        ) \
        .agg(
            avg("risk_score").alias("avg_risk"),    # mean risk in cell
            count("icao24").alias("flight_count")   # number of flights
        )

    # TODO: implement kernel-density heatmaps per grid cell using exponential smoothing
    # TODO: count proximity alerts where horizontal <5 NM & vertical <1000 ft

    # -------------------------
    # D) Temporal Analytics (placeholders)
    # -------------------------
    # TODO: trend detection by comparing current window to previous windows
    # TODO: peak detection by maintaining rolling max of flight_count per cell
    # TODO: anomaly scoring (e.g., Poisson baseline deviation)

    # -------------------------
    # E) Enrichment & Context (placeholders)
    # -------------------------
    # TODO: reverse-geocode lat_bin/lon_bin to country/region
    # TODO: count inbound/outbound flights in airport boxes (e.g. ±1° around lat/lon)
    # TODO: join with weather/warnings streams for contextual risk

    # -------------------------
    # F) Machine-learning Features (placeholders)
    # -------------------------
    # TODO: assemble per-flight feature vectors [v, a, ω, dt_contact, …] for ML
    # TODO: apply streaming KMeans or isolation forests for anomaly detection

    # -------------------------
    # OUTPUT STREAMS
    # -------------------------
    # 1) Emit per-flight metrics
    per_flight_out = scored.select(
        to_json(struct(
            "icao24", "fetch_time", "risk_score",
            "acceleration", "turn_rate",
            "alt_stability_idx", "dt_last_contact",
            "altitude_delta"
        )).alias("value")
    )
    # Debug: print to console
    per_flight_out.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start()
    # Publish per-flight metrics to Kafka topic `flight-metrics`
    per_flight_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-metrics") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-metrics") \
        .start()

    # 2) Emit spatial aggregates
    agg_out = agg.select(
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "lat_bin", "lon_bin", "avg_risk", "flight_count"
        )).alias("value")
    )
    # Debug: print to console with update mode
    agg_out.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", False) \
        .start()
    # Publish spatial aggregates to Kafka topic `flight-aggregates`
    agg_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-aggregates") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-aggregates-v2") \
        .start()

    # Block until all streams terminate
    spark.streams.awaitAnyTermination()
