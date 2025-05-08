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
    when, lit, expr, max, min, first, last, stddev
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, ArrayType, MapType
)
from pyspark.sql.window import Window

import os
os.makedirs("/tmp/spark-checkpoints", exist_ok=True)

# Import pure-function metric calculators
from spark.risk_model import compute_risk_score

# -------------------------
# 1°) Schema & Constants
# -------------------------
# Define the expected JSON schema of incoming messages from Kafka "flight-stream".
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
GRID_SIZE = 1.0

if __name__ == "__main__":
    # -------------------------
    # 2°) SparkSession Setup
    # -------------------------
    spark = (
        SparkSession.builder
            .appName("AirspaceCongestionStreaming")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
            )
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.memory", "2g")  # Increase driver memory
            .config("spark.executor.memory", "2g")  # Increase executor memory
            .config("spark.memory.offHeap.enabled", "true")  # Enable off-heap memory
            .config("spark.memory.offHeap.size", "1g")  # Set off-heap memory size
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # -------------------------
    # 3°) Read Raw Stream
    # -------------------------
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "flight-stream") \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse the JSON payload into typed columns
    flights = raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    # ── fill nulls: numeric→0, strings→"", booleans→False
    flights = flights \
        .na.fill(0, subset=[
            'time_position', 'last_contact', 'longitude',
            'latitude', 'baro_altitude', 'velocity',
            'true_track', 'vertical_rate', 'geo_altitude',
            'fetch_time'
        ]) \
        .na.fill("", subset=[
            'icao24', 'callsign', 'origin_country', 'squawk', 'position_source'
        ]) \
        .na.fill(False, subset=['on_ground', 'spi'])

    print(">>> Schema of processed flights:")
    flights.printSchema()

    # -------------------------
    # 4°) Add event timestamp and compute basic risk
    # -------------------------
    
    # Add timestamp column for windowing and watermarking
    flightsWithTS = flights.withColumn(
        "event_ts", 
        expr("CAST(fetch_time/1000 AS TIMESTAMP)")
    )
    
    # Compute risk score (this is a simple operation that works with streaming)
    scored = flightsWithTS.withColumn(
        "risk_score",
        compute_risk_score(
            col("velocity"),
            col("vertical_rate"),
            col("on_ground")
        )
    )
    
    # Add altitude delta (simple operation that works with streaming)
    scored = scored.withColumn(
        "altitude_delta",
        col("geo_altitude") - col("baro_altitude")
    )
    
    # Add time since last contact (simple operation)
    scored = scored.withColumn(
        "dt_last_contact",
        (col("fetch_time") - col("last_contact")) / 1000.0
    )

    # -------------------------
    # 5°) Time-window based metrics 
    # -------------------------
    
    # Use 30 second windows with 10 second sliding intervals
    windowedMetrics = scored \
        .withWatermark("event_ts", "30 seconds") \
        .groupBy(
            window(col("event_ts"), "30 seconds", "10 seconds"),
            col("icao24")
        ) \
        .agg(
            # Identify metrics that can be computed within a window
            avg("risk_score").alias("avg_window_risk"),
            max("risk_score").alias("max_window_risk"),
            min("risk_score").alias("min_window_risk"),
            
            # Compute approximate acceleration as velocity change over time
            (max("velocity") - min("velocity")).alias("velocity_delta"),
            (max("fetch_time") - min("fetch_time")).alias("time_delta_ms"),
            
            # Track heading changes for turn rate estimation
            (max("true_track") - min("true_track")).alias("heading_delta"),
            
            # Altitude stability approximation
            stddev("baro_altitude").alias("alt_stability_idx"),
            
            # Keep last values of important fields
            last("longitude").alias("longitude"),
            last("latitude").alias("latitude"),
            last("baro_altitude").alias("baro_altitude"),
            last("geo_altitude").alias("geo_altitude"),
            last("velocity").alias("velocity"),
            last("vertical_rate").alias("vertical_rate"),
            last("on_ground").alias("on_ground"),
            last("dt_last_contact").alias("dt_last_contact"),
            last("altitude_delta").alias("altitude_delta"),
            last("fetch_time").alias("fetch_time")
        )
    
    # Compute derived metrics that depend on aggregates
    enrichedMetrics = windowedMetrics \
        .withColumn(
            "acceleration",
            when(
                col("time_delta_ms") > 0, 
                col("velocity_delta") / (col("time_delta_ms") / 1000.0)
            ).otherwise(lit(0.0))
        ) \
        .withColumn(
            "turn_rate",
            when(
                col("time_delta_ms") > 0,
                col("heading_delta") / (col("time_delta_ms") / 1000.0)
            ).otherwise(lit(0.0))
        ) \
        .withColumn(
            "risk_anomaly",
            col("max_window_risk") > (col("avg_window_risk") + 0.5)
        )
    
    # -------------------------
    # 6°) Spatial Aggregates
    # -------------------------
    
    # Bin flights into grid cells (for visualization/analytics)
    print(">>> Starting grid cell binning...")
    binned = scored \
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
            when(col("baro_altitude") < 10000, lit("low"))
            .when(col("baro_altitude") > 30000, lit("high"))
            .otherwise(lit("mid"))
        )

    print(">>> Computing grid aggregates...")
    # Compute grid cell aggregates
    gridAggregates = binned \
        .withWatermark("event_ts", "30 seconds") \
        .groupBy(
            window(col("event_ts"), "30 seconds", "10 seconds"),
            col("lat_bin"), 
            col("lon_bin")
        ) \
        .agg(
            avg("risk_score").alias("avg_risk"),
            count("icao24").alias("flight_count")
        )

    # Add debug output for grid aggregates
    gridAggregates.printSchema()
    print(">>> Grid aggregates schema shown above")
    print(">>> Starting streaming queries...")
    
    # Altitude band occupancy
    altitudeBands = binned \
        .withWatermark("event_ts", "30 seconds") \
        .groupBy(
            window(col("event_ts"), "30 seconds", "10 seconds"),
            col("alt_band")
        ) \
        .agg(
            count("icao24").alias("band_count"),
            avg("risk_score").alias("band_avg_risk")
        )
    
    # -------------------------
    # 7°) Output Streams
    # -------------------------
    
    # 7.1) Flight metrics to console and Kafka
    flightMetricsOutput = enrichedMetrics.select(
        col("icao24"),
        col("fetch_time"),
        col("avg_window_risk").alias("avg_risk"),
        col("acceleration"),
        col("turn_rate"),
        col("alt_stability_idx"),
        col("risk_anomaly"),
        col("longitude"),
        col("latitude"),
        col("baro_altitude"),
        col("dt_last_contact"),
        col("altitude_delta")
    )
    
    # Console output for debugging
    consoleQuery = flightMetricsOutput.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start()
    
    # Kafka flight-metrics topic
    flightMetricsKafka = flightMetricsOutput.select(
        col("icao24").alias("key"),
        to_json(struct("*")).alias("value")
    )
    
    kafkaFlightMetricsQuery = flightMetricsKafka.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-metrics") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-metrics") \
        .outputMode("append") \
        .start()
    
    # 7.2) Grid cell aggregates to Kafka
    gridAggOutput = gridAggregates.select(
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("lat_bin"),
            col("lon_bin"),
            col("avg_risk"),
            col("flight_count")
        )).alias("value")
    )
    
    kafkaGridAggQuery = gridAggOutput.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-aggregates") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-aggregates") \
        .outputMode("update") \
        .start()
    
    # 7.3) JSON files for dashboard
    jsonQuery = flightMetricsOutput.writeStream \
        .format("json") \
        .option("path", "data/processed") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/json-writer") \
        .outputMode("append") \
        .start()
    
    # Wait for all queries to terminate
    spark.streams.awaitAnyTermination()