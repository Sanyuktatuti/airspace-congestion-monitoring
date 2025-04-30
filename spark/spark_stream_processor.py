from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr,
    to_json, struct,
    floor, window, avg, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, ArrayType, MapType, TimestampType
)

# 1°) incoming message schema
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

# size of each grid cell (degrees)
GRID_SIZE = 1.0  

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("AirspaceCongestionStreaming") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. RAW → SCORED → flight-scores ─────────────────────────────────────

    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "flight-stream") \
        .option("startingOffsets", "latest") \
        .load()

    flights = raw.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    scored = flights.withColumn(
        "risk_score",
        expr(
            "(CASE WHEN NOT on_ground AND velocity IS NOT NULL "
            "THEN LEAST(velocity/250.0,1.0) ELSE 0 END) + "
            "(CASE WHEN vertical_rate IS NOT NULL "
            "THEN LEAST(ABS(vertical_rate)/10.0,1.0) ELSE 0 END)"
        )
    )

    scored_out = scored.select(
        to_json(struct(
            "icao24", "callsign", "latitude", "longitude",
            "baro_altitude", "risk_score", "fetch_time"
        )).alias("value")
    )

    q1 = scored_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-scores") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-scores") \
        .start()

    # ── 2. SCORED → AGGREGATED → flight-aggregates ──────────────────────────

    # turn fetch_time (ms since epoch) into a Timestamp
    with_ts = scored.withColumn(
        "event_ts",
        expr("CAST(fetch_time/1000 AS TIMESTAMP)")
    )

    binned = with_ts \
        .withColumn("lat_bin", floor((col("latitude") + 90.0) / GRID_SIZE)) \
        .withColumn("lon_bin", floor((col("longitude") + 180.0) / GRID_SIZE))

    agg = binned \
        .withWatermark("event_ts", "2 minutes") \
        .groupBy(
            window(col("event_ts"), "1 minute"),
            col("lat_bin"), col("lon_bin")
        ) \
        .agg(
            avg("risk_score").alias("avg_risk"),
            count("*").alias("flight_count")
        )

    agg_out = agg.select(
        to_json(struct(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "lat_bin", "lon_bin", "avg_risk", "flight_count"
        )).alias("value")
    )

    q2 = agg_out.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "flight-aggregates") \
        .option("checkpointLocation", "/tmp/spark-checkpoints/flight-aggregates-v2") \
        .start()

    # ── WAIT FOR EITHER STREAM TO TERMINATE ────────────────────────────────
    spark.streams.awaitAnyTermination()
