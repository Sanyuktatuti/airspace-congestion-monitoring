# streaming_test.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType, ArrayType, MapType
from risk_model import compute_risk_score

# Use the same schema you have
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

spark = (
    SparkSession.builder
        .appName("FlightStreamTest")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "flight-stream") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
flights = raw.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Add risk score
scored = flights.withColumn(
    "risk_score",
    compute_risk_score(
        col("velocity"),
        col("vertical_rate"),
        col("on_ground")
    )
)

# Output to console
query = scored.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

# Keep running
query.awaitTermination()