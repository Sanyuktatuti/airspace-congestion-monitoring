# spark/geo_density.py
# ----------------------------------------------
# Geospatial Density Streaming Processor
# ----------------------------------------------
# This module reads enriched flight positions from Kafka, converts them into
# spatial geometries using Apache Sedona, and computes rolling/aggregated
# aircraft density per grid cell or via kernel-density estimation.
# Results are emitted back to Kafka (or to flat files) for downstream
# visualization (e.g. heatmaps on Mapbox).

from pyspark.sql import SparkSession
# Sedona SQL functions and types
from sedona.register import SedonaRegistrator
from sedona.sql.types import GeometryType
from pyspark.sql.functions import (
    from_json, col, expr,
    window, count, avg,
    udf
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType,
)
# You may need to import Sedona's spatial functions if using SQL API:
# from sedona.sql.functions import ST_Point, ST_Polygon, ST_Envelope

# ----------------------------------------------
# 1) Define the expected incoming JSON schema
# ----------------------------------------------
schema = StructType([
    StructField("icao24", StringType()),       # Aircraft identifier
    StructField("latitude", DoubleType()),     # Position: latitude
    StructField("longitude", DoubleType()),    # Position: longitude
    StructField("baro_altitude", DoubleType()),# Barometric altitude (m)
    StructField("geo_altitude", DoubleType()), # GPS altitude (m)
    StructField("risk_score", DoubleType()),   # Pre-computed risk metric
    StructField("fetch_time", LongType()),     # Fetch timestamp (ms since epoch)
])

# Grid cell size (in degrees) for uniform tiling
GRID_SIZE = 1.0

# ----------------------------------------------
# 2) Initialize SparkSession with Sedona
# ----------------------------------------------
def create_spark_session():
    """
    Creates a SparkSession and registers Sedona functions.
    Ensure the Sedona package is provided via --packages.
    """
    spark = (
        SparkSession.builder
            .appName("GeoDensityStreaming")
            # Include Sedona SQL and spatial dependencies
            .config("spark.jars.packages",
                    "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.3.1,"
                    "org.datasyslab:geotools-wrapper:1.1.0-25.2")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator",
                    "org.apache.sedona.core.serde.SedonaKryoRegistrator")
            .getOrCreate()
    )
    # Register Sedona (GeoSpark) functions & types
    SedonaRegistrator.registerAll(spark)
    return spark

# ----------------------------------------------
# 3) Read enriched flight-stream from Kafka
# ----------------------------------------------
def read_flight_stream(spark):
    """
    Reads raw Kafka topic 'flight-stream' containing JSON-encoded
    flight position & metrics, parses them into a DataFrame.
    """
    df = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "flight-stream")
            .option("startingOffsets", "earliest")
            .load()
    )
    # value is binary; cast to string and parse JSON
    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    return parsed

# ----------------------------------------------
# 4) Create spatial geometry from lat/lon
# ----------------------------------------------
def enrich_with_geometry(df):
    """
    Adds a Geometry column (Sedona Point) to each row based on
    latitude and longitude. Format: POINT(longitude latitude).
    """
    # Use Sedona SQL function ST_Point(x, y)
    # Note: You may need to register it as a UDF if not directly available.
    # Example if using expression:
    return df.withColumn(
        "geom",
        expr("ST_Point(longitude, latitude)")
    )

# ----------------------------------------------
# 5) Uniform Grid Density Aggregation
# ----------------------------------------------
def compute_grid_density(df):
    """
    Tiles the airspace into uniform GRID_SIZE° cells, then computes
    aircraft count and average risk per cell over tumbling windows.
    """
    # 1) Assign integer grid indices for lat/lon
    binned = df.withColumn(
        "lat_bin", expr(f"floor((latitude + 90)/{GRID_SIZE})"))
    binned = binned.withColumn(
        "lon_bin", expr(f"floor((longitude + 180)/{GRID_SIZE})"))

    # 2) Cast fetch_time (ms) to TimestampType for windowing
    binned = binned.withColumn(
        "event_ts",
        expr("CAST(fetch_time/1000 AS TIMESTAMP)")
    )

    # 3) Group by tumbling time window + spatial bins
    agg = (
        binned
        .withWatermark("event_ts", "10 seconds")   # allow 10s lateness
        .groupBy(
            window(col("event_ts"), "30 seconds"),  # 30s tumbling window
            col("lat_bin"), col("lon_bin")
        )
        .agg(
            count("geom").alias("flight_count"),
            avg("risk_score").alias("avg_risk")
        )
    )
    return agg

# ----------------------------------------------
# 6) Kernel Density Estimation (placeholder)
# ----------------------------------------------
def compute_kernel_density(df):
    """
    Applies a kernel density estimator over point stream to produce
    smooth density surfaces rather than hard grid bins.
    Typically uses ST_KernelDensity or external library.
    """
    # Placeholder: real implementation depends on Sedona support.
    # e.g. SELECT ST_KernelDensity(geom, bandwidth, cell_size) ...
    return df

# ----------------------------------------------
# 7) Write results to sink
# ----------------------------------------------
def write_to_kafka(df, topic, checkpoint_loc):
    """
    Writes a DataFrame to a Kafka topic as JSON.
    Serializes all columns into a single JSON 'value'.
    """
    out = df.select(
        to_json(struct([col(c) for c in df.columns])).alias("value")
    )
    return (
        out.writeStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "localhost:9092")
           .option("topic", topic)
           .option("checkpointLocation", checkpoint_loc)
           .start()
    )

# ----------------------------------------------
# 8) Main execution flow
# ----------------------------------------------
def main():
    spark = create_spark_session()
    flights = read_flight_stream(spark)
    # add geometry
    with_geom = enrich_with_geometry(flights)

    # Option A: uniform grid density
    grid_agg = compute_grid_density(with_geom)
    # emit grid density
    write_to_kafka(grid_agg, "flight-density-grid", "/tmp/checkpoints/density-grid")

    # Option B: kernel density (if implemented)
    # kde = compute_kernel_density(with_geom)
    # write_to_kafka(kde, "flight-density-kde", "/tmp/checkpoints/density-kde")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
