#!/usr/bin/env python3
"""
Historical Flight Metrics Analytics

This module performs batch analytics on historical flight data stored in InfluxDB.
It identifies patterns, congestion hotspots, and anomalies over time periods.
"""

import os
import sys
import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, expr, hour, minute, dayofweek, month,
    count, avg, stddev, max, min, percentile_approx,
    lag, lead, rank, dense_rank, row_number
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

# Import the risk model for consistency with streaming
from spark.risk_model import compute_risk_score

class HistoricalAnalytics:
    """Historical analytics processor for flight metrics data"""
    
    def __init__(self, influx_host="localhost", influx_port=8086, 
                 influx_org="airspace", influx_bucket="flight_metrics"):
        """Initialize the analytics processor"""
        self.influx_host = influx_host
        self.influx_port = influx_port
        self.influx_org = influx_org
        self.influx_bucket = influx_bucket
        
        # Initialize Spark session
        self.spark = (
            SparkSession.builder
                .appName("AirspaceHistoricalAnalytics")
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Create output directory
        os.makedirs("data/analytics", exist_ok=True)
    
    def load_data_from_influxdb(self, start_time, end_time):
        """
        Load historical data from InfluxDB using Spark JDBC connector
        
        Args:
            start_time: Start time for data query (ISO format string)
            end_time: End time for data query (ISO format string)
        
        Returns:
            DataFrame with historical flight metrics
        """
        # For demo purposes, we'll simulate loading from InfluxDB
        # In a production environment, use the InfluxDB JDBC connector
        
        print(f"Loading data from InfluxDB ({start_time} to {end_time})")
        
        # Check if we have a local cache file to use instead
        cache_file = "data/analytics/influxdb_cache.parquet"
        if os.path.exists(cache_file):
            print(f"Using cached data from {cache_file}")
            return self.spark.read.parquet(cache_file)
        
        # In a real implementation, we would use code like this:
        # return (
        #     self.spark.read
        #         .format("jdbc")
        #         .option("url", f"jdbc:influxdb://{self.influx_host}:{self.influx_port}")
        #         .option("dbtable", f"{self.influx_bucket}")
        #         .option("user", "influxdb")
        #         .option("password", os.environ.get("INFLUXDB_TOKEN", ""))
        #         .option("query", f"""
        #             SELECT *
        #             FROM flight_metrics
        #             WHERE time >= '{start_time}' AND time <= '{end_time}'
        #         """)
        #         .load()
        # )
        
        # For demo, generate some sample data
        print("No cached data found. Generating sample data...")
        
        # Convert ISO strings to datetime objects
        start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        
        # Generate a sample DataFrame with synthetic data
        return self._generate_sample_data(start_dt, end_dt)
    
    def _generate_sample_data(self, start_dt, end_dt):
        """Generate sample data for demonstration purposes"""
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
        import random
        
        # Define schema for sample data
        schema = StructType([
            StructField("time", TimestampType()),
            StructField("icao24", StringType()),
            StructField("longitude", DoubleType()),
            StructField("latitude", DoubleType()),
            StructField("baro_altitude", DoubleType()),
            StructField("velocity", DoubleType()),
            StructField("vertical_rate", DoubleType()),
            StructField("on_ground", BooleanType()),
            StructField("avg_risk", DoubleType()),
            StructField("acceleration", DoubleType()),
            StructField("turn_rate", DoubleType()),
            StructField("alt_stability_idx", DoubleType()),
        ])
        
        # Generate sample data
        data = []
        current_dt = start_dt
        
        # Create 100 aircraft
        aircraft_ids = [f"a{i:05d}" for i in range(100)]
        
        # Create flight patterns - morning/evening peaks
        while current_dt < end_dt:
            hour = current_dt.hour
            
            # Simulate higher traffic during morning (7-9) and evening (17-19) peaks
            if 7 <= hour <= 9 or 17 <= hour <= 19:
                num_flights = random.randint(70, 100)
            else:
                num_flights = random.randint(30, 70)
            
            # Select random aircraft for this time
            active_aircraft = random.sample(aircraft_ids, num_flights)
            
            for icao in active_aircraft:
                # Generate flight data with some patterns
                is_peak = 7 <= hour <= 9 or 17 <= hour <= 19
                
                # Base values
                longitude = random.uniform(-180, 180)
                latitude = random.uniform(-90, 90)
                
                # Create congested areas during peak times
                if is_peak:
                    # Create 3 congestion hotspots
                    hotspot = random.randint(0, 2)
                    if hotspot == 0:
                        longitude = random.uniform(-74.2, -73.8)  # NYC area
                        latitude = random.uniform(40.6, 41.0)
                    elif hotspot == 1:
                        longitude = random.uniform(-0.5, 0.3)  # London area
                        latitude = random.uniform(51.3, 51.7)
                    else:
                        longitude = random.uniform(103.8, 104.0)  # Singapore area
                        latitude = random.uniform(1.2, 1.4)
                
                # Altitude patterns
                if is_peak:
                    baro_altitude = random.uniform(8000, 12000)  # More mid-altitude flights during peaks
                else:
                    baro_altitude = random.uniform(30000, 40000)  # More high-altitude during off-peak
                
                # Risk patterns - higher during peaks
                base_risk = 0.3 if is_peak else 0.1
                avg_risk = base_risk + random.uniform(0, 0.5)
                
                # Other metrics
                velocity = random.uniform(200, 900)  # m/s
                vertical_rate = random.uniform(-10, 10)  # m/s
                on_ground = random.random() < 0.05  # 5% chance of being on ground
                acceleration = random.uniform(-2, 2)
                turn_rate = random.uniform(-5, 5)
                alt_stability_idx = random.uniform(0, 100)
                
                # Add some anomalies (1% chance)
                if random.random() < 0.01:
                    avg_risk = random.uniform(0.8, 1.5)
                    velocity = random.uniform(100, 1100)
                    vertical_rate = random.uniform(-30, 30)
                    acceleration = random.uniform(-5, 5)
                    turn_rate = random.uniform(-20, 20)
                    alt_stability_idx = random.uniform(80, 200)
                
                data.append((
                    current_dt, icao, longitude, latitude, baro_altitude,
                    velocity, vertical_rate, on_ground, avg_risk,
                    acceleration, turn_rate, alt_stability_idx
                ))
            
            # Advance time by 5 minutes
            current_dt += timedelta(minutes=5)
        
        # Create DataFrame
        df = self.spark.createDataFrame(data, schema)
        
        # Cache the data for future use
        df.write.mode("overwrite").parquet("data/analytics/influxdb_cache.parquet")
        
        return df
    
    def analyze_temporal_patterns(self, df):
        """
        Analyze temporal patterns in flight data
        
        Args:
            df: DataFrame with historical flight data
            
        Returns:
            Dictionary of DataFrames with analysis results
        """
        print("Analyzing temporal patterns...")
        
        # Add time-based columns
        df = df.withColumn("hour_of_day", hour(col("time")))
        df = df.withColumn("day_of_week", dayofweek(col("time")))
        df = df.withColumn("month", month(col("time")))
        
        # Hourly patterns
        hourly_patterns = df.groupBy("hour_of_day").agg(
            count("icao24").alias("flight_count"),
            avg("avg_risk").alias("avg_risk"),
            avg("velocity").alias("avg_velocity"),
            avg("baro_altitude").alias("avg_altitude")
        ).orderBy("hour_of_day")
        
        # Daily patterns
        daily_patterns = df.groupBy("day_of_week").agg(
            count("icao24").alias("flight_count"),
            avg("avg_risk").alias("avg_risk"),
            avg("velocity").alias("avg_velocity"),
            avg("baro_altitude").alias("avg_altitude")
        ).orderBy("day_of_week")
        
        # Save results
        hourly_patterns.write.mode("overwrite").json("data/analytics/hourly_patterns")
        daily_patterns.write.mode("overwrite").json("data/analytics/daily_patterns")
        
        return {
            "hourly": hourly_patterns,
            "daily": daily_patterns
        }
    
    def identify_congestion_hotspots(self, df):
        """
        Identify geographical congestion hotspots
        
        Args:
            df: DataFrame with historical flight data
            
        Returns:
            DataFrame with congestion hotspots
        """
        print("Identifying congestion hotspots...")
        
        # Grid-based approach - bin flights into geographical cells
        grid_size = 1.0  # 1 degree grid cells
        
        grid_df = df.withColumn(
            "lat_bin",
            expr(f"floor((latitude + 90.0) / {grid_size})")
        ).withColumn(
            "lon_bin",
            expr(f"floor((longitude + 180.0) / {grid_size})")
        )
        
        # Aggregate by grid cell
        hotspots = grid_df.groupBy("lat_bin", "lon_bin").agg(
            count("icao24").alias("flight_count"),
            avg("avg_risk").alias("avg_risk"),
            avg("baro_altitude").alias("avg_altitude"),
            # Convert bin coordinates back to actual coordinates (cell center)
            (col("lat_bin") * grid_size + grid_size/2 - 90).alias("center_latitude"),
            (col("lon_bin") * grid_size + grid_size/2 - 180).alias("center_longitude")
        )
        
        # Find top hotspots
        top_hotspots = hotspots.orderBy(col("flight_count").desc()).limit(20)
        
        # Save results
        top_hotspots.write.mode("overwrite").json("data/analytics/congestion_hotspots")
        
        return top_hotspots
    
    def detect_risk_anomalies(self, df):
        """
        Detect anomalies in risk patterns
        
        Args:
            df: DataFrame with historical flight data
            
        Returns:
            DataFrame with detected anomalies
        """
        print("Detecting risk anomalies...")
        
        # Calculate global statistics
        stats = df.agg(
            avg("avg_risk").alias("global_avg_risk"),
            stddev("avg_risk").alias("global_stddev_risk")
        ).collect()[0]
        
        global_avg = stats["global_avg_risk"]
        global_stddev = stats["global_stddev_risk"]
        
        # Define threshold for anomalies (3 standard deviations)
        threshold = global_avg + 3 * global_stddev
        
        # Identify anomalies
        anomalies = df.filter(col("avg_risk") > threshold)
        
        # Group anomalies by time windows
        anomaly_windows = anomalies.withWatermark("time", "1 hour") \
            .groupBy(window("time", "1 hour")) \
            .agg(
                count("icao24").alias("anomaly_count"),
                avg("avg_risk").alias("avg_anomaly_risk"),
                max("avg_risk").alias("max_anomaly_risk")
            )
        
        # Save results
        anomaly_windows.select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("anomaly_count"),
            col("avg_anomaly_risk"),
            col("max_anomaly_risk")
        ).write.mode("overwrite").json("data/analytics/risk_anomalies")
        
        return anomaly_windows
    
    def cluster_flight_behaviors(self, df):
        """
        Cluster flights based on their behavior patterns
        
        Args:
            df: DataFrame with historical flight data
            
        Returns:
            DataFrame with cluster assignments
        """
        print("Clustering flight behaviors...")
        
        # Aggregate metrics by flight (icao24)
        flight_metrics = df.groupBy("icao24").agg(
            avg("avg_risk").alias("avg_risk"),
            avg("velocity").alias("avg_velocity"),
            avg("baro_altitude").alias("avg_altitude"),
            avg("vertical_rate").alias("avg_vertical_rate"),
            avg("acceleration").alias("avg_acceleration"),
            avg("turn_rate").alias("avg_turn_rate"),
            stddev("baro_altitude").alias("altitude_variability"),
            stddev("velocity").alias("velocity_variability")
        )
        
        # Prepare features for clustering
        feature_cols = [
            "avg_risk", "avg_velocity", "avg_altitude", "avg_vertical_rate",
            "avg_acceleration", "avg_turn_rate", "altitude_variability", "velocity_variability"
        ]
        
        # Assemble features into a vector
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        flight_features = assembler.transform(flight_metrics)
        
        # Apply K-means clustering
        kmeans = KMeans(k=5, seed=42)
        model = kmeans.fit(flight_features)
        clustered = model.transform(flight_features)
        
        # Evaluate clustering
        evaluator = ClusteringEvaluator()
        silhouette = evaluator.evaluate(clustered)
        print(f"Silhouette score: {silhouette}")
        
        # Analyze clusters
        cluster_analysis = clustered.groupBy("prediction").agg(
            count("icao24").alias("flight_count"),
            avg("avg_risk").alias("cluster_avg_risk"),
            avg("avg_velocity").alias("cluster_avg_velocity"),
            avg("avg_altitude").alias("cluster_avg_altitude"),
            avg("avg_vertical_rate").alias("cluster_avg_vertical_rate"),
            avg("avg_acceleration").alias("cluster_avg_acceleration"),
            avg("avg_turn_rate").alias("cluster_avg_turn_rate")
        ).orderBy("prediction")
        
        # Save results
        cluster_analysis.write.mode("overwrite").json("data/analytics/flight_clusters")
        
        return {
            "clusters": clustered,
            "analysis": cluster_analysis,
            "silhouette": silhouette
        }
    
    def generate_insights_report(self, results):
        """
        Generate a comprehensive insights report
        
        Args:
            results: Dictionary of analysis results
            
        Returns:
            String with report content
        """
        print("Generating insights report...")
        
        # Format report
        report = []
        report.append("# Airspace Congestion Historical Analytics Report")
        report.append(f"Generated on: {datetime.now().isoformat()}")
        report.append("\n## Temporal Patterns")
        
        # Add hourly patterns
        hourly = results.get("temporal", {}).get("hourly")
        if hourly:
            peak_hour = hourly.orderBy(col("flight_count").desc()).first()
            report.append(f"Peak traffic hour: {peak_hour['hour_of_day']}:00 with {peak_hour['flight_count']} flights")
            
            highest_risk_hour = hourly.orderBy(col("avg_risk").desc()).first()
            report.append(f"Highest risk hour: {highest_risk_hour['hour_of_day']}:00 with risk score {highest_risk_hour['avg_risk']:.2f}")
        
        # Add congestion hotspots
        hotspots = results.get("hotspots")
        if hotspots:
            report.append("\n## Congestion Hotspots")
            top_spot = hotspots.first()
            report.append(f"Top congestion area: Lat {top_spot['center_latitude']:.2f}, Lon {top_spot['center_longitude']:.2f}")
            report.append(f"Flight count: {top_spot['flight_count']}")
            report.append(f"Average risk: {top_spot['avg_risk']:.2f}")
        
        # Add anomalies
        anomalies = results.get("anomalies")
        if anomalies:
            report.append("\n## Risk Anomalies")
            anomaly_count = anomalies.count()
            report.append(f"Detected {anomaly_count} anomaly windows")
            
            if anomaly_count > 0:
                worst_anomaly = anomalies.orderBy(col("max_anomaly_risk").desc()).first()
                report.append(f"Worst anomaly: {worst_anomaly['max_anomaly_risk']:.2f} risk score")
                report.append(f"Time window: {worst_anomaly['window.start']} to {worst_anomaly['window.end']}")
        
        # Add clustering insights
        clusters = results.get("clusters", {}).get("analysis")
        if clusters:
            report.append("\n## Flight Behavior Clusters")
            report.append(f"Silhouette score: {results.get('clusters', {}).get('silhouette', 0):.2f}")
            
            # Get clusters as rows
            cluster_rows = clusters.collect()
            for row in cluster_rows:
                report.append(f"\nCluster {row['prediction']}: {row['flight_count']} flights")
                report.append(f"  Avg Risk: {row['cluster_avg_risk']:.2f}")
                report.append(f"  Avg Altitude: {row['cluster_avg_altitude']:.0f} m")
                report.append(f"  Avg Velocity: {row['cluster_avg_velocity']:.0f} m/s")
        
        # Save report
        report_text = "\n".join(report)
        with open("data/analytics/insights_report.md", "w") as f:
            f.write(report_text)
        
        return report_text
    
    def run_analysis(self, start_time, end_time):
        """
        Run the complete analytics pipeline
        
        Args:
            start_time: Start time for analysis (ISO format)
            end_time: End time for analysis (ISO format)
            
        Returns:
            Dictionary with all analysis results
        """
        # Load data
        df = self.load_data_from_influxdb(start_time, end_time)
        
        # Cache the DataFrame to improve performance
        df.cache()
        
        # Run analyses
        results = {}
        results["temporal"] = self.analyze_temporal_patterns(df)
        results["hotspots"] = self.identify_congestion_hotspots(df)
        results["anomalies"] = self.detect_risk_anomalies(df)
        results["clusters"] = self.cluster_flight_behaviors(df)
        
        # Generate report
        report = self.generate_insights_report(results)
        results["report"] = report
        
        # Release cache
        df.unpersist()
        
        return results

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Historical Flight Metrics Analytics")
    parser.add_argument("--start", default="2023-01-01T00:00:00Z", 
                        help="Start time (ISO format, default: 2023-01-01T00:00:00Z)")
    parser.add_argument("--end", default="2023-01-07T23:59:59Z",
                        help="End time (ISO format, default: 2023-01-07T23:59:59Z)")
    parser.add_argument("--influx-host", default="localhost",
                        help="InfluxDB host (default: localhost)")
    parser.add_argument("--influx-port", type=int, default=8086,
                        help="InfluxDB port (default: 8086)")
    parser.add_argument("--influx-org", default="airspace",
                        help="InfluxDB organization (default: airspace)")
    parser.add_argument("--influx-bucket", default="flight_metrics",
                        help="InfluxDB bucket (default: flight_metrics)")
    
    args = parser.parse_args()
    
    print(f"Starting historical analytics from {args.start} to {args.end}")
    
    # Initialize analytics processor
    analytics = HistoricalAnalytics(
        influx_host=args.influx_host,
        influx_port=args.influx_port,
        influx_org=args.influx_org,
        influx_bucket=args.influx_bucket
    )
    
    # Run analysis
    results = analytics.run_analysis(args.start, args.end)
    
    print("Analytics complete. Results saved to data/analytics/")
    print(f"Report saved to data/analytics/insights_report.md")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 