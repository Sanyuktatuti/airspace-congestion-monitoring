"""
risk_model.py

Pure‐function library for per‐flight risk scoring and derived metrics.
Each function returns a Column expression that you can plug directly
into .withColumn("…", …) in your Spark streaming pipeline.

All time deltas assume `fetch_time` and `last_contact` are Unix
timestamps in **milliseconds**.
"""

from pyspark.sql.functions import (
    when, least, abs as sql_abs, lag, stddev, lit, col
)
from pyspark.sql.window import Window

def compute_risk_score(velocity_col, vertical_rate_col, on_ground_col):
    """
    Compute a two‐part risk score:
      1. speed component:   risk_speed = min( velocity / 250.0 , 1.0 )
         • if aircraft on ground OR velocity is NULL ⇒ 0
      2. climb component:   risk_climb = min( |vertical_rate| / 10.0 , 1.0 )
         • if vertical_rate is NULL ⇒ 0
      total risk_score = risk_speed + risk_climb

    Units:
      - velocity [m/s]
      - vertical_rate [m/s]
    Output:
      - risk_score in [0,2]
    """
    # risk from horizontal speed
    risk_speed = when(
        (~on_ground_col) & velocity_col.isNotNull(),
        least(velocity_col / 250.0, lit(1.0))
    ).otherwise(lit(0.0))

    # risk from climb/descent rate
    risk_climb = when(
        vertical_rate_col.isNotNull(),
        least(sql_abs(vertical_rate_col) / 10.0, lit(1.0))
    ).otherwise(lit(0.0))

    return risk_speed + risk_climb


def compute_acceleration(velocity_col, fetch_time_col, flight_window):
    """
    Compute instantaneous acceleration a_t = (v_t − v_{t−1}) / Δt

    - v_t       = current velocity_col [m/s]
    - v_{t−1}   = lag(velocity_col) over flight_window
    - Δt [s]     = (fetch_time_t − fetch_time_{t−1}) / 1000

    Returns Column with units [m/s²].  Null for first record.
    """
    prev_v = lag(velocity_col).over(flight_window)
    prev_t = lag(fetch_time_col).over(flight_window)
    delta_v = velocity_col - prev_v
    delta_t = (fetch_time_col - prev_t) / 1000.0
    return delta_v / delta_t


def compute_turn_rate(true_track_col, fetch_time_col, flight_window):
    """
    Compute turn rate ω_t = (θ_t − θ_{t−1}) / Δt

    - θ_t      = current heading true_track_col [degrees]
    - θ_{t−1}  = lag(true_track_col) over flight_window
    - Δt [s]    = (fetch_time_t − fetch_time_{t−1}) / 1000

    Returns Column with units [deg/s]. Null for first record.
    """
    prev_tr = lag(true_track_col).over(flight_window)
    prev_t  = lag(fetch_time_col).over(flight_window)
    delta_heading = true_track_col - prev_tr
    delta_t = (fetch_time_col - prev_t) / 1000.0
    return delta_heading / delta_t


def compute_alt_stability(baro_altitude_col, flight_window, lookback=5):
    """
    Altitude Stability Index = rolling standard deviation of
    barometric altitude over current + previous `lookback` rows.

    - baro_altitude_col [m]
    - lookback = 5 ⇒ window of size 6
    """
    stability_window = flight_window.rowsBetween(-lookback, 0)
    return stddev(baro_altitude_col).over(stability_window)


def compute_dt_last_contact(fetch_time_col, last_contact_col):
    """
    Time since last contact Δt_contact = (fetch_time − last_contact) / 1000
    Units: seconds [s]
    """
    return (fetch_time_col - last_contact_col) / 1000.0


def compute_altitude_delta(geo_alt_col, baro_alt_col):
    """
    Difference between GPS (geo_altitude) and barometric altitude:
      altitude_delta = geo_altitude − baro_altitude
    Units: [m]
    """
    return geo_alt_col - baro_alt_col