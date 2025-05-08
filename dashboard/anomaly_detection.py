#!/usr/bin/env python3
"""
anomaly_detection.py

Anomaly detection algorithms for flight data analysis.
Detects unusual patterns, sudden changes, and statistical outliers in flight parameters.
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, List, Tuple, Union, Optional


def detect_statistical_outliers(
    data: pd.Series, 
    threshold: float = 3.0,
    window_size: int = None
) -> pd.Series:
    """
    Detect statistical outliers using Z-score method.
    
    Args:
        data: Time series data for a flight parameter
        threshold: Z-score threshold for outlier detection (default: 3.0)
        window_size: If provided, uses rolling window for adaptive thresholds
        
    Returns:
        Boolean series where True indicates an outlier
    """
    if data.empty or len(data) < 2:
        return pd.Series(False, index=data.index)
    
    if window_size and len(data) > window_size:
        # Use rolling window for adaptive anomaly detection
        rolling_mean = data.rolling(window=window_size, center=True).mean()
        rolling_std = data.rolling(window=window_size, center=True).std()
        
        # Handle edge cases where we don't have enough data for the window
        rolling_mean.fillna(data.mean(), inplace=True)
        rolling_std.fillna(data.std(), inplace=True)
        
        # Avoid division by zero
        rolling_std = rolling_std.replace(0, data.std() if data.std() > 0 else 1)
        
        z_scores = (data - rolling_mean) / rolling_std
    else:
        # Use global statistics for smaller datasets
        z_scores = stats.zscore(data, nan_policy='omit')
        z_scores = pd.Series(z_scores, index=data.index)
    
    return abs(z_scores) > threshold


def detect_sudden_changes(
    data: pd.Series, 
    threshold_factor: float = 2.0,
    window_size: int = 3
) -> pd.Series:
    """
    Detect sudden changes in time series data.
    
    Args:
        data: Time series data for a flight parameter
        threshold_factor: Factor multiplied by the standard deviation to set the threshold
        window_size: Size of the rolling window for change detection
        
    Returns:
        Boolean series where True indicates a sudden change
    """
    if data.empty or len(data) < window_size + 1:
        return pd.Series(False, index=data.index)
    
    # Calculate differences between consecutive values
    diff = data.diff().abs()
    
    # Calculate rolling standard deviation of differences
    rolling_std = diff.rolling(window=window_size).std()
    
    # Handle NaN values at the beginning
    overall_std = diff.std()
    rolling_std = rolling_std.fillna(overall_std)
    
    # Set threshold as factor times the rolling standard deviation
    threshold = threshold_factor * rolling_std
    
    # Detect changes that exceed the threshold
    sudden_changes = diff > threshold
    
    # First value will be NaN due to diff(), set it to False
    sudden_changes.iloc[0] = False
    
    return sudden_changes


def detect_trend_deviations(
    data: pd.Series, 
    trend_window: int = 10,
    deviation_threshold: float = 2.0
) -> pd.Series:
    """
    Detect deviations from the established trend.
    
    Args:
        data: Time series data for a flight parameter
        trend_window: Window size for establishing the trend
        deviation_threshold: Threshold for deviation from trend
        
    Returns:
        Boolean series where True indicates a deviation from trend
    """
    if data.empty or len(data) < trend_window + 1:
        return pd.Series(False, index=data.index)
    
    # Calculate the trend using rolling mean
    trend = data.rolling(window=trend_window).mean()
    
    # For the initial window where we don't have enough data, use the first valid trend value
    first_valid_idx = trend.first_valid_index()
    if first_valid_idx:
        first_valid_value = trend.loc[first_valid_idx]
        trend.fillna(first_valid_value, inplace=True)
    
    # Calculate deviations from trend
    deviations = (data - trend).abs()
    
    # Calculate standard deviation of deviations
    deviation_std = deviations.rolling(window=trend_window).std()
    deviation_std.fillna(deviations.std(), inplace=True)
    
    # Avoid division by zero
    deviation_std = deviation_std.replace(0, deviations.std() if deviations.std() > 0 else 1)
    
    # Detect significant deviations
    return deviations > (deviation_threshold * deviation_std)


def detect_altitude_anomalies(
    altitude: pd.Series,
    vertical_rate: Optional[pd.Series] = None,
    z_threshold: float = 3.0,
    change_threshold: float = 2.5,
    trend_threshold: float = 2.0
) -> Dict[str, pd.Series]:
    """
    Detect anomalies in altitude data.
    
    Args:
        altitude: Time series of altitude data
        vertical_rate: Optional time series of vertical rate data
        z_threshold: Z-score threshold for statistical outliers
        change_threshold: Threshold for sudden changes
        trend_threshold: Threshold for trend deviations
        
    Returns:
        Dictionary of anomaly indicators for different types of anomalies
    """
    results = {
        'statistical_outliers': detect_statistical_outliers(altitude, threshold=z_threshold),
        'sudden_changes': detect_sudden_changes(altitude, threshold_factor=change_threshold),
        'trend_deviations': detect_trend_deviations(altitude, deviation_threshold=trend_threshold)
    }
    
    # If vertical rate is provided, check for inconsistencies
    if vertical_rate is not None and not vertical_rate.empty:
        # Altitude should increase when vertical rate is positive and decrease when negative
        expected_direction = vertical_rate.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        actual_direction = altitude.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        # Ignore small vertical rates (near level flight)
        significant_rate = vertical_rate.abs() > 2.0  # m/s
        
        # Find inconsistencies where directions don't match and vertical rate is significant
        inconsistencies = (expected_direction != actual_direction) & significant_rate & (expected_direction != 0)
        
        results['vertical_inconsistencies'] = inconsistencies
    
    # Combine all anomalies
    results['any_anomaly'] = pd.Series(False, index=altitude.index)
    for key, anomaly_series in results.items():
        if key != 'any_anomaly':
            results['any_anomaly'] |= anomaly_series
    
    return results


def detect_speed_anomalies(
    velocity: pd.Series,
    z_threshold: float = 3.0,
    change_threshold: float = 2.0,
    trend_threshold: float = 2.0
) -> Dict[str, pd.Series]:
    """
    Detect anomalies in speed data.
    
    Args:
        velocity: Time series of velocity data
        z_threshold: Z-score threshold for statistical outliers
        change_threshold: Threshold for sudden changes
        trend_threshold: Threshold for trend deviations
        
    Returns:
        Dictionary of anomaly indicators for different types of anomalies
    """
    results = {
        'statistical_outliers': detect_statistical_outliers(velocity, threshold=z_threshold),
        'sudden_changes': detect_sudden_changes(velocity, threshold_factor=change_threshold),
        'trend_deviations': detect_trend_deviations(velocity, deviation_threshold=trend_threshold)
    }
    
    # Check for physically impossible accelerations (more than ~2G)
    # Handle both datetime and non-datetime indices
    try:
        # Try to calculate acceleration using datetime index
        if hasattr(velocity.index, 'dtype') and pd.api.types.is_datetime64_any_dtype(velocity.index.dtype):
            # For datetime index
            time_diff = velocity.index.to_series().diff().dt.total_seconds()
            acceleration = velocity.diff() / time_diff
        else:
            # For non-datetime index (assume constant sampling rate of 1 second)
            acceleration = velocity.diff()
            
        results['extreme_acceleration'] = acceleration.abs() > 20  # m/s²
    except Exception as e:
        print(f"Warning: Could not calculate acceleration: {str(e)}")
        # Create a series of False with same index as velocity
        results['extreme_acceleration'] = pd.Series(False, index=velocity.index)
    
    # Combine all anomalies
    results['any_anomaly'] = pd.Series(False, index=velocity.index)
    for key, anomaly_series in results.items():
        if key != 'any_anomaly':
            results['any_anomaly'] |= anomaly_series
    
    return results


def detect_turn_anomalies(
    heading: pd.Series,
    z_threshold: float = 3.0,
    change_threshold: float = 2.5
) -> Dict[str, pd.Series]:
    """
    Detect anomalies in aircraft heading/direction.
    
    Args:
        heading: Time series of heading data (degrees)
        z_threshold: Z-score threshold for statistical outliers
        change_threshold: Threshold for sudden changes
        
    Returns:
        Dictionary of anomaly indicators for different types of anomalies
    """
    # Convert heading to continuous values to handle 0/360 degree crossings
    # This prevents false detection of sudden changes when crossing north
    sin_heading = np.sin(np.deg2rad(heading))
    cos_heading = np.cos(np.deg2rad(heading))
    
    # Calculate turn rate using the sine and cosine components
    sin_diff = sin_heading.diff()
    cos_diff = cos_heading.diff()
    
    # Calculate the angular difference (this handles the 0/360 crossing properly)
    turn_rate = np.arctan2(sin_diff, cos_diff)
    turn_rate = pd.Series(np.rad2deg(turn_rate), index=heading.index)
    
    results = {
        'statistical_outliers': detect_statistical_outliers(turn_rate, threshold=z_threshold),
        'sudden_changes': detect_sudden_changes(turn_rate, threshold_factor=change_threshold)
    }
    
    # Detect extreme turn rates (more than 5 degrees per second is very high for most aircraft)
    results['extreme_turn_rate'] = turn_rate.abs() > 5.0
    
    # Combine all anomalies
    results['any_anomaly'] = pd.Series(False, index=heading.index)
    for key, anomaly_series in results.items():
        if key != 'any_anomaly':
            results['any_anomaly'] |= anomaly_series
    
    return results


def analyze_flight_anomalies(
    flight_data: pd.DataFrame,
    parameters: Dict[str, Dict] = None
) -> Dict[str, Dict[str, pd.Series]]:
    """
    Analyze flight data for anomalies across multiple parameters.
    
    Args:
        flight_data: DataFrame containing flight parameters over time
        parameters: Dictionary of parameters to customize anomaly detection
                    Default parameters will be used if not provided
                    
    Returns:
        Dictionary of anomaly results for each parameter
    """
    if flight_data.empty:
        return {}
    
    # Default parameters if not provided
    if parameters is None:
        parameters = {
            'altitude': {
                'z_threshold': 3.0,
                'change_threshold': 2.5,
                'trend_threshold': 2.0
            },
            'velocity': {
                'z_threshold': 3.0,
                'change_threshold': 2.0,
                'trend_threshold': 2.0
            },
            'heading': {
                'z_threshold': 3.0,
                'change_threshold': 2.5
            }
        }
    
    results = {}
    
    # Check for altitude anomalies
    if 'baro_altitude' in flight_data.columns:
        vertical_rate = flight_data['vertical_rate'] if 'vertical_rate' in flight_data.columns else None
        results['altitude'] = detect_altitude_anomalies(
            flight_data['baro_altitude'],
            vertical_rate=vertical_rate,
            **parameters.get('altitude', {})
        )
    
    # Check for speed anomalies
    if 'velocity' in flight_data.columns:
        results['velocity'] = detect_speed_anomalies(
            flight_data['velocity'],
            **parameters.get('velocity', {})
        )
    
    # Check for heading anomalies
    if 'heading' in flight_data.columns:
        results['heading'] = detect_turn_anomalies(
            flight_data['heading'],
            **parameters.get('heading', {})
        )
    
    # Calculate overall anomaly score
    if results:
        # Create a combined anomaly indicator
        any_anomaly = pd.Series(False, index=flight_data.index)
        anomaly_count = pd.Series(0, index=flight_data.index)
        
        for param_results in results.values():
            if 'any_anomaly' in param_results:
                any_anomaly |= param_results['any_anomaly']
                anomaly_count += param_results['any_anomaly'].astype(int)
        
        # Store the combined results
        results['overall'] = {
            'any_anomaly': any_anomaly,
            'anomaly_count': anomaly_count
        }
    
    return results


def get_anomaly_summary(anomaly_results: Dict[str, Dict[str, pd.Series]]) -> Dict:
    """
    Generate a summary of detected anomalies.
    
    Args:
        anomaly_results: Results from analyze_flight_anomalies
        
    Returns:
        Dictionary with anomaly summary statistics
    """
    if not anomaly_results or 'overall' not in anomaly_results:
        return {
            'total_anomalies': 0,
            'anomaly_percentage': 0.0,
            'max_anomaly_count': 0,
            'parameter_breakdown': {}
        }
    
    # Get overall stats
    any_anomaly = anomaly_results['overall']['any_anomaly']
    total_points = len(any_anomaly)
    total_anomalies = any_anomaly.sum()
    anomaly_percentage = (total_anomalies / total_points * 100) if total_points > 0 else 0
    
    # Get breakdown by parameter
    parameter_breakdown = {}
    for param, param_results in anomaly_results.items():
        if param != 'overall' and 'any_anomaly' in param_results:
            param_anomalies = param_results['any_anomaly'].sum()
            param_percentage = (param_anomalies / total_points * 100) if total_points > 0 else 0
            
            # Get breakdown by anomaly type
            type_breakdown = {}
            for anomaly_type, anomaly_series in param_results.items():
                if anomaly_type != 'any_anomaly':
                    type_count = anomaly_series.sum()
                    type_percentage = (type_count / total_points * 100) if total_points > 0 else 0
                    type_breakdown[anomaly_type] = {
                        'count': int(type_count),
                        'percentage': float(type_percentage)
                    }
            
            parameter_breakdown[param] = {
                'total': int(param_anomalies),
                'percentage': float(param_percentage),
                'types': type_breakdown
            }
    
    # Get max anomaly count (how many parameters had anomalies at the same time)
    max_anomaly_count = anomaly_results['overall']['anomaly_count'].max() if 'anomaly_count' in anomaly_results['overall'] else 0
    
    return {
        'total_anomalies': int(total_anomalies),
        'anomaly_percentage': float(anomaly_percentage),
        'max_anomaly_count': int(max_anomaly_count),
        'parameter_breakdown': parameter_breakdown
    }


def get_anomaly_timestamps(
    anomaly_results: Dict[str, Dict[str, pd.Series]], 
    min_anomaly_count: int = 1
) -> List[Dict]:
    """
    Extract timestamps and details of anomalies for visualization.
    
    Args:
        anomaly_results: Results from analyze_flight_anomalies
        min_anomaly_count: Minimum number of simultaneous anomalies to include
        
    Returns:
        List of dictionaries with anomaly details
    """
    if not anomaly_results or 'overall' not in anomaly_results:
        return []
    
    # Get timestamps where any anomaly occurred
    any_anomaly = anomaly_results['overall']['any_anomaly']
    anomaly_count = anomaly_results['overall']['anomaly_count']
    
    # Filter by minimum anomaly count
    significant_anomalies = anomaly_count >= min_anomaly_count
    
    if not any(significant_anomalies):
        return []
    
    # Get the indices where anomalies occurred
    anomaly_indices = significant_anomalies[significant_anomalies].index
    
    # Prepare the result list
    anomaly_events = []
    
    for idx in anomaly_indices:
        # Get the parameters that had anomalies at this timestamp
        param_details = {}
        for param, param_results in anomaly_results.items():
            if param != 'overall':
                param_anomalies = {}
                for anomaly_type, anomaly_series in param_results.items():
                    if anomaly_type != 'any_anomaly' and anomaly_series.loc[idx]:
                        param_anomalies[anomaly_type] = True
                
                if param_anomalies:
                    param_details[param] = param_anomalies
        
        # Add to the result list
        anomaly_events.append({
            'timestamp': idx,
            'anomaly_count': int(anomaly_count.loc[idx]),
            'parameters': param_details
        })
    
    return anomaly_events


def classify_anomaly_severity(
    anomaly_results: Dict[str, Dict[str, pd.Series]]
) -> pd.Series:
    """
    Classify the severity of anomalies.
    
    Args:
        anomaly_results: Results from analyze_flight_anomalies
        
    Returns:
        Series with severity levels (0=none, 1=low, 2=medium, 3=high)
    """
    if not anomaly_results or 'overall' not in anomaly_results:
        return pd.Series()
    
    # Get the anomaly count
    anomaly_count = anomaly_results['overall']['anomaly_count']
    
    # Initialize severity series
    severity = pd.Series(0, index=anomaly_count.index)
    
    # Classify severity based on anomaly count
    severity = severity.mask(anomaly_count == 1, 1)  # Low severity
    severity = severity.mask(anomaly_count == 2, 2)  # Medium severity
    severity = severity.mask(anomaly_count >= 3, 3)  # High severity
    
    # Increase severity for specific dangerous anomalies
    for param, param_results in anomaly_results.items():
        if param == 'altitude' and 'extreme_acceleration' in param_results:
            # Extreme altitude changes are dangerous
            severity = severity.mask(param_results['extreme_acceleration'] & (severity < 3), 3)
        
        if param == 'velocity' and 'extreme_acceleration' in param_results:
            # Extreme speed changes are concerning
            severity = severity.mask(param_results['extreme_acceleration'] & (severity < 2), 2)
    
    return severity


if __name__ == "__main__":
    # Example usage
    import matplotlib.pyplot as plt
    
    # Create sample data
    np.random.seed(42)
    index = pd.date_range('2023-01-01', periods=100, freq='10s')
    altitude = pd.Series(10000 + np.cumsum(np.random.normal(0, 10, 100)), index=index)
    
    # Add some anomalies
    altitude.iloc[30:35] += 500  # Sudden increase
    altitude.iloc[60] -= 1000  # Sudden drop
    altitude.iloc[80:] += np.linspace(0, 800, 20)  # Trend deviation
    
    # Detect anomalies
    anomalies = detect_altitude_anomalies(altitude)
    
    # Plot results
    plt.figure(figsize=(12, 6))
    plt.plot(altitude, label='Altitude')
    
    # Plot anomalies
    for anomaly_type, anomaly_series in anomalies.items():
        if anomaly_type != 'any_anomaly':
            anomaly_points = altitude[anomaly_series]
            plt.scatter(anomaly_points.index, anomaly_points, 
                        label=f'{anomaly_type} ({len(anomaly_points)} points)')
    
    plt.legend()
    plt.title('Altitude Anomaly Detection')
    plt.ylabel('Altitude (m)')
    plt.tight_layout()
    plt.show() 