#!/usr/bin/env python3
"""
Test map using Folium instead of PyDeck/Streamlit's built-in map
"""

import streamlit as st
import folium
from streamlit_folium import folium_static
import pandas as pd

st.title("🌍 Folium Map Test")
st.write("This is a test of Folium maps as an alternative to PyDeck")

# Create a sample dataframe with flight positions
df = pd.DataFrame({
    "lat": [37.76, 37.77, 37.78, 37.79],
    "lon": [-122.4, -122.41, -122.42, -122.43],
    "flight": ["Flight 1", "Flight 2", "Flight 3", "Flight 4"],
    "altitude": [5000, 10000, 7500, 8000]
})

# Create a folium map centered on the data
m = folium.Map(location=[df["lat"].mean(), df["lon"].mean()], zoom_start=10)

# Add markers for each flight
for _, row in df.iterrows():
    folium.Marker(
        location=[row["lat"], row["lon"]],
        popup=f"{row['flight']}: {row['altitude']} ft",
        icon=folium.Icon(icon="plane", prefix="fa"),
    ).add_to(m)

# Display the map in Streamlit
st.write("### Flight Positions Map")
folium_static(m)

# Also try a simple heatmap
st.write("### Heat Map")
from folium.plugins import HeatMap

m2 = folium.Map(location=[df["lat"].mean(), df["lon"].mean()], zoom_start=10)
HeatMap(data=df[["lat", "lon"]].values).add_to(m2)
folium_static(m2) 