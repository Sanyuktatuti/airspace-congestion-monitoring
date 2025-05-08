import streamlit as st
import pydeck as pdk
import pandas as pd

# Configure page
st.set_page_config(
    page_title="Basic Map Test",
    page_icon="🗺️",
    layout="wide"
)

# Test data - some sample points in different locations
test_data = {
    'latitude': [40.7128, 51.5074, 48.8566, 35.6762],
    'longitude': [-74.0060, -0.1278, 2.3522, 139.6503],
    'name': ['New York', 'London', 'Paris', 'Tokyo']
}

# Create DataFrame
df = pd.DataFrame(test_data)

st.title("Basic Map Test")
st.write("Testing basic map rendering")

# Create simple scatter plot layer
layer = pdk.Layer(
    "ScatterplotLayer",
    data=df,
    get_position=["longitude", "latitude"],
    get_radius=50000,
    pickable=True,
    opacity=0.8,
    filled=True,
    radius_scale=6,
    radius_min_pixels=5,
    radius_max_pixels=100,
    line_width_min_pixels=1
)

# Set the viewport location
view_state = pdk.ViewState(
    latitude=40,
    longitude=0,
    zoom=1,
    pitch=0
)

# Create deck with minimal configuration
deck = pdk.Deck(
    map_style="light",  # Using the basic light style
    initial_view_state=view_state,
    layers=[layer],
    tooltip={"text": "{name}"}
)

# Display the map
st.pydeck_chart(deck)

# Display data table for verification
st.subheader("Data Points")
st.dataframe(df) 