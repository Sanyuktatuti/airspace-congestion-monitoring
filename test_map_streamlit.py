import streamlit as st
import pandas as pd

# Configure page
st.set_page_config(
    page_title="Streamlit Map Test",
    page_icon="🗺️",
    layout="wide"
)

# Test data
test_data = {
    'latitude': [40.7128, 51.5074, 48.8566, 35.6762],
    'longitude': [-74.0060, -0.1278, 2.3522, 139.6503],
    'name': ['New York', 'London', 'Paris', 'Tokyo']
}

df = pd.DataFrame(test_data)

st.title("Map Test using Streamlit")
st.write("Testing basic map rendering with Streamlit's built-in map")

# Display the map using Streamlit's built-in map
st.map(df, zoom=1)

# Display data table for verification
st.subheader("Data Points")
st.dataframe(df) 