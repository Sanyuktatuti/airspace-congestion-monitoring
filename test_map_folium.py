import streamlit as st
import folium
from streamlit_folium import st_folium
import pandas as pd

# Configure page
st.set_page_config(
    page_title="Folium Map Test",
    page_icon="🗺️",
    layout="wide"
)

# Print debug info
st.write("Browser Info:", st.runtime.get_instance().get_browser_info())
st.write("Streamlit Version:", st.__version__)

# Test data
test_data = {
    'latitude': [40.7128, 51.5074, 48.8566, 35.6762],
    'longitude': [-74.0060, -0.1278, 2.3522, 139.6503],
    'name': ['New York', 'London', 'Paris', 'Tokyo']
}

df = pd.DataFrame(test_data)

st.title("Map Test using Folium")
st.write("Testing basic map rendering with Folium")

# Create a base map centered at the mean of our data points
m = folium.Map(
    location=[df['latitude'].mean(), df['longitude'].mean()],
    zoom_start=2,
    tiles='OpenStreetMap',
    width=800,
    height=600
)

# Add markers for each city
for idx, row in df.iterrows():
    folium.Marker(
        [row['latitude'], row['longitude']],
        popup=row['name']
    ).add_to(m)

# Display the map with explicit width and height
st_folium(m, width=800, height=600)

# Display data table for verification
st.subheader("Data Points")
st.dataframe(df)

# Add some troubleshooting info
st.markdown("""
### Troubleshooting Tips:
1. Try refreshing the page
2. Check if JavaScript is enabled in your browser
3. Try a different browser (Chrome or Firefox recommended)
4. Clear your browser cache
""") 