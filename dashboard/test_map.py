# dashboard/test_map.py

import streamlit as st
import pandas as pd

st.title("🚦 Streamlit Map Smoke-Test")
st.write("If you see TWO red dots on this mini-map, Leaflet is working!")

# two hard-coded points (San Francisco area)
df = pd.DataFrame({
    "lat": [37.76, 37.77],
    "lon": [-122.4, -122.41],
})

st.map(df, zoom=10, use_container_width=True)
