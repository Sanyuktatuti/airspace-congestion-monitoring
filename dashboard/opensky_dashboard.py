import streamlit as st
import pandas as pd
import pydeck as pdk
import json

st.set_page_config(page_title="OpenSky JSONL Viewer", layout="wide")
st.title("🛩️ OpenSky JSONL Aircraft Tracker")

# Path to your file
file_path = "data/test_samples.jsonl"

try:
    # Read each line as a JSON object and extract the "value" dictionary
    data = None
    with open(file_path, 'r') as f:
        data = [json.loads(line)["value"] for line in f]

    df = pd.DataFrame(data)

    if df.empty:
        st.warning("No aircraft data found.")
    else:
        # Filter out rows without coordinates
        df = df.dropna(subset=["longitude", "latitude"])

        # Display data table
        st.subheader("📋 Aircraft Data")
        st.dataframe(df[[
            "icao24", "callsign", "origin_country",
            "longitude", "latitude", "velocity", "geo_altitude"
        ]])

        # Create pydeck map
        st.subheader("🗺️ Aircraft Positions")

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position="[longitude, latitude]",
            get_fill_color="[200, 30, 0, 160]",
            get_radius=3000,
            pickable=True
        )

        view_state = pdk.ViewState(
            latitude=df["latitude"].mean(),
            longitude=df["longitude"].mean(),
            zoom=4,
            pitch=0
        )

        st.pydeck_chart(pdk.Deck(
            map_style="mapbox://styles/mapbox/light-v9",
            initial_view_state=view_state,
            layers=[layer],
            tooltip={"text": "Callsign: {callsign}\nCountry: {origin_country}\nAltitude: {geo_altitude} m"}
        ))

except FileNotFoundError:
    st.error(f"File not found: {file_path}")
except json.JSONDecodeError as e:
    st.error(f"Invalid JSON in file: {e}")