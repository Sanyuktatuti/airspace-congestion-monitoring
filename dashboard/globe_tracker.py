# dashboard/globe_tracker.py
import streamlit as st
import pandas as pd
import json
import os
import pydeck as pdk

# 1) Figure out the path *streamlit* will run under
HERE = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(HERE, os.pardir, "data", "test_samples.jsonl")

st.title("✈️ Aircraft Movement Tracker")

st.write(f"Loading `{DATA_PATH}` …")
# 2) Load into a DataFrame
records = []
with open(DATA_PATH, "r") as f:
    for line in f:
        obj = json.loads(line)
        # obj["value"] is your struct
        v = obj["value"]
        # and we only need icao24, latitude, longitude for now
        records.append({
            "icao24": v["icao24"],
            "lat": v["latitude"],
            "lon": v["longitude"],
        })

df = pd.DataFrame(records)
st.write(f"Loaded {len(df):,} records")

# 3) Pick top 10 unique aircraft
top10 = df.drop_duplicates("icao24").head(10)
st.write("### 🔢 Top 10 unique aircraft")
st.dataframe(top10)

# 4) Try the built-in Streamlit map
st.write("### 🗺️ Quick smoke-test with `st.map`")
st.map(top10)

# 5) If that works, hop into PyDeck
st.write("### 🛰️ PyDeck Globe with ScatterplotLayer")
view = pdk.ViewState(
    latitude=top10["lat"].mean(),
    longitude=top10["lon"].mean(),
    zoom=2,
    pitch=30
)
layer = pdk.Layer(
    "ScatterplotLayer",
    data=top10,
    get_position=["lon", "lat"],
    get_fill_color=[255, 140, 0, 180],
    get_radius=50000,
    pickable=True,
)
deck = pdk.Deck(layers=[layer], initial_view_state=view)
st.pydeck_chart(deck)