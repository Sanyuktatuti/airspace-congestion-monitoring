import streamlit as st
import json
import pydeck as pdk

# Load your JSONL file
def load_data(file_path):
    aircraft_data = []
    with open(file_path, 'r') as f:
        for line in f:
            # Parse each line of JSONL
            data = json.loads(line)
            aircraft_data.append(data['value'])
    return aircraft_data

# Process the aircraft data to get the top 10 unique aircraft
def get_top_10_aircraft(aircraft_data):
    unique_aircraft = {}
    for entry in aircraft_data:
        key = entry['icao24']
        if key not in unique_aircraft:
            unique_aircraft[key] = entry
    return list(unique_aircraft.values())[:10]  # Top 10 unique aircraft

# Set up the initial view for the globe based on the aircraft's location
def get_view_state(aircraft_data):
    latitudes = [point["latitude"] for point in aircraft_data]
    longitudes = [point["longitude"] for point in aircraft_data]
    return pdk.ViewState(
        latitude=sum(latitudes) / len(latitudes),
        longitude=sum(longitudes) / len(longitudes),
        zoom=3,
        pitch=45
    )

# Create the scatterplot layer for aircraft on the globe
def create_aircraft_layer(aircraft_data):
    return pdk.Layer(
        "ScatterplotLayer",
        data=aircraft_data,
        get_position="[longitude, latitude]",
        get_fill_color="[255, 140, 0, 160]",
        get_radius=30000,
        pickable=True
    )

# Streamlit app
def main():
    st.title('Aircraft Movement Tracker')
    
    # Path to your JSONL file
    file_path = 'data/test_samples.jsonl'
    
    # Load and process the data
    aircraft_data = load_data(file_path)
    top_10_aircraft = get_top_10_aircraft(aircraft_data)

    # Create the view state for the globe
    view_state = get_view_state(top_10_aircraft)

    # Create the layer for the aircraft
    aircraft_layer = create_aircraft_layer(top_10_aircraft)

    # Create the deck and render the globe
    deck = pdk.Deck(
        layers=[aircraft_layer],
        initial_view_state=view_state,
        map_style="mapbox://styles/mapbox/satellite-v9"
    )
    
    st.pydeck_chart(deck)

if __name__ == '__main__':
    main()