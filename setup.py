from setuptools import setup, find_packages

setup(
    name="airspace-congestion-monitoring",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'streamlit',
        'pydeck',
        'kafka-python',
        'influxdb-client',
        'matplotlib',
        'plotly',
        'folium',
        'streamlit-folium',
        'python-dotenv'
    ],
) 