# GeoPulse

GeoPulse is a geospatial analytics project that uses synthetic GPS movement data to analyze customer movement patterns, store footfall, store overlap, and potential store cannibalization.

## Project Objective

The project aims to understand:

- Where people move
- When people visit stores
- Store-wise footfall
- Unique visitors
- Shared visitors between stores
- Potential cannibalization between nearby stores

## Data

The project uses synthetic GPS data containing:

- Device ID
- Latitude
- Longitude
- Timestamp
- Store

No real user location data is used.

## Technology Stack

- Python
- Snowflake
- PySpark
- Apache Sedona
- SQL
- dbt
- React
- Kepler.gl
- H3
- Apache Airflow

## Current Data

The current synthetic dataset contains approximately 2,000 GPS records generated for multiple devices across multiple days.

## Project Structure

```text
GeoPulse/
├── README.md
├── data/
│   └── raw/
│       └── synthetic_gps_data.csv
└── scripts/
    ├── generate_gps.py
    └── validate_gps_data.py
    