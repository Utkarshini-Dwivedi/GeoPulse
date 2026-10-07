# GeoPulse Geospatial Processing

This module performs the spatial transformation stage of the GeoPulse
retail mobility analytics pipeline.

## Input

GPS mobility data is stored in Snowflake:

- Database: GEOPULSE
- Schema: RAW
- Table: GPS_DATA

## Technologies

- PySpark
- Apache Sedona
- Snowflake
- Python

## Processing Pipeline

```text
Snowflake RAW.GPS_DATA
        ↓
GPS Coordinate Validation
        ↓
GPS Spatial Points
        ↓
Store Reference Points
        ↓
500m Catchment Areas
        ↓
Spatial Join
        ↓
Calculated Store Assignment
        ↓
Nearest Store & Distance Calculation
        ↓
Spatial Validation
        ↓
Snowflake SPATIAL.GPS_STORE_MAPPING