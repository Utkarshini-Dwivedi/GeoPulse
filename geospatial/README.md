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

Snowflake GPS_DATA
        ↓
     PySpark
        ↓
 Apache Sedona
        ↓
 GPS Spatial Points
        ↓
 Store Spatial Points
        ↓
 500m Catchment Areas
        ↓
 Spatial Join
        ↓
 Device → Store Mapping
        ↓
 Snowflake

## Planned Output

The processed spatial dataset will contain GPS records
associated with the calculated store catchment area.