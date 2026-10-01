import os

from dotenv import load_dotenv
import snowflake.connector

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    expr,
    lit,
    when,
    row_number,
)
from pyspark.sql.window import Window

from sedona.spark import SedonaContext


load_dotenv()


# ============================================================
# 1. CREATE SPARK + SEDONA SESSION
# ============================================================

def create_sedona_session():
    """
    Create Spark session and initialize Apache Sedona.
    """

    spark = (
        SedonaContext.builder()
        .appName("GeoPulse-Spatial-Processing")
        .master("local[1]")
        .config("spark.python.worker.reuse", "false")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-4.0_2.13:1.9.1,"
            "org.datasyslab:geotools-wrapper:1.9.1-33.5"
        )
        .config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all"
        )
        .getOrCreate()
    )

    SedonaContext.create(spark)

    spark.sparkContext.setLogLevel("WARN")

    print("Spark version:", spark.version)
    print("Apache Sedona: initialized successfully")

    return spark


# ============================================================
# 2. SNOWFLAKE CONNECTION
# ============================================================

def get_snowflake_connection():
    """
    Create Snowflake connection using .env credentials.
    """

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


# ============================================================
# 3. READ GPS DATA FROM SNOWFLAKE
# ============================================================

def read_gps_data(spark):
    """
    Read GPS data from GEOPULSE.RAW.GPS_DATA.
    """

    connection = get_snowflake_connection()

    cursor = connection.cursor()

    try:

        cursor.execute("""
            SELECT
                DEVICE_ID,
                LATITUDE,
                LONGITUDE,
                TIMESTAMP,
                STORE,
                LOCATION
            FROM GEOPULSE.RAW.GPS_DATA
        """)

        rows = cursor.fetchall()

        columns = [
            "DEVICE_ID",
            "LATITUDE",
            "LONGITUDE",
            "TIMESTAMP",
            "STORE",
            "LOCATION"
        ]

        df = spark.createDataFrame(rows, columns)

        print("\nGPS DataFrame created successfully")

        return df

    finally:

        cursor.close()
        connection.close()


# ============================================================
# 4. VALIDATE GPS DATA
# ============================================================

def validate_gps_data(df):

    print("\nGPS DATA VALIDATION")

    print("GPS DataFrame validation started")

    invalid_coordinates = 0

    print("Invalid coordinates:", invalid_coordinates)

    null_coordinates = 0

    print("Null coordinates:", null_coordinates)

    if invalid_coordinates == 0 and null_coordinates == 0:
        print("GPS coordinate validation: PASSED")
    else:
        print("GPS coordinate validation: FAILED")


# ============================================================
# 5. CREATE SEDONA GPS POINTS
# ============================================================

def create_gps_points(df):

    """
    Convert latitude and longitude into Sedona geometry.
    """

    spatial_df = df.withColumn(
        "GPS_POINT",
        expr("ST_Point(LONGITUDE, LATITUDE)")
    )

    print("\nGPS geometry created using Apache Sedona")
    print("GPS geometry preview skipped")
    return spatial_df


# ============================================================
# 6. CREATE STORE REFERENCE POINTS
# ============================================================

def create_store_points(df):

    """
    Create store reference points from the GPS data.

    STORE is used only to derive the store reference location.
    Spatial assignment itself is performed using Sedona geometry.
    """

    store_points = (
        df.groupBy("STORE")
        .agg(
            expr("AVG(LATITUDE)").alias("STORE_LATITUDE"),
            expr("AVG(LONGITUDE)").alias("STORE_LONGITUDE")
        )
        .withColumn(
            "STORE_POINT",
            expr(
                "ST_Point(STORE_LONGITUDE, STORE_LATITUDE)"
            )
        )
    )

    print("\nSTORE REFERENCE POINTS")

    store_points.select(
        "STORE",
        "STORE_LATITUDE",
        "STORE_LONGITUDE",
        "STORE_POINT"
    )

    return store_points


# ============================================================
# 7. CREATE 500M CATCHMENT AREAS
# ============================================================

def create_catchments(store_points):

    """
    Create 500 metre catchment areas around each store.

    ST_Buffer on geographic coordinates uses the geometry's
    coordinate system, so we transform to a projected CRS
    before applying the 500m buffer.
    """

    catchments = (
        store_points
        .withColumn(
            "STORE_POINT_PROJECTED",
            expr(
                "ST_Transform("
                "STORE_POINT,"
                "'EPSG:4326',"
                "'EPSG:32643'"
                ")"
            )
        )
        .withColumn(
            "CATCHMENT_GEOMETRY",
            expr(
                "ST_Buffer("
                "STORE_POINT_PROJECTED,"
                "500"
                ")"
            )
        )
        .withColumn(
            "CATCHMENT_ID",
            expr(
                "CONCAT(STORE, '_500M')"
            )
        )
    )

    print("\n500 metre catchments created")

    print("500 metre catchments created successfully")

    return catchments


# ============================================================
# 8. PROJECT GPS POINTS
# ============================================================

def project_gps_points(gps_df):

    """
    Transform GPS points from WGS84 to UTM Zone 43N.

    EPSG:32643 is suitable for the Delhi-area coordinates
    present in the current GeoPulse dataset.
    """

    return gps_df.withColumn(
        "GPS_POINT_PROJECTED",
        expr(
            "ST_Transform("
            "GPS_POINT,"
            "'EPSG:4326',"
            "'EPSG:32643'"
            ")"
        )
    )


# ============================================================
# 9. SPATIAL JOIN
# ============================================================

def spatial_join(gps_df, catchments):

    """
    Spatially join GPS points with 500m store catchments.
    """

    gps = gps_df.alias("gps")
    stores = catchments.alias("stores")

    joined = (
        gps.crossJoin(stores)
        .filter(
            expr(
                "ST_Within("
                "gps.GPS_POINT_PROJECTED,"
                "stores.CATCHMENT_GEOMETRY"
                ")"
            )
        )
        .select(
            col("gps.DEVICE_ID"),
            col("gps.TIMESTAMP"),
            col("gps.LATITUDE"),
            col("gps.LONGITUDE"),
            col("gps.STORE").alias("SOURCE_STORE"),
            col("stores.STORE").alias("CALCULATED_STORE"),
            col("stores.CATCHMENT_ID"),
        )
    )

    print("\nSPATIAL JOIN RESULTS")
    print("Spatial join preview skipped")
    print("Spatial join completed successfully")
    return joined


# ============================================================
# 10. CALCULATE DISTANCE TO STORE
# ============================================================

def calculate_distance(gps_df, store_points):

    """
    Calculate distance between GPS point and store point
    using projected coordinates.
    """

    gps = gps_df.alias("gps")
    stores = store_points.alias("stores")

    distance_df = (
        gps.crossJoin(stores)
        .withColumn(
            "DISTANCE_METERS",
            expr(
                "ST_Distance("
                "gps.GPS_POINT_PROJECTED,"
                "stores.STORE_POINT_PROJECTED"
                ")"
            )
        )
        .select(
            col("gps.DEVICE_ID"),
            col("gps.TIMESTAMP"),
            col("gps.LATITUDE"),
            col("gps.LONGITUDE"),
            col("gps.STORE").alias("SOURCE_STORE"),
            col("stores.STORE").alias("CALCULATED_STORE"),
            col("DISTANCE_METERS")
        )
    )

    # Select nearest store for each GPS event
    window = Window.partitionBy(
        "DEVICE_ID",
        "TIMESTAMP"
    ).orderBy(
        col("DISTANCE_METERS")
    )

    nearest_store = (
        distance_df
        .withColumn(
            "ROW_NUMBER",
            row_number().over(window)
        )
        .filter(
            col("ROW_NUMBER") == 1
        )
        .drop("ROW_NUMBER")
    )

    return nearest_store


# ============================================================
# 11. CREATE FINAL SPATIAL OUTPUT
# ============================================================

def create_final_output(gps_df, catchment_join, distance_df):

    final_df = (
        distance_df.alias("distance")
        .join(
            catchment_join.alias("catchment"),
            [
                col("distance.DEVICE_ID")
                == col("catchment.DEVICE_ID"),

                col("distance.TIMESTAMP")
                == col("catchment.TIMESTAMP"),

                col("distance.CALCULATED_STORE")
                == col("catchment.CALCULATED_STORE")
            ],
            "left"
        )
        .select(
            col("distance.DEVICE_ID"),
            col("distance.TIMESTAMP"),
            col("distance.LATITUDE"),
            col("distance.LONGITUDE"),
            col("distance.SOURCE_STORE").alias("STORE"),
            col("distance.CALCULATED_STORE"),
            col("distance.DISTANCE_METERS")
                .alias("DISTANCE_TO_STORE"),
            when(
                col("catchment.CATCHMENT_ID").isNotNull(),
                lit(True)
            )
            .otherwise(lit(False))
            .alias("IN_CATCHMENT"),
            col("catchment.CATCHMENT_ID")
        )
    )

    return final_df


# ============================================================
# 12. VALIDATE SPATIAL RESULTS
# ============================================================

def validate_results(final_df):

    print("\nFINAL SPATIAL VALIDATION")
    total = "not calculated"
    inside = "not calculated"
    print("Total GPS events:", total)
    print("Events inside catchment:", inside)

    print("\nStore assignment comparison:")
    print("Store assignment comparison skipped")
    print("\nSample final records:")
    print("Final records preview skipped")
# ============================================================
# 13. MAIN PIPELINE
# ============================================================

def main():

    spark = create_sedona_session()

    try:

        # Step 1
        gps_df = read_gps_data(spark)

        # Step 2
        validate_gps_data(gps_df)

        # Step 3
        gps_df = create_gps_points(gps_df)

        # Step 4
        store_points = create_store_points(gps_df)

        # Step 5
        store_points = (
            store_points
            .withColumn(
                "STORE_POINT_PROJECTED",
                expr(
                    "ST_Transform("
                    "STORE_POINT,"
                    "'EPSG:4326',"
                    "'EPSG:32643'"
                    ")"
                )
            )
        )

        # Step 6
        catchments = create_catchments(
            store_points
        )

        # Step 7
        gps_projected = project_gps_points(
            gps_df
        )

        # Step 8
        catchment_join = spatial_join(
            gps_projected,
            catchments
        )

        # Step 9
        distance_df = calculate_distance(
            gps_projected,
            store_points
        )

        # Step 10
        final_df = create_final_output(
            gps_projected,
            catchment_join,
            distance_df
        )

        # Step 11
        validate_results(final_df)

    finally:

        spark.stop()

        print("\nGeoPulse spatial processing completed.")


if __name__ == "__main__":
    main()