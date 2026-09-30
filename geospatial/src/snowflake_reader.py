import os

import snowflake.connector
from dotenv import load_dotenv


load_dotenv()


def get_connection():
    """Create and return a Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

def validate_gps_schema(cursor):
    """Validate the structure of the GPS_DATA table."""

    cursor.execute("""
        SELECT *
        FROM GEOPULSE.RAW.GPS_DATA
        LIMIT 1
    """)

    columns = [column[0] for column in cursor.description]

    expected_columns = [
        "DEVICE_ID",
        "LATITUDE",
        "LONGITUDE",
        "TIMESTAMP",
        "STORE",
        "LOCATION"
    ]

    print("\nGPS_DATA columns:")
    for column in columns:
        print(f" - {column}")

    missing_columns = [
        column for column in expected_columns
        if column not in columns
    ]

    if missing_columns:
        print("\nMissing columns:", missing_columns)
        return False

    print("\nGPS_DATA schema validation: PASSED")
    return True

def validate_location_geometry(cursor):
    """Validate the LOCATION geography column."""

    cursor.execute("""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(LOCATION) AS location_rows,
            COUNT_IF(ST_ISVALID(LOCATION)) AS valid_locations
        FROM GEOPULSE.RAW.GPS_DATA
    """)

    total_rows, location_rows, valid_locations = cursor.fetchone()

    print("\nLOCATION geometry validation:")
    print(" - Total rows:", total_rows)
    print(" - Non-null LOCATION:", location_rows)
    print(" - Valid LOCATION:", valid_locations)

    if total_rows == location_rows == valid_locations:
        print("LOCATION geometry validation: PASSED")
        return True

    print("LOCATION geometry validation: FAILED")
    return False

def test_connection():
    """Test Snowflake connection and inspect GPS_DATA."""
    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()")
        print("Snowflake location:", cursor.fetchone())

        cursor.execute("""
            SELECT COUNT(*)
            FROM GEOPULSE.RAW.GPS_DATA
        """)
        print("GPS_DATA row count:", cursor.fetchone()[0])

        validate_gps_schema(cursor)

        validate_location_geometry(cursor)

        cursor.execute("""
            SELECT *
            FROM GEOPULSE.RAW.GPS_DATA
            LIMIT 5
        """)

        print("\nSample GPS records:")
        for row in cursor.fetchall():
            print(row)

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    test_connection()