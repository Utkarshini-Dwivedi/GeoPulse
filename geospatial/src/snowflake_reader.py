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