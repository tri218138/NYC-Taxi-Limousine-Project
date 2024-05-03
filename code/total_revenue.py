from my_enum import table_name
import pandas as pd
from pyhive import hive


def hive_connection():
    # Connect to Hive
    conn = hive.Connection(host="localhost", port=10000)

    # Create a cursor
    cursor = conn.cursor()

    return cursor, conn


def execute_commands(cursor, commands: list = None):
    # Execute the Hive query
    cursor.execute("USE nyc_taxi_limousine")
    cursor.execute(
        f"""
        SELECT TO_DATE(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000000 AS BIGINT))) AS pickup_date,
            SUM(total_amount) AS total_revenue
        FROM {table_name}
        GROUP BY TO_DATE(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000000 AS BIGINT)))
        ORDER BY pickup_date
    """
    )
    results = cursor.fetchall()
    return results


def visualize_result(results):
    df = pd.DataFrame(results, columns=["Pickup Date", "Total Revenue"])
    return df


def main():
    cursor, conn = hive_connection()
    results = execute_commands(cursor)
    cursor.close()
    conn.close()
    print(visualize_result(results))


if __name__ == "__main__":
    main()
