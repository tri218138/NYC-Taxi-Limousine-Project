import pandas as pd
from pyhive import hive

table_name = "yellow_tripdata_2022_01"
from my_enum import table_name

# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute("USE nyc_taxi_limousine")
cursor.execute(
    """SELECT
    VendorID,
    COUNT(*) AS num_trips,
    100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS market_share_percentage
FROM
    {table_name}
GROUP BY
    VendorID""".format(
        table_name=table_name
    )
)

# Fetch the result
result = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

# Convert list of tuples to DataFrame
df = pd.DataFrame(result, columns=["VendorID", "Count", "Percentage"])

# Print the DataFrame
print(df)
