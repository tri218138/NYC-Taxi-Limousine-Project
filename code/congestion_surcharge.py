import pandas as pd
from pyhive import hive

table_name = "yellow_tripdata_2022_01"
bin_size = 5

# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute('USE nyc_taxi_limousine')
cursor.execute("""
SELECT
    PULocationID,
    DOLocationID,
    COUNT(*) AS num_trips,
    SUM(Congestion_surcharge) AS total_congestion_surcharge
FROM
    {table_name}
WHERE
    Congestion_surcharge > 0
GROUP BY
    PULocationID,
    DOLocationID
ORDER BY
    total_congestion_surcharge DESC""".format(table_name=table_name))

# Fetch the result
result = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

df = pd.DataFrame(result, columns=["PULocationID", "DOLocationID", "Count", "TotalCongestionSurcharge"])
print(df)
