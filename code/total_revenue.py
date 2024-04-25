import pandas as pd
from pyhive import hive

# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute('USE nyc_taxi_limousine')
cursor.execute('''
    SELECT TO_DATE(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000000 AS BIGINT))) AS pickup_date,
           SUM(total_amount) AS total_revenue
    FROM yellow_tripdata_2022_01
    GROUP BY TO_DATE(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000000 AS BIGINT)))
    ORDER BY pickup_date
''')

# Fetch the result
results = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

df = pd.DataFrame(results, columns=['Pickup Date', 'Total Revenue'])
print(df)
