import pandas as pd
from pyhive import hive

table_name = "yellow_tripdata_2022_01"
bin_size = 5
from my_enum import table_name

# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute("USE nyc_taxi_limousine")
cursor.execute("SET hivevar:bin_size={bin_size}".format(bin_size=bin_size))
cursor.execute(
    """
SELECT
    CONCAT(CAST(FLOOR(total_amount / ${{hivevar:bin_size}}) * ${{hivevar:bin_size}} AS STRING), '-', CAST(FLOOR(total_amount / ${{hivevar:bin_size}}) * ${{hivevar:bin_size}} + ${{hivevar:bin_size}} AS STRING)) AS payment_bin,
    COUNT(*) AS count_payments
FROM
    {table_name}
GROUP BY
    CONCAT(CAST(FLOOR(total_amount / ${{hivevar:bin_size}}) * ${{hivevar:bin_size}} AS STRING), '-', CAST(FLOOR(total_amount / ${{hivevar:bin_size}}) * ${{hivevar:bin_size}} + ${{hivevar:bin_size}} AS STRING))
ORDER BY
    LENGTH(payment_bin), payment_bin""".format(
        table_name=table_name
    )
)

# Fetch the result
result = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

df = pd.DataFrame(result, columns=["Bin", "Count"])
print(df)
