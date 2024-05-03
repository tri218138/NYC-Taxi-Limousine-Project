import pandas as pd
from pyhive import hive
from my_enum import table_name


# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute("USE nyc_taxi_limousine")
cursor.execute(
    f"""SELECT
    payment_type,
    COUNT(*) AS payment_type_count,
    100.0 * COUNT(*) / SUM(COUNT(*)) OVER () AS payment_type_percentage
FROM
    {table_name}
GROUP BY
    payment_type""".format(
        table_name=table_name
    )
)

# Fetch the result
result = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

# Now you can use the 'result' variable which contains the result of the query
df = pd.DataFrame(result, columns=["Payment Type", "Count", "Percentage"])
print(df)
