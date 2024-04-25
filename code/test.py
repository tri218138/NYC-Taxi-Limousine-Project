from pyhive import hive

# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute('USE nyc_taxi_limousine')
cursor.execute("""WITH t1 AS (SELECT 1 AS col),
     t2 AS (SELECT 2),
     t3 AS (SELECT 3)
SELECT col FROM t1
WHERE col=1""")

# Fetch the result
result = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

# Now you can use the 'result' variable which contains the result of the query
print(result)
