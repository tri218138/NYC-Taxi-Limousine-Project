from pyhive import hive

# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute('USE nyc_taxi_limousine')
cursor.execute('''
WITH LocationIDsAirportFee AS (
    SELECT PULocationID AS LocationID, Airport_fee
    FROM yellow_tripdata_2022_01
    UNION ALL
    SELECT DOLocationID AS LocationID, Airport_fee
    FROM yellow_tripdata_2022_01
),
LocationIDs AS (
    SELECT DISTINCT LocationID, Airport_fee
    FROM LocationIDsAirportFee
),
-- All locations with fee = 0
ZeroLocationIDs AS (
    SELECT DISTINCT LocationID
    FROM LocationIDs
    WHERE Airport_fee = 0
),
NonZeroAirportFee AS (
   SELECT tb.PULocationID, tb.DOLocationID, tb.Airport_fee
   FROM yellow_tripdata_2022_01 tb
   LEFT SEMI JOIN ZeroLocationIDs z ON (tb.PULocationID = z.LocationID AND tb.DOLocationID = z.LocationID)
   WHERE tb.Airport_fee != 0
),
-- Locations with fee = 1 and at least 0
LocationIDsNonZeroAirportFee AS (
    SELECT PULocationID AS LocationID
    FROM NonZeroAirportFee
    UNION ALL
    SELECT DOLocationID AS LocationID
    FROM NonZeroAirportFee
),
OneLocationIDs AS (
    SELECT DISTINCT LocationID
    FROM LocationIDsNonZeroAirportFee
),
-- Locations with fee = 1 but not necessarily 0
MaybeOneLocationIDs AS (
    SELECT DISTINCT l.LocationID
    FROM LocationIDs l
    LEFT SEMI JOIN ZeroLocationIDs z ON l.LocationID = z.LocationID
    LEFT SEMI JOIN OneLocationIDs o ON l.LocationID = o.LocationID
    WHERE z.LocationID IS NULL AND o.LocationID IS NULL
)
-- Combine all results and label IsAirPort accordingly
SELECT DISTINCT LocationID,
    CASE
        WHEN LocationID IN (SELECT LocationID FROM ZeroLocationIDs) THEN 0
        WHEN LocationID IN (SELECT LocationID FROM OneLocationIDs) THEN 1
        WHEN LocationID IN (SELECT LocationID FROM MaybeOneLocationIDs) THEN -1
    END AS IsAirPort
FROM LocationIDs
''')

# Fetch the result
results = cursor.fetchall()
print(results)
# Print the result
#print("Total revenue by day in January 2022:")
#for row in results:
#    print(f"Date {row[0]}: {row[1]}")

# Close the cursor and connection
cursor.close()
conn.close()
