from pyhive import hive
from datetime import datetime, timedelta

table_name = "yellow_tripdata_2022_01"

# Connect to Hive
conn = hive.Connection(host="localhost", port=10000)

# Create a cursor
cursor = conn.cursor()

# Execute the Hive query
cursor.execute('USE nyc_taxi_limousine')
cursor.execute("""SELECT tpep_pickup_datetime, tpep_dropoff_datetime 
FROM {table_name}
WHERE 
    TO_DATE(FROM_UNIXTIME(CAST(tpep_pickup_datetime / 1000000 AS BIGINT))) = '2022-01-01' 
    AND TO_DATE(FROM_UNIXTIME(CAST(tpep_dropoff_datetime / 1000000 AS BIGINT))) = '2022-01-01'""".format(table_name=table_name))

# Fetch the result
result = cursor.fetchall()

# Close the cursor and connection
cursor.close()
conn.close()

# print(result[:10])
def calculate_overlap(records):
    # Convert Unix timestamps to datetime objects
    records = [{'start_time': datetime.utcfromtimestamp(int(record[0])//1000000), 'end_time': datetime.utcfromtimestamp(int(record[1])/1000000)} for record in records]
    
    # Create a list of timestamps
    timestamps = []
    for record in records:
        timestamps.append((record['start_time'], 'start'))
        timestamps.append((record['end_time'], 'end'))

    # Sort the timestamps by time
    timestamps.sort()

    overlap_count = {}
    prev_time, event_type = timestamps[0]
    current_counter = 1 if event_type == 'start' else 0
    for timestamp in timestamps[1:]:
        time, event_type = timestamp
        # process all event before this timestamp
        if time != prev_time:
            overlap_count[(prev_time, time)] = current_counter

        if event_type == 'start':
            current_counter += 1
        elif event_type == 'end':
            current_counter -= 1
        
        if time != prev_time:
            prev_time = time

    assert current_counter == 0, "Because there is no any car is running"
    return overlap_count


# Calculate overlap
overlap_count = calculate_overlap(result)

# Tìm số lượng trùng nhau lớn nhất
max_overlap = max(overlap_count.values())

print("Khoảng thời gian có số lượng trùng nhau lớn nhất là:")
for (start, end), count in overlap_count.items():
    if count == max_overlap:
        print(f"Từ {start} đến {end}, với số lượng trùng nhau là {count}.")

print('='*20)
print("Addition information:")
_start = None
for (start, end), count in overlap_count.items():
	if _start is not None:
		if start < _start + timedelta(minutes=30):
			continue
	_start = start
	print(f"From {start} to {end}, number of overlap is {count}")
