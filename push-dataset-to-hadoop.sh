#!/bin/bash

# Get the current directory
current_dir=$(pwd)

# Define the kinds of data
#kinds=("yellow" "green" "fhv" "fhvhv")
kinds=("yellow")

# Define the range of years and months
years=( {2022..2022} )
months=( {01..02} )

echo "Files are pushing to hdfs://localhost:9000"
hadoop fs -mkdir hdfs://localhost:9000/NYC-Taxi-Limousine-Project

# Loop over each kind, year, and month combination
for year in "${years[@]}"; do
    for month in "${months[@]}"; do
        for kind in "${kinds[@]}"; do
            
            # Construct the file name
            file_name="${kind}_tripdata_${year}-${month}.parquet"
            
            # Check if the file already exists in the current directory
            if [ ! -f "$current_dir/dataset/$file_name" ]; then
	        # Construct the URL
	        url="https://d37ci6vzurychx.cloudfront.net/trip-data/${kind}_tripdata_${year}-${month}.parquet"
	    
                # Download the file using wget
                wget -P "$current_dir/dataset" "$url"
            fi
            
            # Push the file to Hadoop
            hadoop fs -put "$current_dir/dataset/$file_name" "hdfs://localhost:9000/NYC-Taxi-Limousine-Project/$file_name"
            echo "File '$file_name' is pushed"
            break
        done
    done
done

