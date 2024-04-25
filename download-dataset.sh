#!/bin/bash

# Get the current directory
current_dir=$(pwd)

# Define the kinds of data
kinds=("yellow" "green" "fhv" "fhvhv")

# Define the range of years and months
years=( {2022..2023} )
months=( {01..12} )

echo "Files is downloading to $current_dir/dataset"

# Loop over each kind, year, and month combination
for year in "${years[@]}"; do
    for month in "${months[@]}"; do
        for kind in "${kinds[@]}"; do
            # Construct the URL
            url="https://d37ci6vzurychx.cloudfront.net/trip-data/${kind}_tripdata_${year}-${month}.parquet"
            
            # Construct the file name
            file_name="${kind}_tripdata_${year}-${month}.parquet"
            
            # Check if the file already exists in the current directory
            if [ ! -f "$current_dir/dataset/$file_name" ]; then
                # Download the file using wget
                wget -P "$current_dir/dataset" "$url"
            else
                echo "File '$file_name' already exists"
            fi
        done
    done
done

