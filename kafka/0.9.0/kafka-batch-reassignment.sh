#!/bin/bash

# Ensure that the correct number of arguments are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <input_file> <partitions_per_batch> [output_directory]"
    exit 1
fi

# Input file, number of partitions per batch, and output directory
input_file="$1"
partitions_per_batch="$2"
output_dir="${3:-reassignment_batches}"  # Default to "reassignment_batches" if no directory is specified

# Validate if the input file exists
if [ ! -f "$input_file" ]; then
    echo "Error: Input file '$input_file' does not exist."
    exit 1
fi

# Check if the output directory exists and is not empty
if [ -d "$output_dir" ] && [ "$(ls -A "$output_dir")" ]; then
    echo "Error: Output directory '$output_dir' is not empty. Please ensure the directory is empty before proceeding."
    exit 1
fi

# Read the JSON content from the input file and extract the "Proposed partition reassignment configuration"
json_data=$(awk '/Proposed partition reassignment configuration/{f=1} f' "$input_file" | sed '1d' | tr -d '\r')

# Validate if json_data is not empty
if [ -z "$json_data" ]; then
    echo "Error: Failed to extract JSON data from '$input_file'."
    exit 1
fi

# Extract the "partitions" array using jq
partitions=$(echo "$json_data" | jq '.partitions')

# Check if jq successfully parsed the partitions array
if [ $? -ne 0 ]; then
    echo "Error: Failed to parse partitions in JSON data."
    exit 1
fi

# Count the total number of partitions
total_partitions=$(echo "$partitions" | jq 'length')

# Create the output directory (if it doesn't already exist)
mkdir -p "$output_dir"

# Function to create batch files
create_batch_file() {
    local start=$1
    local end=$2
    local batch_number=$3

    # Slice the partitions array into a batch using jq
    batch_partitions=$(echo "$partitions" | jq ".[$start:$end]")

    # Create the batch JSON with the correct structure
    batch_json=$(echo "$json_data" | jq --argjson partitions "$batch_partitions" '.partitions = $partitions')

    # Write to a new JSON file in the specified output directory
    echo "$batch_json" > "$output_dir/reassignment_batch_${batch_number}.json"
}

# Process partitions in batches
batch_number=1
for ((i=0; i<total_partitions; i+=partitions_per_batch)); do
    # Set start and end indices for the batch
    start=$i
    end=$((i+partitions_per_batch))

    # Ensure the end does not exceed the total partitions
    if [ "$end" -gt "$total_partitions" ]; then
        end="$total_partitions"
    fi

    # Create a batch file
    create_batch_file "$start" "$end" "$batch_number"
    batch_number=$((batch_number + 1))
done

# Output result
echo "Batch files created in directory '$output_dir'."
