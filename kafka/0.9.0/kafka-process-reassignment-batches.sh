#!/bin/bash

# Ensure the correct number of arguments are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <reassignment_directory> <zookeeper_address>"
    exit 1
fi

# Input reassignment directory and zookeeper address
reassignment_dir="$1"
zookeeper_address="$2"

# Validate if the reassignment directory exists
if [ ! -d "$reassignment_dir" ]; then
    echo "Error: Directory '$reassignment_dir' does not exist."
    exit 1
fi

# List all JSON reassignment files in the directory
reassignment_files=$(find "$reassignment_dir" -type f -name "reassignment_batch_*.json" | sort)

# Check if there are any files to process
if [ -z "$reassignment_files" ]; then
    echo "No reassignment files found in '$reassignment_dir'."
    exit 1
fi

# Execute each reassignment file one by one
for reassignment_file in $reassignment_files; do
    echo "Checking reassignment status for file: $reassignment_file"
    
    # First, verify the reassignment status
    verify_output=$(kafka-reassign-partitions --zookeeper "$zookeeper_address" --reassignment-json-file "$reassignment_file" --verify)
    
    # Check if the reassignment is already completed
    echo "$verify_output" | grep -vF 'completed successfully' | grep -vFc 'Status of partition reassignment' > /dev/null
    
    # If it's not completed, execute the reassignment
    if [ $? -eq 0 ]; then
        echo "Reassignment not completed for file '$reassignment_file'. Executing reassignment..."

        # Execute the reassignment using kafka-reassign-partitions
        kafka-reassign-partitions --zookeeper "$zookeeper_address" --reassignment-json-file "$reassignment_file" --execute

        if [ $? -ne 0 ]; then
            echo "Error: Reassignment failed for file '$reassignment_file'. Exiting."
            exit 1
        fi
    else
        echo "Reassignment already completed for file '$reassignment_file'. Skipping execution."
        continue
    fi

    # Check the status of the reassignment to verify it's completed successfully
    echo "Waiting for reassignment to complete for file '$reassignment_file'..."

    # Retry counter
    retries=0
    max_retries=10

    # Wait until the reassignment is completed successfully
    while [ $retries -lt $max_retries ]; do
        # Run the verify command and check the output
        verify_output=$(kafka-reassign-partitions --zookeeper "$zookeeper_address" --reassignment-json-file "$reassignment_file" --verify)
        
        # Check if the output contains "completed successfully"
        echo "$verify_output" | grep -vF 'completed successfully' | grep -vFc 'Status of partition reassignment' > /dev/null
        
        # If grep returns 0, meaning it's not "completed successfully", continue checking
        if [ $? -eq 0 ]; then
            echo "Reassignment still in progress for file '$reassignment_file'. Retrying in 30 seconds..."
            sleep 30
            retries=$((retries + 1))
        else
            echo "Reassignment completed successfully for '$reassignment_file'."
            break
        fi
    done

    # If the reassignment still hasn't completed after max retries, exit with an error
    if [ $retries -ge $max_retries ]; then
        echo "Error: Reassignment for file '$reassignment_file' did not complete after $max_retries attempts. Exiting."
        exit 1
    fi
done

# All reassignments completed successfully
echo "All reassignments completed successfully."
