#!/bin/bash

# Check if the user provided a file as input
if [ $# -ne 1 ]; then
  echo "Usage: $0 <path_to_json_file>"
  exit 1
fi

# Input JSON file
FILE=$1

# Check if the file exists
if [ ! -f "$FILE" ]; then
  echo "File not found!"
  exit 1
fi

# Function to generate broker stats
generate_stats() {
  local json=$1
  declare -A broker_total_replicas
  declare -A broker_preferred_replicas

  # Process the partitions in one go
  partitions=$(echo "$json" | jq -c '.partitions[]')

  # Loop over each partition and handle replica counts
  while IFS= read -r partition; do
    # Extract the replica information
    replicas=$(echo "$partition" | jq -r '.replicas | @csv' | tr -d '"')

    # Extract the preferred replica (the first in the list)
    preferred_replica=$(echo "$replicas" | cut -d',' -f1)

    # Increment the total replica and preferred replica counters for each broker
    for broker in $(echo "$replicas" | tr ',' '\n'); do
      ((broker_total_replicas[$broker]++))

      # If the broker is the preferred replica, increment preferred counter
      if [ "$broker" == "$preferred_replica" ]; then
        ((broker_preferred_replicas[$broker]++))
      fi
    done
  done <<< "$partitions"

  # Prepare output and store in an array
  output=()
  for broker in "${!broker_total_replicas[@]}"; do
    total_replicas=${broker_total_replicas[$broker]}
    preferred_replicas=${broker_preferred_replicas[$broker]:-0}
    output+=("Broker $broker - Total Replicas: $total_replicas, Preferred Replicas: $preferred_replicas")
  done

  # Sort and output
  printf "%s\n" "${output[@]}" | sort -n
}

# Extract the JSON content for "Current partition replica assignment"
current_json=$(awk '/Current partition replica assignment/{flag=1;next} /Proposed partition reassignment configuration/{flag=0} flag' "$FILE")

# Extract the JSON content for "Proposed partition reassignment configuration"
proposed_json=$(awk '/Proposed partition reassignment configuration/{flag=1;next} flag' "$FILE")

# Check if we successfully extracted the JSON content
if [ -z "$current_json" ]; then
  echo "Error: Current partition replica assignment not found or empty."
  exit 1
fi

if [ -z "$proposed_json" ]; then
  echo "Error: Proposed partition reassignment configuration not found or empty."
  exit 1
fi

# Output the stats for current assignment
echo "Current Partition Assignment Stats:"
generate_stats "$current_json"
echo

# Output the stats for proposed reassignment
echo "Proposed Partition Assignment Stats:"
generate_stats "$proposed_json"
