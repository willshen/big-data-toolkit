#!/bin/bash

# Function to display the help message
usage() {
    echo "Usage: $0 --zookeeper <zookeeper-connection-string> [--output <output-file>]"
    echo
    echo "  --zookeeper <zookeeper-connection-string>   Specify the Zookeeper connection string."
    echo "  --output <output-file>                      Specify the output file location (default: topics-to-move.json)."
    echo
    echo "This script generates a topics-to-move.json file for Kafka reassignment."
    echo
    exit 1
}

# Default output file location
OUTPUT_FILE="topics-to-move.json"

# Check if arguments are provided
if [ $# -lt 2 ]; then
    usage
fi

# Parse the command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --zookeeper)
            ZOOKEEPER=$2
            shift 2
            ;;
        --output)
            OUTPUT_FILE=$2
            shift 2
            ;;
        *)
            echo "Invalid argument: $1"
            usage
            ;;
    esac
done

# Ensure Zookeeper connection string is provided
if [ -z "$ZOOKEEPER" ]; then
    echo "Error: Zookeeper connection string is required."
    usage
fi

# Get the list of topics from Kafka using the provided Zookeeper connection string
topics=$(kafka-topics.sh --zookeeper "$ZOOKEEPER" --list)

# Create the JSON file
echo '{"topics": [' > "$OUTPUT_FILE"
for topic in $topics; do
  echo "{\"topic\": \"$topic\"}," >> "$OUTPUT_FILE"
done

# Remove the last comma and close the JSON array
sed -i '$ s/,$//' "$OUTPUT_FILE"
echo '],"version":1}' >> "$OUTPUT_FILE"

echo "topics-to-move.json has been generated at $OUTPUT_FILE."
