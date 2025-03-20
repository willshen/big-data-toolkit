#!/bin/bash

# Default values for arguments
FILE_PATH=""
ZOOKEEPER=""

# Function to display usage
usage() {
  echo "Usage: $0 --file <file_with_topic_names> --zookeeper <zookeeper_address>"
  echo "  --file      Path to the file containing Kafka topic names"
  echo "  --zookeeper Zookeeper address (e.g., localhost:2181)"
  exit 1
}

# Parse long flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    --file)
      FILE_PATH="$2"
      shift 2
      ;;
    --zookeeper)
      ZOOKEEPER="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      usage
      ;;
  esac
done

# Check if the necessary arguments are provided
if [ -z "$FILE_PATH" ] || [ -z "$ZOOKEEPER" ]; then
  usage
fi

# Check if the file exists
if [ ! -f "$FILE_PATH" ]; then
  echo "File not found: $FILE_PATH"
  exit 1
fi

# Loop through each line in the file and delete the corresponding Kafka topic
while IFS= read -r topic; do
  # Skip empty lines or lines starting with a comment
  if [ -z "$topic" ] || [[ "$topic" =~ ^# ]]; then
    continue
  fi
  
  echo "Deleting Kafka topic: $topic"

  # Execute the Kafka command to delete the topic using Zookeeper
  kafka-topics.sh --zookeeper "$ZOOKEEPER" --topic "$topic" --delete

  # Check if the delete operation was successful
  if [ $? -eq 0 ]; then
    echo "Topic $topic deleted successfully."
  else
    echo "Error deleting topic $topic."
  fi
done < "$FILE_PATH"
