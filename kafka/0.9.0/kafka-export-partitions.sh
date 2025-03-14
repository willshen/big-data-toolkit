#!/bin/bash

# Default values
ZOOKEEPER_URL="localhost:2181"
PARTITION_OUTPUT_FILE="kafka_topic_partitions.csv"
SUMMARY_OUTPUT_FILE="kafka_topic_summary.csv"

# Function to print the usage of the script
print_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --zookeeper-url <url>      Zookeeper URL (host:port) (default: localhost:2181)"
    echo "  --output-dir <dir>         Directory to save CSV output files (default: current directory)"
    echo "  --help                     Show this help message"
    echo ""
}

# Parse command-line arguments
while [[ "$1" != "" ]]; do
    case $1 in
        --zookeeper-url )        shift
                                ZOOKEEPER_URL=$1
                                ;;
        --output-dir )           shift
                                OUTPUT_DIR=$1
                                ;;
        --help )                 print_help
                                exit 0
                                ;;
        * )                      echo "Invalid option: $1"
                                print_help
                                exit 1
                                ;;
    esac
    shift
done

# If OUTPUT_DIR is set, use it; otherwise, use the current directory
if [[ -n "$OUTPUT_DIR" ]]; then
    PARTITION_OUTPUT_FILE="$OUTPUT_DIR/kafka_topic_partitions.csv"
    SUMMARY_OUTPUT_FILE="$OUTPUT_DIR/kafka_topic_summary.csv"
fi

# Write headers for both CSV files
echo "Topic,Partition,Leader,Replicas,ISRs" > $PARTITION_OUTPUT_FILE
echo "Topic,Partition Count,Replication Factor" > $SUMMARY_OUTPUT_FILE

# List all topics using Zookeeper
TOPICS=$(kafka-topics.sh --zookeeper $ZOOKEEPER_URL --list)

# Total number of topics for progress tracking
TOTAL_TOPICS=$(echo "$TOPICS" | wc -l)
CURRENT_TOPIC=0

# Loop through each topic and describe it
for TOPIC in $TOPICS; do
    CURRENT_TOPIC=$((CURRENT_TOPIC + 1))

    # Print progress to the console
    echo "Processing topic $CURRENT_TOPIC of $TOTAL_TOPICS: $TOPIC"

    # Describe the topic and process each partition
    PARTITIONS=$(kafka-topics.sh --zookeeper $ZOOKEEPER_URL --describe --topic $TOPIC)

    # Initialize variables for partition count and replication factor
    PARTITION_COUNT=0
    REPLICATION_FACTOR=0

    # Extract PartitionCount and ReplicationFactor from the topic description
    PARTITION_COUNT=$(echo "$PARTITIONS" | sed -n 's/.*PartitionCount:[[:space:]]*\([0-9]*\).*/\1/p')
    REPLICATION_FACTOR=$(echo "$PARTITIONS" | sed -n 's/.*ReplicationFactor:[[:space:]]*\([0-9]*\).*/\1/p')

    # Loop through each partition's details
    echo "$PARTITIONS" | grep -P "Partition:\s*\d+" | while read -r line; do
        # Extract Partition, Leader, Replicas, and ISRs using specific regex
        PARTITION=$(echo "$line" | sed -E 's/.*Partition:\s*([0-9]+)\s*Leader.*/\1/')
        LEADER=$(echo "$line" | sed -E 's/.*Leader:\s*([0-9]+)\s*Replicas.*/\1/')
        REPLICAS=$(echo "$line" | sed -E 's/.*Replicas:\s*([0-9,]+)\s*Isr:.*/\1/' | sed 's/,/;/g')
        ISRS=$(echo "$line" | sed -E 's/.*Isr:\s*([0-9,]+)\s*/\1/' | sed 's/,/;/g')

        # Append partition info to the partition CSV file
        echo "$TOPIC,$PARTITION,$LEADER,$REPLICAS,$ISRS" >> "$PARTITION_OUTPUT_FILE"
    done

    # Append the summary (Topic, Partition Count, Replication Factor) to the summary CSV file
    echo "$TOPIC,$PARTITION_COUNT,$REPLICATION_FACTOR" >> $SUMMARY_OUTPUT_FILE
done

echo "Partition information saved to $PARTITION_OUTPUT_FILE"
echo "Topic summary saved to $SUMMARY_OUTPUT_FILE"
