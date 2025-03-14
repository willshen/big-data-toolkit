#!/bin/bash

# Default values
ZOOKEEPER_URL="localhost:2181"
BROKER_URL="localhost:9092"
PARTITION_OUTPUT_FILE="kafka_topic_partition_offsets.csv"

# Function to print the usage of the script
print_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --zookeeper-url <url>      Zookeeper URL (host:port) (default: localhost:2181)"
    echo "  --broker-url <url>         Broker URL (host:port) (default: localhost:9092)"
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
        --broker-url )        shift
                                BROKER_URL=$1
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
    PARTITION_OUTPUT_FILE="$OUTPUT_DIR/kafka_topic_partition_offsets.csv"
fi

# Write headers for both CSV files
echo "Topic,Partition,Latest Offset" > $PARTITION_OUTPUT_FILE

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

    # Get the latest offset for  each partition of the topic
    PARTITIONS=$(kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $BROKER_URL --topic $TOPIC --time -1)

    # Loop through each partition's details
    echo "$PARTITIONS" | grep -P "$TOPIC:[0-9]+:[0-9]+" | while IFS=':' read -r topic partition offset; do
        # Append partition info to the partition CSV file
        echo "$topic,$partition,$offset" >> "$PARTITION_OUTPUT_FILE"
    done

done

echo "Partition offset information saved to $PARTITION_OUTPUT_FILE"
