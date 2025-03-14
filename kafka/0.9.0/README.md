# Kafka Partition and Topic Information Export Script

This script is used to gather detailed information about Kafka topics and their partitions from a Kafka cluster using ZooKeeper. It outputs the following data in CSV format:

- **Partition Details**: Topic, Partition, Leader, Replicas, and ISR (In-Sync Replicas)
- **Summary**: Topic, Partition Count, and Replication Factor

It is designed to help administrators and developers monitor and analyze the Kafka cluster’s topic and partition setup.

## Features

- Retrieve a list of all Kafka topics and their partitions.
- Gather details such as leaders, replicas, and in-sync replicas (ISRs) for each partition.
- Generate two CSV files:
  - `kafka_topic_partitions.csv`: Contains detailed partition information for each topic.
  - `kafka_topic_summary.csv`: Contains the summary information for each topic (Partition Count, Replication Factor).
- Displays progress to the console as the script processes each topic.
- Optionally, specify the output directory for the generated CSV files.

## Requirements

- Kafka 0.9.0 or later
- Access to the Kafka cluster and ZooKeeper
- `kafka-topics.sh` available in your Kafka installation directory

## Installation

1. Clone this repository or download the script.
2. Ensure that you have the `kafka-topics.sh` script available from your Kafka installation.

## Usage

```bash
./kafka-export-partitions.sh --zookeeper-url <zookeeper-url> --output-dir <output-directory>
```
