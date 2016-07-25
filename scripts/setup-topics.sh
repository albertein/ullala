#! /bin/sh

kafka_2.11-0.10.0.0/bin/kafka-topics.sh --create --zookeeper KAFKA:2181 --replication-factor 1 --partitions 6 --topic CAMERA_FEED
kafka_2.11-0.10.0.0/bin/kafka-topics.sh --create --zookeeper KAFKA:2181 --replication-factor 1 --partitions 1 --topic PROCESSED_FEED
kafka_2.11-0.10.0.0/bin/kafka-topics.sh --create --zookeeper KAFKA:2181 --replication-factor 1 --partitions 1 --topic APPLICATION_LOGS
