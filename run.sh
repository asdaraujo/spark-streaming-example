#!/bin/bash
set -u
set -e

ACCESS_KEY=$1
PUBLIC_IP=${PUBLIC_IP:-$(curl ifconfig.me)}

TARGET_JAR=./target/spark-streaming-example-1.0-SNAPSHOT-jar-with-dependencies.jar

CHANGED_FILES=$(find src/ -type f ! -name "*.swp" -newer $TARGET_JAR 2>&1 | wc -l)

kafka-consumer-groups --bootstrap-server $(hostname -f):9092 --delete --group spark-consumer 2>/dev/null

([[ $CHANGED_FILES -eq 0 ]] || mvn clean package) && \
spark-submit \
  --master local[2] \
  --jars kudu-spark2_2.11-1.9.0.jar,spark-core_2.11-1.5.2.logging.jar \
  --packages org.apache.spark:spark-streaming-kafka_2.11:1.6.3 \
  --class com.cloudera.workshop.SparkStreamingIoT \
  $TARGET_JAR \
  $ACCESS_KEY $PUBLIC_IP 2>&1 | tee run.log
