#!/bin/bash

# contains the correct packages for decoding protobuf files and connecting to AWS S3 and Snowflake for Spark 3.5.0

spark-submit \
  --packages org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,org.apache.spark:spark-protobuf_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,net.snowflake:spark-snowflake_2.12:3.0.0,net.snowflake:snowflake-jdbc:3.13.30 \
  spark_load_static.py