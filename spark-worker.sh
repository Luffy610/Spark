#!/bin/bash

# Define the Spark Master URL (Replace with actual master IP or hostname if needed)
SPARK_MASTER_URL="spark://localhost:7077"

# Start the Spark Worker node
spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL