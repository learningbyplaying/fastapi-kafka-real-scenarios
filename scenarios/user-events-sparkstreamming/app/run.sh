#!/bin/bash
set -o allexport; source /app/.credentials; set +o allexport

MY_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0

spark-submit --packages $MY_PACKAGES consumer-spark2.py
