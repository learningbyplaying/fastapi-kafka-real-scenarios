#!/bin/bash

#echo ">>GET Subjects"
#echo ""
#curl -X GET http://0.0.0.0:8081/subjects
#echo ""
#echo ""

#echo ">>POST Schemas"
#echo ""
#subject=ecommerce_event
#curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
#  --data @ecommerce_event.avsc \
#  "http://0.0.0.0:8081/subjects/$subject/versions"
#echo ""
#echo ""

echo ">>Topic creation"
echo ""
topic=ecommerce_events
curl -X 'POST' \
  'http://0.0.0.0:80/kafka-topic-create' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d "{\"topic\": \"$topic\", \"num_partitions\": 1, \"replication_factor\": 1}"
echo ""
echo ""
