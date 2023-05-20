#!/bin/bash

echo ">>GET Subjects"
curl -X GET http://0.0.0.0:8081/subjects
echo ""

echo ">>POST Subjects"

subject=user-value
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @user.avsc \
  "http://0.0.0.0:8081/subjects/$subject/versions"
echo ""


subject=ecommerce-event
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data @channels/ecommerce/ecommerce_event.avsc \
  "http://0.0.0.0:8081/subjects/$subject/versions"
echo ""
