# User Events

## Business requirements

  The application has to gather all the user activity of different sources, the data set resultant called "Events".

  We have some specified sources:

  - Ecommerce like (visits, clicks, basket items, checkouts)
  - Marketing email publisher:
    - user activity like in email marketing (email impact, email open, email click)
  - Marketing display publisher:
    - user activity like banner (banner view, banner clicked)

  - Alowing to add new user activity sources

## Setup

### Ingestion infrastructure

```
sudo docker-compose -f ingestion-environment.yml up -d
```

### Setup .credentials

Use the .credentials.backup as template:

```
cp $REPO_PATH/scenarios/user-events/app/.credentials.backup $REPO_PATH/scenarios/user-events/app/.credentials
```

Fill the following .env variables with the AWS credentials that has AmazonS3FullAccess

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=
```

### Setup kafka topics

Each source has to has defined the topic definition. Sample source "ecommerce":

$REPO_PATH/scenarios/user-events/app/sources/ecommerce/topic.json

```
{
  "topic": "ecommerce_events",
  "num_partitions": 1,
  "replication_factor": 1
}
```

Create the topic:

```
SCENARIO_PATH=$REPO_PATH/scenarios/user-events/app

cd $SCENARIO_PATH && ./scripts/setup.sh sources/ecommerce/topic.json
```

### Setup consumers:

```
sudo docker-compose -f consumers.yml up
```

## Run

You can send user events of "ecommerce" source:

```
curl -X 'POST' \
  'http://0.0.0.0/ecommerce/events' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "event_type": "view",
  "user_id": "xxx11",
  "url": "https://site.com/home",
  "created_date": "2023-05-23T18:27:10.471Z"
}'
```

The events go over kafka topic and then the consumers would store the events on S3 in batches.
https://www.youtube.com/watch?v=xzxldb1qunY
https://stackoverflow.com/questions/63053460/no-module-named-pyspark-streaming-kafka-even-with-older-spark-version
