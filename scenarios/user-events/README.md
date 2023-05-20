## Setup Ingestion Environment:

```
sudo docker-compose -f ingestion-environment.yml up -d
```

## Setup channel "Ecommerce":

```
cd $REPO_PATH/scenarios/user-events/app && ./setup.sh channels/ecommerce/topic.json
```

## Setup consumers:

```
sudo docker-compose -f consumers.yml up -d
```

# Fast API Kafka Events




## Business requirements

  The application has to gather all the user activity of different sources, the data set resultant called "Events".

  We have some specified sources:

  - Ecommerce like (visits, clicks, basket items, checkouts)
  - Marketing email publisher:
    - user activity like in email marketing (email impact, email open, email click)
  - Marketing display publisher:
    - user activity like banner (banner view, banner clicked)

  - Alowing to add new user activity sources

## Infraestructure


## Setup

### Setup docker

```
cd $REPO_PATH/scenarios/events && docker-compose up
```
### How to run

1. Go to URL: http://0.0.0.0/docs

2. And POST **/setup** to create the topic:

```
curl -X 'POST' \
  'http://0.0.0.0/setup' \
  -H 'accept: application/json' \
  -d ''
```

3. And POST **/producer** to run the producer, you can choose the message:

```
curl -X 'POST' \
  'http://0.0.0.0/producer' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "text": "my text input"
}'
```

You can see the result on the docker-compose output
