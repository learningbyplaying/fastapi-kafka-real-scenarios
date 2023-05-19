# FastAPI Kafka Hello World


## Setup docker

```
cd $REPO_PATH && docker-compose up
```
## How to run

1. Go to URL: http://0.0.0.0/docs

2. And post /setup to create the topic:

```
curl -X 'POST' \
  'http://0.0.0.0/setup' \
  -H 'accept: application/json' \
  -d ''
```

3. And post /producer to run the producer, you can choose the message:

```
curl -X 'POST' \
  'http://0.0.0.0/producer' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "text": "my text input"
}'
```
