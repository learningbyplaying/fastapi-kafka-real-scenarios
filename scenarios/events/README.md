# Events

1. Events are comming from different sources

  - Marketing publishers:

  - Ecommerce:


2. Setup kafka environment

3. Setup topics:

  "publishers":
    - partitions: 1
    - replicas: 1


4. Setup producer to write on topic "tablename"

4. Setup consumer to write on s3 Datalake:

  BUCKET_NAME/events/event_source=$event_source/
