

1. Setup kafka in local

2. Setup topics, partitions

3. Setup producer to write on topic "tablename"

4. Setup consumer to write on s3 Datalake:

  BUCKET_NAME/tablename=$tablename/delta_at=${delta_at}

5. Setup schema:

   customers:
    - id
    - name
    - description
    - delta_at

    
5. Run over producer:

  T1

  Customer
    - id: 1
    - name: 'Amazon.es'
    - description: "Amazon"


  T2
  Customer
    - id: 1
    - name: 'Amazon.es'
    - description: "Amazon ES"
