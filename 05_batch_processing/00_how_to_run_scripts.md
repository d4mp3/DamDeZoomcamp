python3 07_spark_local_cluster.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
    

# Use spark-submit for running the script on the cluster

URL="spark://ubuntu:7077"

spark-submit \
    --master="${URL}" \
    07_spark_local_cluster.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021
        
        
# Using Google Cloud SDK for submitting to dataproc (link)

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-north1\
    gs://dtc_data_lake_dt-zoomcamp-nytaxi/code/07_spark_local_cluster.py \
    -- \
        --input_green=gs://dtc_data_lake_dt-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_dt-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_dt-zoomcamp-nytaxi/report-2020

# Big Query

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-north1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_dt-zoomcamp-nytaxi/code/08_spark_bigquery.py \
    -- \
        --input_green=gs://dtc_data_lake_dt-zoomcamp-nytaxi/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_dt-zoomcamp-nytaxi/pq/yellow/2020/*/ \
        --output=trips_data_all.reports-2020