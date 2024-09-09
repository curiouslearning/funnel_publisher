# funnel_publisher
Cloud Run function to write the data funnel to BigQuery.

To deploy:

gcloud functions deploy funnel-publisher \
--gen2 \
--region=us-central1     \
--runtime=python312 \
--source=. \
--entry-point=publish_funnel \
--trigger-topic=funnel_publisher_topic
