# funnel_publisher
This is a python app setup to run as a google cloud function. In order to test locally you need to setup a local gcloud environment and use the functions_framework package.

Locally run: functions-framework --target publish_funnel and then load http://localhost:8080/ in your browser to trigger

Cloud Run function to write the data funnel to BigQuery.

To deploy:

gcloud functions deploy funnel-publisher \
--gen2 \
--region=us-central1     \
--runtime=python312 \
--source=. \
--entry-point=publish_funnel \
--trigger-topic=funnel_publisher_topic

There are tutorials and examples of how to do this here:

https://towardsdatascience.com/how-to-develop-and-test-your-google-cloud-function-locally-96a970da456f

https://github.com/GoogleCloudPlatform/functions-framework-python

