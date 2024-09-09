from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import secretmanager
import json

def get_gcp_credentials():
    # first get credentials to secret manager
    client = secretmanager.SecretManagerServiceClient()

    # get the secret that holds the service account key
    name = "projects/405806232197/secrets/service_account_json/versions/latest"
    response = client.access_secret_version(name=name)
    key = response.payload.data.decode("UTF-8")

    # use the key to get service account credentials
    service_account_info = json.loads(key)
    # Create BigQuery API client.
    gcp_credentials = service_account.Credentials.from_service_account_info(
        service_account_info,
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ],
    )

    bq_client = bigquery.Client(
        credentials=gcp_credentials, project="dataexploration-193817"
    )
    return bq_client

def publish_funnel():
    bq_client = get_gcp_credentials()
    print( "Hello world!!!")