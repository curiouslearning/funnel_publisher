from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import secretmanager
import json
import pandas as pd
import users
import metrics
from pandas_gbq import to_gbq

def get_gcp_credentials():
    # first get credentials to secret manager
    try:
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
            credentials=gcp_credentials, project="dataexploration-193817")
 
    except Exception as e:
        print("Failed to obtain a Big Query client")
        print(e)

    return bq_client,gcp_credentials

def publish_funnel():
    print("Publishing funnel")

    bq_client,gcp_credentials = get_gcp_credentials()
    
    df_cr_users, df_cr_first_open, df_cr_app_launch = users.get_users_list(bq_client)

    languages = users.get_language_list(bq_client)

    df_funnel = metrics.build_funnel_dataframe( df_cr_users, df_cr_first_open, df_cr_app_launch,index_col="language", languages=languages)
    df_funnel = metrics.add_level_percents(df_funnel)
    try:
        to_gbq(df_funnel, 'dataexploration-193817.user_data.funnel_snapshots', project_id='dataexploration-193817', if_exists='append', credentials=gcp_credentials)
        print("Successfully wrote " + str(len(df_funnel)) + " rows to the table")
    except Exception as e:
        print("Failed to publish to the table")
        print(e)
