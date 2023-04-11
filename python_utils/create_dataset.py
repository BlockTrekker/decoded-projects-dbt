import os
from google.cloud import bigquery


def create_dataset(sql_body, dry_run = True):

    # First set the credentials 

    credential_path = "../keys/blocktrekker-admin.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    
    client = bigquery.Client()

    # Then create the dataset

    job_config = bigquery.QueryJobConfig(dry_run = dry_run)

    query_job = client.query(
        sql_body,
        job_config=job_config
    ) # API request - starts the query
    query_job.result()  # Waits for the query to finish

    print("dataset created")

    estimated_cost = query_job.total_bytes_processed / 10**12 * 5

    return estimated_cost