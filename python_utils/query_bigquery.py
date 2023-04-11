import os
from google.cloud import bigquery


def query_bigquery(query_body, dry_run = True):

    # First set the credentials 

    credential_path = "../keys/blocktrekker-admin.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
    
    client = bigquery.Client()

    # Then create the table

    job_config = bigquery.QueryJobConfig(dry_run = dry_run)

    query_job = client.query(
        query_body,
        job_config=job_config
    ) # API request - starts the query
    query_job.result()  # Waits for the query to finish

    print("Query results loaded to the table")
    
    estimated_cost = query_job.total_bytes_processed / 10**12 * 5

    return estimated_cost