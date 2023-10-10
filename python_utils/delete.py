from google.cloud import bigquery
import os
from concurrent.futures import ThreadPoolExecutor

def run_query(query_string):
    """Run a single query and return its result."""
    query_job = client.query(query_string)
    result = query_job.result()
    
    if query_job.errors:
        return ("Error", query_job.errors)
    else:
        return ("Success", None)

# Set the path to the JSON key file for the service account
credential_path = "/app/keys/blocktrekker-admin.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Create a client object
client = bigquery.Client()

# Set the project ID
project_id = 'blocktrekker'

# Set the query configuration
query_config = bigquery.QueryJobConfig()

# Set the query dataset
dataset_ref = "decoded_projects"

# Set the query table
table_ref = "delete"

# Set the query string
query_string = f"""
SELECT *
FROM `{project_id}.{dataset_ref}.{table_ref}`
"""

# Execute the query and fetch the result
query_job = client.query(query_string)
result = query_job.result()

delete_count = 0
query_strings_to_run = [row[0] for row in result]

# Use ThreadPoolExecutor to run queries in parallel
with ThreadPoolExecutor(max_workers=32) as executor:
    for status, error in executor.map(run_query, query_strings_to_run):
        if status == "Error":
            print(f"Error running query: {error}")
        else:
            delete_count += 1
            print(f"success: {delete_count}")
