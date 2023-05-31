from google.cloud import bigquery
import os

# Set the path to the JSON key file for the service account
credential_path = "../keys/blocktrekker-admin.json"
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

print(query_string)

# Execute the query and fetch the result
query_job = client.query(query_string)
result = query_job.result()

delete_count = 0

# Iterate over each row in the result
for row in result:
    # Access each column of the row by name or index
    new_query_string = row[0]

    # now query with the result as the query string
    query_job = client.query(new_query_string)
    result = query_job.result()

    if query_job.errors:
        print("Error running query: {}".format(query_job.errors))
    else:
        print("success: {}".format(delete_count))