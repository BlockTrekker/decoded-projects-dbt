
import os
from google.cloud import bigquery

credential_path = "../keys/blocktrekker-admin.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Set up the BigQuery client
client = bigquery.Client()


# Fetch the rows from the `blocktrekker.decoded_projects.delete` table
query = """
    SELECT statement
    FROM `blocktrekker.decoded_projects.delete`
"""
rows = client.query(query).result()

# Execute each row as a SQL statement
for row in rows:
    statement = row["statement"]
    print(f"Executing SQL statement: {statement}")
    client.query(statement)