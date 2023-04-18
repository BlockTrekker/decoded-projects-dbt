import json
import csv
import os
import glob
from google.cloud import bigquery
from google.cloud.bigquery import job
from google.cloud import storage
from query_bigquery import query_bigquery
from create_dataset import create_dataset
import pandas as pd
    
credential_path = "../keys/blocktrekker-admin.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# # Get the Etherscan API key
# with open('../keys/etherscan_key.json') as f:
#     data = json.load(f)
#     etherscan_api_key = data["key"]

class Contract:
  def __init__(self, address):
    self.address =  address
    self.evt_names = []
    self.fx_names = []

class Event:
  def __init__(self, contract_address, evt_name, inputs):
    self.contract_address = contract_address
    self.evt_name = evt_name
    self.inputs = inputs
    self.input_names = json.load(inputs)
    self.input_types = []
    self.query_lines = []

class Function:
  def __init__(self, contract_address, fx_name, inputs):
    self.contract_address = contract_address
    self.fx_name = fx_name
    self.inputs = json.load(inputs)
    self.input_names = []
    self.input_types = []
    self.query_lines = []

def create_dbt_sql_file(query_body, name, namespace):
    # Create the directory
    os.makedirs(f"""models/{namespace}""", exist_ok=True)
    # Create the dbt sql file
    with open(f"""models/{namespace}/{name}.sql""", "w") as f:
        f.write(query_body)
    # print("Created dbt sql file")

def count_duplicates(current_string, strings_list):
    unique_strings = set(strings_list)
    count = 0
    for string in unique_strings:
        if string == current_string:
            count += 1
    return count - 1 if count > 1 else 0

# def query_etherscan(address, etherscan_api_key):
#     response = requests.get(f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={etherscan_api_key}")
#     result = {"contract_name" : response.json()['result'][0]['ContractName'], "abi" : response.json()['result'][0]['ABI']}
#     return result

bigquery_client = bigquery.Client()
storage_client = storage.Client()

# set configurations
project_id = 'blocktrekker'
dataset_id = 'decoded_contracts'
table_id = 'decode_contracts'
bucket_name = 'blocktrekker'
blob_prefix = 'decoded_contracts*.json'

# set table reference
table_ref = bigquery_client.dataset(dataset_id).table(table_id)

# set destination uri
destination_uri = f"gs://{bucket_name}/{blob_prefix}"

# set job configuration
job_config = bigquery.ExtractJobConfig()
job_config.destination_format = bigquery.DestinationFormat.CSV

# create extract job
extract_job = bigquery_client.extract_table(
    source=table_ref,
    destination_uris=destination_uri,
    job_config=job_config
)

# wait for job to complete
extract_job.result()


# set local directory path
local_dir_path = 'static_data'

if not os.path.exists(local_dir_path):
    os.makedirs(local_dir_path)
    print("Directory created successfully")
else:
    print("Directory already exists")


# get bucket and blob
bucket = storage_client.bucket(bucket_name)

# download all blobs with prefix to local directory
blobs = bucket.list_blobs(prefix='decoded_contracts')
file_list = []
for blob in blobs:
    # only download csv files
    if blob.name.endswith('.csv'):
        # construct local file path
        local_file_path = os.path.join(local_dir_path, blob.name)
        file_list.append(local_file_path)
        # download blob to local file
        blob.download_to_filename(local_file_path)
    print("new_file_download: {}".format(blob.name))

count_5 = 0
fxn_table = "{{ source('clustered_sources', 'clustered_traces') }}"
evt_table = "{{ source('clustered_sources', 'clustered_logs') }}"

project_id = "blocktrekker"
dataset_id = "spells"
count1 = 0
amt = 0
estimated_cost = 0
left_bracket = "{"
right_bracket = "}"

# open then reformat the json files
for file in file_list: 
    print("new_file_read")
    # Get the data for decoded_contracts from BQ
    with open(file, 'r') as f:
        # Read its contents as a string
        csv_reader = csv.DictReader(f)

    for row in csv_reader:
        table_name = row["sub_name"]
        name_space = row["name_space"]
        type = row["type"]
        hash_id = row["hash_id"]
        input_json = json.load(row["inputs"])
        output_json = json.load(row["outputs"])
        contract_details = json.load(row["contract_details"])
        contract_addresses_sql = []
        current_min_ts = 2723827000. #some arbitrarily new number
        for contracts in contract_details:
            query_lines = []
            contract_addresses_sql.append(contracts["address"])
            current_min_ts =  min(current_min_ts, contracts["created_ts"])
        if row["type"] == "event":
            input_count = 0
            for inputs in input_json:
                query_lines.append(f"SAFE_CAST(topics[SAFE_OFFSET({input_count})] as {input['type']}) as {input['name']}")
                input_count = input_count + 1
            query_body = f"""
{left_bracket}{left_bracket}
config(
    materialized='view',
    schema='{name_space}',
    name='{name_space}',
)
{right_bracket}{right_bracket}
SELECT
    address as contract_address,
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else '' }
    block_number as evt_block_number,
    block_timestamp as evt_block_time,
    log_index as evt_index,
    transaction_hash as evt_tx_hash,
    transaction_index,
    evt_hash
FROM 
    {evt_table}
WHERE 
    evt_hash = '{hash_id}'
AND 
    address IN [{','.join(contract_addresses_sql)}]
AND 
    block_timestamp >= '{current_min_ts}'"""
                        
            create_dbt_sql_file(query_body, table_name, name_space)
                # estimated_cost = estimated_cost + query_bigquery(evt_id.query_body)
            count_5 = count_5 + 1
            quit()

        if row["type"] == "call":
            input_count = 0
            output_count = 0
            query_lines = []
            output_query_sql = []
            for input in input_json:
                query_lines.append(f"SAFE_CAST(SUBSTRING(input, {11 + 64 * input_count}, {64}) as {input['type']}) as {input['name']}")
                count = count + 1 
            for output in output_json:
                output_query_sql.append(f"SAFE_CAST(SUBSTRING(output, {11 + 64 * output_count}, {64}) as {output['type']}) as {output['name']}")
            method_id = hash_id
            query_body = f"""
{left_bracket}{left_bracket}
config(
materialized='view',
schema='blocktrekker',
name='{name_space}',
)
{right_bracket}{right_bracket}
SELECT 
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else ''}
    transaction_hash as call_tx_hash,
    to_address as contract_address,
    output as output_0,
    block_number as call_block_number,
    block_timestamp as call_block_time,
    status as call_success,
    trace_address as call_trace_address,
    trace_id as call_trace_id,
    error as call_error,
    trace_type as call_trace_type,
    from_address as trace_from_address,
    value as trace_value,
    method_id
FROM 
    {fxn_table}
WHERE 
    LEFT(input,10) = '{method_id}'
AND 
    to_address IN [{','.join(contract_addresses_sql)}]
AND 
    block_timestamp >= '{current_min_ts}'"""
                
            create_dbt_sql_file(query_body, table_name, name_space)
            count_5 = count_5 + 1


    # print(f"total:{estimated_cost}`
print(count_5)
print(f"total:{estimated_cost}")