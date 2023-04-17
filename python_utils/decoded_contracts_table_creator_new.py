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

def create_dbt_sql_file(evt_id, name, namespace):
    # Create the directory
    os.makedirs(f"""models/{namespace}""", exist_ok=True)
    # Create the dbt sql file
    with open(f"""models/{namespace}/{name}.sql""", "w") as f:
        f.write(evt_id.query_body)
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
job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

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


# get bucket and blob
bucket = storage_client.bucket(bucket_name)

# download all blobs with prefix to local directory
blobs = bucket.list_blobs(prefix='decoded_contracts')
file_list = []
for blob in blobs:
    # only download json files
    if blob.name.endswith('.json'):
        # construct local file path
        local_file_path = os.path.join(local_dir_path, blob.name)
        file_list.append(local_file_path)
        # download blob to local file
        blob.download_to_filename(local_file_path)
    print("new_file_download: {}".format(blob.name))

# open then reformat the json files
for file in file_list: 
    print("new_file_read")
    # Get the data for decoded_contracts from BQ
    with open(file, 'r') as f:
        # Read its contents as a string
        contents = f.read()

    # Split the string into individual objects
    objects = contents.split("\n")
    new_objects = []

        # Remove any empty objects
    objects = [o for o in objects if o]

    # # remove all square brackets in data objects
    # for object in objects:
    #     sub_object = json.loads(object)
    #     sub_object["data"] = sub_object["data"][1:-1]
    #     new_objects.append(json.dumps(sub_object))

    # Add a square bracket at the beginning and end of the file
    wrapped = "[" + ",".join(objects) + "]"

    # load wrapped
    wrapped_loaded = json.loads(wrapped)

    # Write the JSON data to a new file
    with open(file, 'w') as f:
        json.dump(json.loads(wrapped), f, indent=4)
    
    for row in wrapped_loaded:
        data_object = json.loads(row["data"].replace('"{', "{").replace('}"', "}").replace('\\', ''))
        print(data_object[0]["address"])
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
        contents = f.read()

    # Split the string into individual objects
    objects = contents.split("\n")
    new_objects = []

        # Remove any empty objects
    objects = [o for o in objects if o]

    # # remove all square brackets in data objects
    # for object in objects:
    #     sub_object = json.loads(object)
    #     sub_object["data"] = sub_object["data"][1:-1]
    #     new_objects.append(json.dumps(sub_object))

    # Add a square bracket at the beginning and end of the file
    wrapped = "[" + ",".join(objects) + "]"

    # load wrapped
    wrapped_loaded = json.loads(wrapped)

    # Write the JSON data to a new file
    with open(file, 'w') as f:
        json.dump(json.loads(wrapped), f, indent=4)        

    for row in wrapped_loaded:
        name = row["sub_name"]
        data = json.loads(row["data"].replace('"{', "{").replace('}"', "}").replace('\\', ''))
        for table_name in data:
             # parse JSON string in 'json_column' and replace with actual JSON object
            name = table_name["name"]
            address = table_name["address"]
            namespace = f"""{table_name["namespace"]}_ethereum"""
            type = table_name["type"]
            hash_id = table_name["hash_id"]
            created_ts = table_name["created_ts"]
            # estimated_cost = estimated_cost + create_dataset(dataset_creation_body)
            if type == "event":
                evt_id = Event(address,row[0],table_name["inputs"])
                count = 0
                for input in evt_id.inputs:
                    evt_id.query_lines.append(f"SAFE_CAST(topics[SAFE_OFFSET({count})] as {input['type']}) as {input['name']}")
                    count = count + 1 
                evt_id.full_name = evt["signature"]
                evt_id.evt_hash = evt["evt_hash"]
                evt_id.query_body = f"""
    {left_bracket}{left_bracket}
    config(
        materialized='view',
        schema='{namespace}',
        name='{namespace}',
    )
    {right_bracket}{right_bracket}
    SELECT
        address as contract_address,
        {','.join(evt_id.query_lines) + ',' if evt_id.query_lines != [] and evt_id.query_lines != '' else '' }
        block_number as evt_block_number,
        block_timestamp as evt_block_time,
        log_index as evt_index,
        transaction_hash as evt_tx_hash,
        transaction_index,
        evt_hash
    FROM 
        {evt_table}
    WHERE 
        evt_hash = '{evt_id.evt_hash}'
    AND 
        address = '{address}'
    AND 
        block_timestamp >= '{created_ts}'"""
                            
                create_dbt_sql_file(evt_id, evt['name'], namespace)
                # estimated_cost = estimated_cost + query_bigquery(evt_id.query_body)
                count_5 = count_5 + 1

                if data["type"] == call:
                    call_name = call["name"]
                    duplicate_list_call.append(call_name)
                    duplicate_number = count_duplicates(call["name"],duplicate_list_call)
                    if duplicate_number > 0:
                        call["name"] = f"{call['name']}_{str(duplicate_number)}"
                    call_id = Function(address,call["name"],call["inputs"])
                    count = 0
                    for input in call_id.inputs:
                        call_id.query_lines.append(f"SAFE_CAST(SUBSTRING(input, {11 + 64 * count}, {64}) as {schema_dict[input['type']]}) as {input['name']}")
                        count = count + 1 
                    call_id.full_name = call["signature"]
                    call_id.method_id = call["method_id"]
                    call_id.query_body = f"""
            {left_bracket}{left_bracket}
            config(
            materialized='view',
            schema='blocktrekker',
            name='{namespace}',
            )
            {right_bracket}{right_bracket}
            SELECT 
                {','.join(call_id.query_lines) + ',' if call_id.query_lines != [] and call_id.query_lines != '' else ''}
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
                LEFT(input,10) = '{call_id.method_id}'
            AND 
                to_address = '{address}'
            AND 
                block_timestamp >= '{created_ts}'"""
                    create_dbt_sql_file(call_id, call['name'], namespace)
                    count_5 = count_5 + 1


    # print(f"total:{estimated_cost}`
print(count_5)
print(f"total:{estimated_cost}")