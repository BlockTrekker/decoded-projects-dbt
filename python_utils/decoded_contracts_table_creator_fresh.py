import json
import os
from google.cloud import bigquery
from google.cloud.bigquery import job
from google.cloud import storage
from query_bigquery import query_bigquery
from create_dataset import create_dataset
import pandas as pd
import math
import shutil
from collections import Counter
import time


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

def create_dbt_sql_file(query_body, table_name, namespace, separate_models):
    # Create the directory
    model_num = model_num = math.floor(count_5/2500) + separate_models

    os.makedirs(f"""models_{model_num}/{namespace}""", exist_ok=True)
    # Create the dbt sql file
    with open(f"""models_{model_num}/{namespace}/{table_name}.sql""", "w") as f:
        f.write(query_body)
    # print("Created dbt sql file")

def count_duplicates(string, string_list):
    counts = Counter(string_list)
    return counts[string]

bigquery_client = bigquery.Client()
storage_client = storage.Client()

# set configurations
project_id = 'blocktrekker'
dataset_id = 'decoded_contracts'
table_id = 'decode_contracts'
bucket_name = 'blocktrekker'
blob_prefix = 'decoded_contracts*.csv'

# fresh download or just take your files
while True:
    user_input = input("Do you want to download fresh csv's from the bucket? (Y/N)   ").lower()
    if user_input == "y":
        # Do something  
        break
    elif user_input == "n":
        print("Breaking the loop.")
        break
    else:
        print("Invalid input. Please enter Y or N.")

# get bucket and blob
bucket = storage_client.bucket(bucket_name)

# List all files in the bucket
blobs = bucket.list_blobs()

if user_input == "y":
    # Delete each file in the bucket
    for blob in blobs:
        blob.delete()

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
        shutil.rmtree(local_dir_path)
        os.makedirs(local_dir_path)
        print("Directory (static_directory) deleted then recreated successfully")


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
        if user_input == "y":
            blob.download_to_filename(local_file_path)
    print("new_file_download: {}".format(blob.name))

count_5 = 0
fxn_table = "{{ source('ethereum', 'traces') }}"
evt_table = "{{ source('ethereum', 'logs') }}"

project_id = "blocktrekker"
dataset_id = "spells"
count1 = 0
amt = 0
estimated_cost = 0
left_bracket = "{"
right_bracket = "}"

# open then reformat the csv files
for file in file_list: 
    dfs = []
    print("new_file_read")
    # Get the data for decoded_contracts from BQ
    # with open(file, 'r') as f:
        # Read its contents as a string

    csv_reader = pd.read_csv(file, delimiter=',', low_memory=False)
    evt_sub_name_counter = []
    call_sub_name_counter = []
    
    for index, row in csv_reader.iterrows():
        table_name = row["sub_name"].replace(".", "_").replace("++", "")
        name_space = f"{row['namespace']}_ethereum"
        type = row["type"]
        hash_ids = row["hash_ids"][1:-1]

        if row['inputs_']:
            try:
                if row['inputs_'] == "[null]":
                    input_json = []
                else:
                    input_json_array = json.loads(f'[{json.loads(row["inputs_"])[0]}]')
            except json.decoder.JSONDecodeError as e:
                print(e)
                print("input: "f'[{json.loads(row["inputs_"])[0]}]')
                input_json_array = []
        else:
            input_json_array = []
        if row['outputs']:
            try:
                if row['outputs'] == "[null]" or row['outputs'] == '[""]':
                    output_json_array = []
                else:
                    output_json_array = json.loads(f'[{json.loads(row["outputs"])[0]}]')
            except json.decoder.JSONDecodeError as e:
                print(e)
                print("output: "f'[{json.loads(row["outputs"])[0]}]')
                output_json_array = []
        else:
            output_json_array = []
        contract_addresses = row['contract_addresses'][1:-1]
        current_min_ts = row['min_created_ts']
        if row["type"] == "event":
            query_lines = []
            input_count = 0
            for input_ in input_json_array:
                if input_['type'].startswith('ARRAY'):
                    cast_type = input_['type'].replace('ARRAY', '').replace('<', '').replace('>', '')
                    try:
                        query_lines.append(f"(SELECT ARRAY_AGG(SAFE_CAST(item AS {cast_type})) as weights FROM UNNEST(SPLIT((SELECT MAX(IF(d.key = '{input_['name']}', d.value, NULL)) FROM UNNEST(decoded_values) AS d), ',')) AS item WHERE item IS NOT NULL AND SAFE_CAST(item AS BIGNUMERIC) IS NOT NULL) AS `{input_['name']}`")    
                        # query_lines.append(f"SAFE_CAST(topic{input_count} as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                    except KeyError as e:
                        query_lines.append(f"(SELECT ARRAY_AGG(SAFE_CAST(item AS {cast_type})) as weights FROM UNNEST(SPLIT((SELECT MAX(IF(d.key = 'input_{input_count}', d.value, NULL)) FROM UNNEST(decoded_values) AS d), ',')) AS item WHERE item IS NOT NULL AND SAFE_CAST(item AS BIGNUMERIC) IS NOT NULL) AS `input_{input_count}`")    
                        input_count = input_count + 1
                else:
                    try:
                        query_lines.append(f"SAFE_CAST((SELECT MAX(IF(d.key = '{input_['name']}', d.value, NULL)) FROM UNNEST(decoded_values) AS d) AS {input_['type']}) AS `{input_['name']}`")
                    except KeyError as e:
                        query_lines.append(f"SAFE_CAST((SELECT MAX(IF(d.key = 'input_{input_count}', d.value, NULL)) FROM UNNEST(decoded_values) AS d) AS {input_['type']}) AS `input_{input_count}`")
                        input_count = input_count + 1
            query_body = f"""
{left_bracket}{left_bracket}
config(
    materialized='table',
    schema='{name_space}',
    name='{name_space}',
)
{right_bracket}{right_bracket}
WITH cte AS (
    SELECT
        contract_address,
        block_number as evt_block_number,
        block_time as evt_block_time,
        `index` as evt_index,
        tx_hash as evt_tx_hash,
        tx_index as evt_tx_index,
        udfs.DECODE_LOG_ENTRY(topic0, topic1, topic2, topic3, data, abi) as decoded_values,
        evt_hash
    FROM {evt_table} AS l
    LEFT JOIN `blocktrekker.decoded_contracts.dune_abis` AS da ON da.address = l.contract_address
    WHERE evt_hash in ({hash_ids})
    AND 
    contract_address IN ({contract_addresses})
    AND 
    block_time >= '{current_min_ts}'
)

SELECT
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else '' }
    contract_address,
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    evt_tx_index,
    evt_hash,
FROM 
    cte, UNNEST(decoded_values) as d
    """
                        
            # Big Query has a 256k character limit on queries, which we trigger with large contract address lists
            # To get around this, we get an address list CTE and call that into the query 
            if len(query_body) > 250000:
                query_body = f"""
{left_bracket}{left_bracket}
config(
    materialized='table',
    schema='{name_space}',
    name='{name_space}',
)
{right_bracket}{right_bracket}
WITH contract_addresses AS (
    SELECT
        TRIM(array_element, "[]") as contract_address
    FROM 
        {left_bracket}{left_bracket} source('decoded_contracts', 'decode_contracts') {right_bracket}{right_bracket}, 
        UNNEST(SPLIT(contract_addresses, ',')) AS array_element
    WHERE
        sub_name = '{table_name}'
),

cte AS (
    SELECT
        contract_address,
        block_number as evt_block_number,
        block_time as evt_block_time,
        `index` as evt_index,
        tx_hash as evt_tx_hash,
        tx_index as evt_tx_index,
        udfs.DECODE_LOG_ENTRY(topic0, topic1, topic2, topic3, data, abi) as decoded_values,
        evt_hash
    FROM {evt_table} AS l
    LEFT JOIN `blocktrekker.decoded_contracts.dune_abis` AS da ON da.address = l.contract_address
    WHERE evt_hash in ({hash_ids})
    AND 
    contract_address IN (SELECT contract_address FROM (select * from contract_addresses))
    AND 
    block_time >= '{current_min_ts}'
)

SELECT
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else '' }
    contract_address,
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    evt_tx_index,
    evt_hash,
FROM 
    cte, UNNEST(decoded_values) as d
    """
            separate_models = count_duplicates(table_name, evt_sub_name_counter)
            evt_sub_name_counter.append(table_name)
            create_dbt_sql_file(query_body, table_name, name_space, separate_models)
            count_5 = count_5 + 1
    
        if row["type"] == "call":
            input_count = 0
            output_count = 0
            query_lines = []
            output_query_sql = []
            for input_ in input_json_array:
                if input_['type'].startswith('ARRAY'):
                    cast_type = input_['type'].replace('ARRAY', '').replace('<', '').replace('>', '')
                    try:
                        query_lines.append(f"(SELECT ARRAY_AGG(SAFE_CAST(item AS {cast_type})) as weights FROM UNNEST(SPLIT((SELECT MAX(IF(d.key = '{input_['name']}', d.value, NULL)) FROM UNNEST(decoded_values) AS d), ',')) AS item WHERE item IS NOT NULL AND SAFE_CAST(item AS BIGNUMERIC) IS NOT NULL) AS `{input_['name']}`")    
                        # query_lines.append(f"SAFE_CAST(topic{input_count} as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                    except KeyError as e:
                        query_lines.append(f"(SELECT ARRAY_AGG(SAFE_CAST(item AS {cast_type})) as weights FROM UNNEST(SPLIT((SELECT MAX(IF(d.key = 'input_{input_count}', d.value, NULL)) FROM UNNEST(decoded_values) AS d), ',')) AS item WHERE item IS NOT NULL AND SAFE_CAST(item AS BIGNUMERIC) IS NOT NULL) AS `input_{input_count}`")    
                        input_count = input_count + 1
                else:
                    try:
                        query_lines.append(f"SAFE_CAST((SELECT MAX(IF(d.key = '{input_['name']}', d.value, NULL)) FROM UNNEST(decoded_values) AS d) AS {input_['type']}) AS `{input_['name']}`")
                    except KeyError as e:
                        query_lines.append(f"SAFE_CAST((SELECT MAX(IF(d.key = 'input_{input_count}', d.value, NULL)) FROM UNNEST(decoded_values) AS d) AS {input_['type']}) AS `input_{input_count}`")
                        input_count = input_count + 1
            for output in output_json_array:
                if output['type'].startswith('ARRAY'):
                    cast_type = input_['type'].replace('ARRAY', '').replace('<', '').replace('>', '')
                    try:
                        query_lines.append(f"(SELECT ARRAY_AGG(SAFE_CAST(item AS {cast_type})) as weights FROM UNNEST(SPLIT((SELECT MAX(IF(d.key = '{output['name']}', d.value, NULL)) FROM UNNEST(decoded_values) AS d), ',')) AS item WHERE item IS NOT NULL AND SAFE_CAST(item AS BIGNUMERIC) IS NOT NULL) AS `{output['name']}`")    
                        # query_lines.append(f"SAFE_CAST(topic{input_count} as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                    except KeyError as e:
                        query_lines.append(f"(SELECT ARRAY_AGG(SAFE_CAST(item AS {cast_type})) as weights FROM UNNEST(SPLIT((SELECT MAX(IF(d.key = 'output_{output_count}', d.value, NULL)) FROM UNNEST(decoded_values) AS d), ',')) AS item WHERE item IS NOT NULL AND SAFE_CAST(item AS BIGNUMERIC) IS NOT NULL) AS `output_{output_count}`")    
                        output_count = output_count + 1
                else:
                    try:
                        query_lines.append(f"SAFE_CAST((SELECT MAX(IF(d.key = '{output['name']}', d.value, NULL)) FROM UNNEST(decoded_values) AS d) AS {output['type']}) AS `{output['name']}`")
                    except KeyError as e:
                        query_lines.append(f"SAFE_CAST((SELECT MAX(IF(d.key = 'output_{output_count}', d.value, NULL)) FROM UNNEST(decoded_values) AS d) AS {output['type']}) AS `output_{output_count}`")
                        output_count = output_count + 1
            query_body = f"""
{left_bracket}{left_bracket}
config(
materialized='table',
schema='{name_space}',
name='{name_space}',
)
{right_bracket}{right_bracket}
WITH cte AS (
    SELECT
        tx_hash as call_tx_hash,
        `to` as contract_address,
        output as output_0,
        block_number as call_block_number,
        block_time as call_block_time,
        success as call_success,
        trace_address as call_trace_address,
        trace_id as call_trace_id,
        `error` as call_error,
        type as call_trace_type,
        `from` as trace_from_address,
        value as trace_value,
        method_id,
        udfs.DECODE_CALL_ENTRY(abi, input, output) as decoded_values
    FROM {fxn_table} AS f
    LEFT JOIN `blocktrekker.decoded_contracts.dune_abis` AS da ON da.address = f.`to`
    WHERE method_id in ({hash_ids})
    AND 
    f.`to` IN ({contract_addresses})
    AND 
    block_time >= '{current_min_ts}'
)
SELECT 
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else ''}
    call_tx_hash,
    contract_address,
    output_0,
    call_block_number,
    call_block_time,
    call_success,
    call_trace_address,
    call_trace_id,
    call_error,
    call_trace_type,
    trace_from_address,
    trace_value,
    method_id
FROM
    cte"""
            # Big Query has a 256k character limit on queries, which we trigger with large contract address lists
            # To get around this, we get an address list CTE and call that into the query 
            if len(query_body) > 250000:
                query_body = f"""
{left_bracket}{left_bracket}
config(
materialized='table',
schema='{name_space}',
name='{name_space}',
)
{right_bracket}{right_bracket}
WITH contract_addresses AS (
    SELECT
        TRIM(array_element, "[]") as contract_address
    FROM 
        {left_bracket}{left_bracket} source('decoded_contracts', 'decode_contracts') {right_bracket}{right_bracket}, 
        UNNEST(SPLIT(contract_addresses, ',')) AS array_element
    WHERE
        sub_name = '{table_name}'
),

cte AS (
    SELECT
        tx_hash as call_tx_hash,
        `to` as contract_address,
        output as output_0,
        block_number as call_block_number,
        block_time as call_block_time,
        success as call_success,
        trace_address as call_trace_address,
        trace_id as call_trace_id,
        `error` as call_error,
        type as call_trace_type,
        `from` as trace_from_address,
        value as trace_value,
        method_id,
        udfs.DECODE_CALL_ENTRY(abi, input, output) as decoded_values
    FROM {fxn_table} AS f
    LEFT JOIN `blocktrekker.decoded_contracts.dune_abis` AS da ON da.address = f.`to`
    WHERE method_id in ({hash_ids})
    AND  
    f.`to` IN (SELECT contract_address FROM (select * from contract_addresses))
    AND 
    block_time >= '{current_min_ts}'
)
SELECT 
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else ''}
    call_tx_hash,
    contract_address,
    output_0,
    call_block_number,
    call_block_time,
    call_success,
    call_trace_address,
    call_trace_id,
    call_error,
    call_trace_type,
    trace_from_address,
    trace_value,
    method_id
FROM
    cte
"""
            separate_models = count_duplicates(table_name, call_sub_name_counter)
            call_sub_name_counter.append(table_name)
            create_dbt_sql_file(query_body, table_name, name_space, separate_models)
            count_5 = count_5 + 1
            # print(count_5)
                
print("total models:")
print(count_5)
