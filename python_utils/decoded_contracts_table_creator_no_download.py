import json
import os
import glob
from google.cloud import bigquery
from query_bigquery import query_bigquery
from create_dataset import create_dataset
import csv
import sys
from datetime import datetime
import pandas as pd
import math

    
credential_path = "../keys/blocktrekker-admin.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

pd.set_option('max_colwidth', 32000)

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
    self.input_names = []
    self.input_types = []
    self.query_lines = []

class Function:
  def __init__(self, contract_address, fx_name, inputs):
    self.contract_address = contract_address
    self.fx_name = fx_name
    self.inputs = inputs
    self.input_names = []
    self.input_types = []
    self.query_lines = []

def create_dbt_sql_file(query_body, name, namespace):
    # Create the directory
    model_num = model_num = math.floor(count_5/2500)
    os.makedirs(f"""models_{model_num}/{namespace}""", exist_ok=True)
    # Create the dbt sql file
    with open(f"""models_{model_num}/{namespace}/{name}.sql""", "w") as f:
        f.write(query_body)
    # print("Created dbt sql file")


count_5 = 0
fxn_table = "{{ source('clustered_sources', 'clustered_traces') }}"
evt_table = "{{ source('clustered_sources', 'clustered_logs') }}"

project_id = "blocktrekker"
dataset_id = "spells"
count1 = 0
amt = 0
query_body_list = []
estimated_cost = 0


left_bracket = "{"
right_bracket = "}"

file_list = glob.glob('static_data/decoded_contracts*.csv')

# open then reformat the csv files
for file in file_list: 
    dfs = []
    print("new_file_read")
    # Get the data for decoded_contracts from BQ
    # with open(file, 'r') as f:
        # Read its contents as a string

    csv_reader = pd.read_csv(file, delimiter=',', low_memory=False)
    
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
                try:
                    query_lines.append(f"SAFE_CAST(topics[SAFE_OFFSET({input_count})] as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                except KeyError as e:
                    query_lines.append(f"SAFE_CAST(topics[SAFE_OFFSET({input_count})] as {input_['type']}) as input_{input_count}")
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
    evt_hash in ({hash_ids})
AND 
    address IN ({contract_addresses})
AND 
    block_timestamp >= '{current_min_ts}'"""
                        
            # Big Query has a 256k character limit on queries, which we trigger with large contract address lists
            # To get around this, we get an address list CTE and call that into the query 
            if len(query_body) > 250000:
                query_body = f"""
{left_bracket}{left_bracket}
config(
    materialized='view',
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
)

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
    evt_hash in ({hash_ids})
AND 
    address IN (SELECT * FROM contract_addresses)
AND 
    block_timestamp >= '{current_min_ts}'"""
            create_dbt_sql_file(query_body, table_name, name_space)
            count_5 = count_5 + 1
    
        if row["type"] == "call":
            input_count = 0
            output_count = 0
            query_lines = []
            output_query_sql = []
            for input_ in input_json_array:
                try:        
                    query_lines.append(f"SAFE_CAST(SUBSTRING(input, {11 + 64 * input_count}, {64}) as {input_['type']}) as `{input_['name']}`")
                except KeyError as e:
                    query_lines.append(f"SAFE_CAST(SUBSTRING(input, {11 + 64 * input_count}, {64}) as {input_['type']}) as input_{input_count}")
                input_count = input_count + 1
            for output in output_json_array:
                output_query_sql.append(f"SAFE_CAST(SUBSTRING(output, {64 * output_count}, {64}) as {output['type']}) as {output['name']}")
                output_count = output_count + 1
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
    LEFT(input,10) in ({hash_ids})
AND 
    to_address IN ({contract_addresses})
AND 
    block_timestamp >= '{current_min_ts}'"""
            # Big Query has a 256k character limit on queries, which we trigger with large contract address lists
            # To get around this, we get an address list CTE and call that into the query 
            if len(query_body) > 250000:
                query_body = f"""
{left_bracket}{left_bracket}
config(
materialized='view',
schema='blocktrekker',
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
)
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
    LEFT(input,10) in ({hash_ids})
AND 
    to_address IN (SELECT contract_address FROM (select * from contract_addresses))
AND 
    block_timestamp >= '{current_min_ts}'"""
            create_dbt_sql_file(query_body, table_name, name_space)
            count_5 = count_5 + 1
            print(count_5)
                
print("total models:")
print(count_5)