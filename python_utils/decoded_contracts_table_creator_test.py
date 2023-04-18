import json
import os
import glob
from google.cloud import bigquery
from query_bigquery import query_bigquery
from create_dataset import create_dataset
import csv
import sys
from datetime import datetime

# json.field_size_limit(sys.maxsize)
    
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
    os.makedirs(f"""models/{namespace}""", exist_ok=True)
    # Create the dbt sql file
    with open(f"""models/{namespace}/{name}.sql""", "w") as f:
        f.write(query_body)
    # print("Created dbt sql file")

# def query_etherscan(address, etherscan_api_key):
#     response = requests.get(f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={etherscan_api_key}")
#     result = {"contract_name" : response.json()['result'][0]['ContractName'], "abi" : response.json()['result'][0]['ABI']}
#     return result

count_5 = 0
fxn_table = "`clustered_sources.clustered_traces`"
evt_table = "`clustered_sources.clustered_logs`"

project_id = "blocktrekker"
dataset_id = "spells"
count1 = 0
amt = 0
query_body_list = []
estimated_cost = 0


left_bracket = "{"
right_bracket = "}"

file_list = glob.glob('static_data/decoded_contracts*.csv')
# print(file_list)

# json_string = '["{\"address\":\"0x9d9c46aca6a2c5ff6824a92d521b6381f9f8f1a9\",\"name\":\"MultiSigWalletWithDailyLimit\",\"namespace\":\"foundation\",\"created_ts\":\"2020-05-24T04:33:42Z\",\"signature\":\"MultiSigWalletWithDailyLimit_evt_Confirmation(address,uint256)\",\"type\":\"event\"}","{\"address\":\"0xab4e5b618fb8f1f3503689dfbdf801478ff6c252\",\"name\":\"MultiSigWalletWithDailyLimit\",\"namespace\":\"axieinfinity\",\"created_ts\":\"2020-02-07T18:05:04Z\",\"signature\":\"MultiSigWalletWithDailyLimit_evt_Confirmation(address,uint256)\",\"type\":\"event\"}"]'

# python_object = json.loads(json_string.replace('"{', "{").replace('}"', "}"))

# print(python_object[0]["address"])
# quit()
# open then reformat the csv files
for file in file_list: 
    print("new_file_read")
    # Get the data for decoded_contracts from BQ
    with open(file, 'r') as f:
        # Read its contents as a string
        csv_reader = csv.DictReader(f)

        for row in csv_reader:
            table_name = row["sub_name"]
            name_space = f"{row['namespace']}_ethereum"
            type = row["type"]
            hash_id = row["hash_id"]
            input_json = json.loads(f"[{row['inputs']}]")
            output_json = json.loads(f"[{row['outputs']}]")
            contract_details = json.loads(row["contract_details"])
            contract_addresses_sql = []
            current_min_ts = datetime.now().timestamp()
            for contract in contract_details:
                contract = json.loads(contract)
                print(contract["address"])
                query_lines = []
                contract_addresses_sql.append(contract["address"])      
                current_min_ts =  min(current_min_ts, datetime.strptime(contract["created_ts"], '%Y-%m-%dT%H:%M:%SZ').timestamp())
            if row["type"] == "event":
                input_count = 0
                for input in input_json:
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
    address IN ("{'","'.join(contract_addresses_sql)}")
AND 
    block_timestamp >= '{datetime.fromtimestamp(current_min_ts)}'"""
                        
            create_dbt_sql_file(query_body, table_name, name_space)
                # estimated_cost = estimated_cost + query_bigquery(evt_id.query_body)
            count_5 = count_5 + 1
        
            if row["type"] == "call":
                input_count = 0
                output_count = 0
                query_lines = []
                output_query_sql = []
                for input in input_json:
                    query_lines.append(f"SAFE_CAST(SUBSTRING(input, {11 + 64 * input_count}, {64}) as {input['type']}) as {input['name']}")
                    input_count = input_count + 1 
                for output in output_json:
                    output_query_sql.append(f"SAFE_CAST(SUBSTRING(output, {11 + 64 * output_count}, {64}) as {output['type']}) as {output['name']}")
                    output_count = output_count + 1
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
    block_timestamp >= '{datetime.fromtimestamp(current_min_ts)}'"""
                
                create_dbt_sql_file(query_body, table_name, name_space)
                count_5 = count_5 + 1
                

print(count_5)
print(f"total:{estimated_cost}")