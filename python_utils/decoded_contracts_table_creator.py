import requests
import json
import os
import glob
from google.cloud import bigquery
# from query_scheduler import schedule_query
# from get_last_run import get_last_run_time
# from query_bigquery import query_bigquery


    
credential_path = "../keys/blocktrekker-admin.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Get the Etherscan API key
with open('../keys/etherscan_key.json') as f:
    data = json.load(f)
    etherscan_api_key = data["key"]

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

# def query_etherscan(address, etherscan_api_key):
#     response = requests.get(f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={etherscan_api_key}")
#     result = {"contract_name" : response.json()['result'][0]['ContractName'], "abi" : response.json()['result'][0]['ABI']}
#     return result

count_5 = 0
fxn_table = "`clustered_sources.clustered_traces`"
evt_table = "`clustered_sources.clustered_logs`"

project_id = "awesome-web3"
dataset_id = "decoded_contracts"
count1 = 0
amt = 0
query_body_list = []

# # Get the dune contract mapping
# with open('json_data/dune_contract_mapping.json') as f:
#     contents = f.read()
#     data = json.loads(contents)
#     dune_contract_mapping = data
#     dune_address = []
#     for val in data:
#         dune_address.append(val["Address"])

file_list = glob.glob('static_data/decoded_eth_contract_*')

for file in file_list: 
    # Get the data for decoded_contracts from BQ
    with open(file) as f:
        data = {}
        json_data = json.load(f)
        decoded_contracts = []
        decoded_addresses = []
        decoded_fxs_evts = []
        for ln in json_data:
            decoded_contracts.append(ln)
            decoded_addresses.append(ln["address"])

    # Get around the f string \ problem
    new_line = "\n" 

    for contract in decoded_contracts:
        address = contract["address"]
        name = contract["name"]
        namespace = f"""{contract["namespace"]}_ethereum"""
        dataset_creation_body = f"""
CREATE SCHEMA blocktrekker.{namespace}
  OPTIONS (
    description = 'DESCRIPTION',
    location = 'US'
  )
"""
        for evt in contract["evts"]:
            evt_id = Event(address,evt["name"],evt["inputs"])
            count = 0
            for input in evt_id.inputs:
                if input["name"] == "":
                    input["name"] = f"input_{count}"
                elif input["name"] == "from":
                    input["name"] = "from_address"
                elif input["name"] == "to":
                    input["name"] = "to_address"
                elif input["name"] == "limit":
                    input["name"] = "_limit"
                evt_id.query_lines.append(f"SAFE_CAST(topics[SAFE_OFFSET({count})] as {input['type']}) as {input['name']}")
                count = count + 1 
            evt_id.full_name = evt["signature"]
            evt_id.evt_hash = evt["evt_hash"]
            evt_id.query_body = f"""
    CREATE OR REPLACE TABLE `{project_id}.{namespace}.{evt["name"]}` 
    AS
    SELECT
        address as contract_address,
        {','.join(evt_id.query_lines) if evt_id.query_lines else ''},
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
        block_timestamp >= created_ts;"""
            
            count_5 = count_5 + 1

        for call in contract["calls"]:
            call_id = Function(address,call["name"],call["inputs"])
            count = 0
            for input in call_id.inputs:
                if input["name"] == "":
                    input["name"] = f"input_{count}"
                elif input["name"] == "from":
                    input["name"] = "from_address"
                elif input["name"] == "to":
                    input["name"] = "to_address"
                elif input["name"] == "limit":
                    input["name"] = "_limit"
                call_id.query_lines.append(f"SAFE_CAST(SUBSTRING(input, {11 + 64 * count}, {64}) as {input['type']}) as {input['name']}")
                count = count + 1 
            call_id.full_name = call["signature"]
            call_id.method_id = call["method_id"]
            call_id.query_body = f"""
    CREATE OR REPLACE TABLE `{project_id}.{namespace}.{call["name"]}`  
    AS
    SELECT 
        {','.join(call_id.query_lines) if call_id.query_lines else ''},
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
        block_timestamp >= created_ts;"""
            count_5 = count_5 + 1

    #             contract_dict[result["name"] + "_fx"] = fx_id
    #     for name in contract.evt_names:
    #         if  len(contract_dict[name].input_names) > 0:
    #             full_name = contract_dict[name].full_name
    #             query_body = contract_dict[name].query_body
    #             query_inputs = contract_dict[name].input_names
    #             query_types = contract_dict[name].input_types
    #             table_id = f"{contract_name}_{name}"
    #             # amt = amt + query_bigquery(query_body,table_id, True)
    #             # schedule_query(project_id, dataset_id, table_id, query_body, interval_min, partitioning_field, append_or_truncate)
    #             # print(count1)
    #             # count1= count1 + 1
    #             query_body_list.append(query_body)
    #             # schedule_query(project_id, dataset_id, table_id, query_body, interval_min, partitioning_field, append_or_truncate)    
                
    #     for name in contract.fx_names:
    #         if  len(contract_dict[name].input_names) > 0:
    #             full_name = contract_dict[name].full_name
    #             query_body = contract_dict[name].query_body
    #             query_inputs = contract_dict[name].input_names
    #             query_types = contract_dict[name].input_types
    #             table_id = f"{contract_name}_{name}"
    #             # amt = amt + query_bigquery(query_body,table_id, True)
    #             # schedule_query(project_id, dataset_id, table_id, query_body, interval_min, partitioning_field, append_or_truncate)
    #             # print(count1)
    #             count1 = count1 + 1
    #             query_body_list.append(query_body)
    #             # schedule_query(project_id, dataset_id, table_id, query_body, interval_min, partitioning_field, append_or_truncate)    

    # # query_body = f"{new_line}".join(query_body_list)

    # # query_bigquery(query_body,"test", True)
    # # print(query_body)
    # # schedule_query(project_id, dataset_id, table_id, query_body, interval_min, partitioning_field, append_or_truncate)    
        

print(count_5)