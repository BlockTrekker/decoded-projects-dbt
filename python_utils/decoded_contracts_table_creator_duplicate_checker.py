import json
import os
import glob
from google.cloud import bigquery
from query_bigquery import query_bigquery
from create_dataset import create_dataset
    
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

def find_duplicates(strings_list):
    unique_strings = set()
    duplicates = set()
    for string in strings_list:
        if string in unique_strings:
            duplicates.add(string)
        else:
            unique_strings.add(string)
    return duplicates

count_5 = 0
fxn_table = "`clustered_sources.clustered_traces`"
evt_table = "`clustered_sources.clustered_logs`"

project_id = "blocktrekker"
dataset_id = "spells"
count1 = 0
amt = 0
query_body_list = []
estimated_cost = 0

schema_dict = {
    "uint32[]":"INT64",
    "uint16[]":"INT64",
    "uint8[]":"INT64", 
    "uint64[]": "INT64",
    "uint128[]": "INT64",
    "uint256[]": "BIGNUMERIC",
    "bool[]":"BOOL",
    "address[]":"STRING",
    "string[]":"STRING",
    "bytes[]":"BYTES",
    "bytes4":"BYTES",
    "bytes32":"BYTES",
    "uint32":"INT64",
    "uint16":"INT64",
    "uint8":"INT64", 
    "uint64": "INT64",
    "unit80": "INT64",
    "uint112": "INT64",
    "uint128": "INT64",
    "uint168": "BIGNUMERIC",
    "uint256": "BIGNUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "bool":"BOOL",
    "address":"STRING",
    "STRING": "STRING",
    "string":"STRING",
    "bytes":"BYTES"} 

file_list = glob.glob('static_data/decoded_eth_contract_*')

for file in file_list: 
    # Get the data for decoded_contracts from BQ
    with open(file) as f:
        data = {}
        json_data = json.load(f)
        name_list = []
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
        created_ts = contract["created_ts"]

        # estimated_cost = estimated_cost + create_dataset(dataset_creation_body)

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
                elif input["name"] == "all":
                    input["name"] = "_all"
                if input["type"] not in schema_dict:
                    if input["type"][:4].lower().startswith("uint"):
                        input["type"] = "BIGNUMERIC"
                    else:
                        input["type"] = "STRING"
                evt_id.query_lines.append(f"SAFE_CAST(topics[SAFE_OFFSET({count})] as {schema_dict[input['type']]}) as {input['name']}")
                count = count + 1 
            evt_id.full_name = evt["signature"]
            evt_id.evt_hash = evt["evt_hash"]
            name_list.append(evt["name"])
            
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
                elif input["name"] == "all":
                    input["name"] = "_all"                    
                if input["type"] not in schema_dict:
                    if input["type"][:4].lower().startswith("uint"):
                        input["type"] = "BIGNUMERIC"
                    else:
                        input["type"] = "STRING"
                call_id.query_lines.append(f"SAFE_CAST(SUBSTRING(input, {11 + 64 * count}, {64}) as {schema_dict[input['type']]}) as {input['name']}")
                count = count + 1 
            call_id.full_name = call["signature"]
            call_id.method_id = call["method_id"]
            name_list.append(call["name"])

duplicates = find_duplicates(name_list)
print("Duplicate strings:", duplicates)