import json
import os
import glob
from google.cloud import bigquery
from query_bigquery import query_bigquery
from create_dataset import create_dataset
import csv
import sys

# json.field_size_limit(sys.maxsize)
    
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

left_bracket = "{"
right_bracket = "}"

file_list = glob.glob('static_data/decoded_contracts*.json')
# print(file_list)

# json_string = '["{\"address\":\"0x9d9c46aca6a2c5ff6824a92d521b6381f9f8f1a9\",\"name\":\"MultiSigWalletWithDailyLimit\",\"namespace\":\"foundation\",\"created_ts\":\"2020-05-24T04:33:42Z\",\"signature\":\"MultiSigWalletWithDailyLimit_evt_Confirmation(address,uint256)\",\"type\":\"event\"}","{\"address\":\"0xab4e5b618fb8f1f3503689dfbdf801478ff6c252\",\"name\":\"MultiSigWalletWithDailyLimit\",\"namespace\":\"axieinfinity\",\"created_ts\":\"2020-02-07T18:05:04Z\",\"signature\":\"MultiSigWalletWithDailyLimit_evt_Confirmation(address,uint256)\",\"type\":\"event\"}"]'

# python_object = json.loads(json_string.replace('"{', "{").replace('}"', "}"))

# print(python_object[0]["address"])
# quit()
for file in file_list: 
    print("new_file_read: ", file)
    # Get the data for decoded_contracts from BQ
    # with open(file, 'r') as f:
    #     # Read its contents as a string
    #     contents = f.read()

    # # Split the string into individual objects
    # objects = contents.split("\n")

    #     # Remove any empty objects
    # objects = [o for o in objects if o]

    # # remove all square brackets in data objects
    # for object in objects:
    #     sub_object = json.loads(object)
    #     sub_object["data"] = sub_object["data"][1:-1]

    # # Add a square bracket at the beginning and end of the file
    # wrapped = "[" + ",".join(objects) + "]"

    # # Write the JSON data to a new file
    # with open(file, 'w') as f:
    #     json.dump(json.loads(wrapped), f, indent=4)

#     print("new_file_read")
#     # Get the data for decoded_contracts from BQ
#     with open(file, 'r') as f:
#         # Read its contents as a string
#         contents = f.read()

#     # Split the string into individual objects
#     objects = contents.split("\n")

#     # Remove any empty objects
#     objects = [o for o in objects if o]

#     # Add a square bracket at the beginning and end of the file
#     wrapped = "[" + ",".join(objects) + "]"

#     # Write the JSON data to a new file
#     with open(file, 'w') as f:
#         json.dump(json.loads(wrapped), f, indent=4)

#         # Load the JSON data
#         # print(wrapped)
    # print(file)
    with open(file, 'r') as f:
        data = json.load(f)
    count = 
    for row in data:
        data_object = json.loads(row["data"].replace('"{', "{").replace('}"', "}").replace('\\', ''))
        print(data_object[0]["address"])
        # parse JSON string in 'json_column' and replace with actual JSON object
        # test = json.load(row["data"])
        # print(test[0]["address"])
        # address = row["data"]
        # name = row["sub_name"]
        # namespace = f"""{row["namespace"]}_ethereum"""
        # data_list = json.loads(row["data"])
        # address = data_list[0]["address"]
        # print(address)

    quit()