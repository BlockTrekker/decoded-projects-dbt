
import json

with open('json_data/dune_contract_mapping.json') as f:
    contents = f.read()
    data = json.loads(contents)
    dune_address = []
    for val in data:
        dune_address.append(val["Address"])
                
    # print(dune_address)
    
with open('json_data/decoded_contracts.json') as f:
    decoded_contracts = {}
    decoded_addresses = []
    decoded_fxs_evts = []
    for ln in f:
        data = json.loads(ln)
        decoded_contracts[data["address"]] = data
        decoded_addresses.append(data["address"])
        
with open('json_data/decoded_contracts.json') as f:
    decoded_contracts = []
    decoded_addresses = []
    decoded_fxs_evts = []
    for ln in f:
        data = json.loads(ln)
        decoded_contracts.append(data)
        decoded_addresses.append(data["address"])
    

decoded_addresses_set = set(decoded_addresses)
non_decoded_addresses = [address for address in dune_address if address not in decoded_addresses_set]


print("nondecoded    " + str(len(non_decoded_addresses)))
print("decoded    " + str(len(decoded_addresses)))
print("dune_addresses    " + str(len(dune_address)))

# # Retrieve all the decoded contracts
# print(decoded_contracts[0]["address"])

# # Print the result
# print(len(decoded_addresses))

# print(decoded_addresses)
