import csv
import json
import glob


file_list = glob.glob('static_data/decoded_contracts/decoded_eth_contracts00000000000*')
print(file_list)
for file in file_list:
    csv_file = file
    json_file = f'static_data/decoded_eth_contract_{file[-7:-4]}.json'
    # Open the CSV file for reading
    with open(csv_file, 'r') as csvfile:
        # Create a CSV reader object
        reader = csv.reader(csvfile)
        headers = next(reader)

        # Create a list of dictionaries for each row
        rows = []
        for row in reader:
            d = {"address": row[0], "evts": json.loads(row[1]), "calls": json.loads(row[2])}
            rows.append(d)

        # # Create a JSON object
        # data = {"data": rows}
    

    # Dump the data dictionary to a JSON file
    with open(json_file, 'w') as jsonfile:
        json.dump(rows, jsonfile)