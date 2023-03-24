import json
import csv

# change the file name and directory to change a new file
json_name = 'dune_mapping/chainlink_mapping.json'
csv_name = 'seeds/chainlink_mapping.csv'

# Load the JSON data from a file
with open(json_name) as f:
    data = json.load(f)

# Open a CSV file for writing
with open(csv_name, 'w', newline='') as f:
    writer = csv.writer(f)

    # Write the header row based on the keys in the first object
    header = list(data[0].keys())
    header.extend(['first_key', 'second_key'])
    writer.writerow(header)

    # Write each row of data
    for row in data:
        row_values = list(row.values())
        json_pairs = row['pair']  # Convert the 'docs' JSON string to a Python object
        first_key, second_key = json_pairs[0], json_pairs[1]  # Get the first and second keys from the JSON pairs
        row_values.extend([first_key, second_key])
        writer.writerow(row_values)
