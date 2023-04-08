import glob
import os
import pandas


# These were actually csv files downloaded from bq with a json extension.
# This changes the extension to csv.
# file_list = glob.glob('json_data/decoded_contracts/decoded_eth_contracts*.json')

# for file in file_list:
#     new_file_name = f"{os.path.splitext(file)[0]}.csv"
#     os.rename(file, new_file_name)


# Now combine the csv files into one csv file 

output_file = 'json_data/decoded_eth_contracts_all.json'
file_list = glob.glob('json_data/decoded_contracts/decoded_eth_contracts*')

# combine all csv files into a single dataframe
combined_df = pandas.concat([pandas.read_csv(file) for file in file_list])
# export dataframe to csv
combined_df.to_csv(output_file, index=False)