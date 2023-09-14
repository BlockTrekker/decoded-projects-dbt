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
import re


credential_path = "../keys/blocktrekker-admin.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path

# Constants:
call_cte_lines ="""        tx_hash as call_tx_hash,
        `to` as contract_address,
        block_number as call_block_number,
        block_time as call_block_time,
        success as call_success,
        trace_address as call_trace_address,
        trace_id as call_trace_id,
        `error` as call_error,
        type as call_trace_type,
        `from` as trace_from_address,
        value as trace_value,
        method_id,"""
call_main_lines ="""    call_tx_hash,
    contract_address,
    call_block_number,
    call_block_time,
    call_success,
    call_trace_address,
    call_trace_id,
    call_error,
    call_trace_type,
    trace_from_address,
    trace_value,
    method_id"""
event_cte_lines = """        contract_address,
        block_number as evt_block_number,
        block_time as evt_block_time,
        `index` as evt_index,
        tx_hash as evt_tx_hash,
        tx_index as evt_tx_index,
        evt_hash,"""
event_main_lines ="""contract_address,
    evt_block_number,
    evt_block_time,
    evt_index,
    evt_tx_hash,
    evt_tx_index,
    evt_hash,"""


def process_json_field(row_field, default_value):
    """Process a JSON field from a CSV row."""
    try:
        if row_field == "[null]" or row_field == '[""]':
            return default_value
        else:
            return json.loads(f'[{json.loads(row_field)[0]}]')
    except json.decoder.JSONDecodeError as e:
        print(e)
        print(f'Field: {row_field}')
        return default_value

def process_csv_row(row):
    """Process each row from the CSV file."""
    table_name = row["sub_name"].replace(".", "_").replace("++", "")
    name_space = f"{row['namespace']}_ethereum"
    type = row["type"]
    hash_ids = row["hash_ids"][1:-1]
    contract_addresses = row['contract_addresses'][1:-1]
    current_min_ts = row['min_created_ts']
    
    input_default = []
    output_default = [{"name": "output_0", "type": "STRING"}]
    
    input_json_array = process_json_field(row.get('inputs_', None), input_default)
    output_json_array = process_json_field(row.get('outputs', None), output_default)

    
    
    return table_name, name_space, type, hash_ids, contract_addresses, current_min_ts, input_json_array, output_json_array


# Generates the `config` section
def generate_config(name_space):
    return f"""
{{{{
    config(
        materialized='table',
        schema='{name_space}',
        name='{name_space}',
    )
}}}}

WITH"""

# Generates the table for large sql functions 
def generate_contract_addresses_cte(table_name):
    return f"""
contract_addresses AS (
    SELECT
        SUBSTR(TRIM(array_element, "[]"), 2, LENGTH(TRIM(array_element, "[]")) - 2) AS contract_address
    FROM 
        {{{{ source('decoded_contracts', 'decode_contracts') }}}}, 
        UNNEST(SPLIT(contract_addresses, ',')) AS array_element
    WHERE
        sub_name = '{table_name}'
)"""

# Generates the iface_extract section which shrinks the ABI to only include the function or event of interest
def generate_iface_extract(table_name, type):
    if type == 'call':
        udf = 'REDUCE_ABI_FOR_FUNCTION_NAME'
        split = 'call_'
    else:
        udf = 'REDUCE_ABI_FOR_EVENT_NAME'    
        split = 'evt_'
    return f"""
, iface_extract AS (
SELECT
    udfs.{udf}(abi, "{table_name.rsplit(split, 1)[-1]}") as  iface,
FROM `blocktrekker.decoded_contracts.dune_abis`
    WHERE address IN (SELECT * FROM contract_addresses)
    AND abi LIKE '%"name":"{table_name.rsplit(split, 1)[-1]}"%'
LIMIT 1
)"""
    

def json_array_from_array(array_string, input_name, index_type, parent_path=None, count=0):
    # Extract the content within the angle brackets, case-insensitively
    match = re.search(r'(?i)ARRAY<(.+)>', array_string)
    if not match:
        return None

    # If parent_path is not specified, set it to its default value
    if parent_path is None:
        if index_type == "input":
            parent_path = f"$.input.{input_name}"
        elif index_type == "output":
            parent_path = f"$.output.{input_name}"
        else:
            parent_path = f"$.{index_type}"

    content = match.group(1)
    sections = content.split(", ")

    if len(sections) == 1 & count == None:
        # Handle the case where the array is a single type
        name, data_type = sections[0].split(" ")
        query = f"\n    JSON_VALUE_ARRAY(decoded_values, '{parent_path}.{name}') AS `{name}`"
        return query
    if count == 0:
        as_string = "\n        AS `" + input_name + "`"
        padding = "\n"
    else:
        as_string = ""
        padding = "    "
    count = count + 1
    query_lines = []

    for section in sections:
        # Handle nested arrays
        nested_match = re.search(r'(?i)ARRAY<', section)
        if nested_match:
            nested_array_end = section.rfind(">")
            name = section[:nested_match.start()].strip()
            nested_array = section[nested_match.start():nested_array_end + 1]
            new_parent_path = f"{parent_path}.{name}"
            nested_query = json_array_from_array(nested_array, input_name, index_type, parent_path=new_parent_path, count=count)
            if nested_query:
                query_lines.append(f"'{name}', {nested_query}")
        else:
            try:
                name, data_type = section.split(" ")
            except ValueError:
                print(f"Error parsing array: {array_string}, section: {section}, parent_path: {parent_path}")
                continue
            line = f"    '{name}', JSON_VALUE_ARRAY(decoded_values, '{parent_path}.{name}')"
            query_lines.append(line)


    query_lines_joined = f",{padding}    ".join(query_lines)
    query = padding + "    JSON_OBJECT(\n    " + query_lines_joined + ")" + as_string
    return query

def create_dbt_sql_file(query_body, table_name, namespace, separate_models):
    # Create the directory
    model_num = model_num = math.floor(count_5/2500) + separate_models

    os.makedirs(f"""models_{model_num}/{namespace}""", exist_ok=True)
    # Create the dbt sql file
    with open(f"""models_{model_num}/{namespace}/{table_name}.sql""", "w") as f:
        f.write(query_body)

def count_duplicates(string, string_list):
    counts = Counter(string_list)
    return counts[string]

# Your main functions now become shorter and clearer
def create_query_body_sql(type, name_space, hash_ids, current_min_ts, query_lines, table_name, arrays):

    if type == 'call':
        table_type = "{{ source('ethereum', 'traces') }}"
        cte_lines = call_cte_lines
        main_lines = call_main_lines
        decode_fx = 'DECODE_CALL_ENTRY'
        where_clause = f"""WHERE
        method_id in ({hash_ids}) 
    AND `to` IN (SELECT * FROM contract_addresses)
    AND block_time >= '{current_min_ts}'"""
        from_line = f"""CASE 
            WHEN success = TRUE THEN udfs.{decode_fx}(`to`, tx_hash, iface, input, output) 
            ELSE NULL 
            END as decoded_values
    FROM {table_type} AS f
    CROSS JOIN iface_extract"""
        # if the type is event
    
    else:
        table_type = "{{ source('ethereum', 'logs') }}"
        decode_fx = 'DECODE_LOG_ENTRY(topic0, topic1, topic2, topic3, data, iface)'
        cte_lines = event_cte_lines
        main_lines = event_main_lines
        where_clause = f"""WHERE
    evt_hash in ({hash_ids}) 
AND 
    contract_address IN (SELECT * FROM contract_addresses)
AND 
    block_time >= '{current_min_ts}'"""
        from_line = f"""udfs.{decode_fx} as decoded_values
        FROM {table_type} AS f
        CROSS JOIN iface_extract"""

    if arrays == True:
        iface_extract = generate_iface_extract(table_name, type)
        cte = f""", cte as (
    SELECT
{cte_lines}
        {from_line}
        {where_clause}
    )"""    
        main = f"""SELECT
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else ''}
    {main_lines}
    FROM cte"""

    else:
        cte = ""
        iface_extract = ""
        from_line = f"""FROM
    {table_type}
    {where_clause}"""
        main = f"""SELECT
    {','.join(query_lines) + ',' if query_lines != [] and query_lines != '' else ''}
{cte_lines}
{from_line}"""

    query_body = f"""
{generate_config(name_space)}
{generate_contract_addresses_cte(table_name)}
{iface_extract}
{cte}
{main}
"""
    return query_body

# Start the non-functions
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
        print("Ok using local data")
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
else:
    local_dir_path = 'static_data'

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

project_id = "blocktrekker"
dataset_id = "spells"
count1 = 0
amt = 0

# open then reformat the csv files
for file in file_list: 
    dfs = []
    print("new_file_read")
    new_line = "/n"
    
    csv_reader = pd.read_csv(file, delimiter=',', low_memory=False)
    evt_sub_name_counter = []
    call_sub_name_counter = []
    
    # Your main loop
    for index, row in csv_reader.iterrows():
        table_name, name_space, type, hash_ids, contract_addresses, current_min_ts, input_json_array, output_json_array = process_csv_row(row)
        input_count = 0
        output_count = 0
        query_lines = []
        output_query_sql = []
        for input_ in input_json_array:
            if type == 'call':
                input_['index_type'] = 'input'

        if any(input_['type'].startswith('ARRAY') for input_ in input_json_array) or any(output_['type'].startswith('ARRAY') for output_ in output_json_array):
        # There's at least one input of type 'ARRAY' this will impact the query body
            arrays = True
            for input_ in input_json_array:
                if input_['type'].startswith('ARRAY'):
                    cast_type = input_['type'].replace('ARRAY', '').replace('<', '').replace('>', '')
                    json_object = json_array_from_array(input_['type'], input_['name'],input_['index_type'])
                    query_lines.append(json_object) 
                else:
                    try:
                        query_lines.append(f"\n    JSON_VALUE(decoded_values, '$.{input_['index_type']}.{input_['name']}') AS `{input_['name']}`")
                    except KeyError as e:
                        query_lines.append(f"\n    JSON_VALUE(decoded_values, '$.{input_['index_type']}.{input_['name']}') AS `input_{input_count}`")
                        input_count = input_count + 1
            if type == 'call':
                for output in output_json_array:
                    if output['type'].startswith('ARRAY'):
                        cast_type = output['type'].replace('ARRAY', '').replace('<', '').replace('>', '')
                        json_object = json_array_from_array(output['type'], output['name'],'output')
                        query_lines.append(json_object) 
                else:
                    try:
                        query_lines.append(f"\n    JSON_VALUE(decoded_values, '$.output.{output['name']}') AS `{output['name']}`")
                    except KeyError as e:
                        query_lines.append(f"\n    JSON_VALUE(decoded_values, '$.output.{output['name']}') AS `output_{output_count}`")
                        output_count = output_count + 1
        else:
            arrays = False
            topic_count = 0
            for input_ in input_json_array:
                if input_['index_type'] == 'topic':
                    if input_['type'] == 'BIGNUMERIC' or input_['type'] == 'INT64':
                        try:
                            query_lines.append(f"\n    SAFE_CAST(udfs.hexToInt({input_['index_type']}{str(topic_count)}) as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST(udfs.hexToInt({input_['index_type']}{str(topic_count)}) as as {input_['type']}) as input_{input_count}\n")
                    elif input_['type'] == 'ADDRESS':
                        try: 
                            query_lines.append(f"\n    SAFE_CAST(CONCAT('0x', RIGHT({input_['index_type']}{str(topic_count)}, 40)) as STRING) as `{input_['name'].replace('_partition', 'partition')}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST(CONCAT('0x', RIGHT({input_['index_type']}{str(topic_count)}, 40)) as STRING) as input_{input_count}\n")
                    elif input_['type'] == 'BOOL':
                        try:
                            query_lines.append(f"\n    SAFE_CAST(SAFE_CAST(udfs.hexToInt({input_['index_type']}{str(topic_count)}) as INT) as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST(SAFE_CAST(udfs.hexToInt({input_['index_type']}{str(topic_count)}) as INT) as {input_['type']}) as input_{input_count}\n")
                    else:
                        try:
                            query_lines.append(f"\n    SAFE_CAST({input_['index_type']}{str(topic_count)} as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST({input_['index_type']}{str(topic_count)} as {input_['type']}) as input_{input_count}")
                    topic_count = topic_count + 1
                else: 
                    if input_['type'] == 'BIGNUMERIC' or input_['type'] == 'INT64':
                        try: 
                            query_lines.append(f"\n    SAFE_CAST(udfs.hexToInt(CONCAT('0x' ,SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}))) as {input_['type']}) as `{input_['name']}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST(udfs.hexToInt(CONCAT('0x', SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}))) as {input_['type']}) as input_{input_count}\n")
                    elif input_['type'] == 'ADDRESS':
                        try: 
                            query_lines.append(f"\n    SAFE_CAST(CONCAT('0x', RIGHT(SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}), 40)) as STRING) as `{input_['name'].replace('_partition', 'partition')}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST(CONCAT('0x', RIGHT(SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}), 40)) as STRING) as input_{input_count}\n")
                    elif input_['type'] == 'BOOL':
                        try: 
                            query_lines.append(f"\n    SAFE_CAST(SAFE_CAST(udfs.hexToInt(CONCAT('0x' ,SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}))) as INT) as BOOL) as `{input_['name'].replace('_partition', 'partition')}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST(SAFE_CAST(udfs.hexToInt(CONCAT('0x' ,SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}))) as INT) as BOOL) as input_{input_count}\n")
                    else:
                        try: 
                            query_lines.append(f"\n    SAFE_CAST(SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}) as {input_['type']}) as `{input_['name'].replace('_partition', 'partition')}`")
                        except KeyError as e:
                            query_lines.append(f"\n    SAFE_CAST(SUBSTRING({input_['index_type']}, {3 + 64 * input_count}, {64}) as {input_['type']}) as input_{input_count}\n")
                    input_count = input_count + 1
            if type == 'call':
                for output_ in output_json_array:
                    print("output name: " + output_['name'])
                    if output_['type'] == 'BIGNUMERIC' or input_['type'] == 'INT64':
                        query_lines.append(f"\n    SAFE_CAST(udfs.hexToInt(CONCAT('0x', SUBSTRING(output, {3 + 64 * output_count}, {64}))) as {output_['type']}) as output_{'0' if output_['name'] == 'output_0' else output_['name']}")
                        output_count = output_count + 1
                    elif output_['type'] == 'ADDRESS':
                        query_lines.append(f"\n    SAFE_CAST(CONCAT('0x',RIGHT(SUBSTRING(output, {3 + 64 * output_count}, {64}), 40)) as STRING) as output_{'0' if output_['name'] == 'output_0' else output_['name']}")
                        output_count = output_count + 1
                    elif output_['type'] == 'BOOL':
                        query_lines.append(f"\n    SAFE_CAST(SAFE_CAST(udfs.hexToInt(CONCAT('0x', SUBSTRING(output, {3 + 64 * output_count}, {64}))) as INT) as {output_['type']}) as output_{'0' if output_['name'] == 'output_0' else output_['name']}")
                        output_count = output_count + 1
                    else:
                        query_lines.append(f"\n    SAFE_CAST(SUBSTRING(output, {3 + 64 * output_count}, {64}) as {output_['type']}) as output_{'0' if output_['name'] == 'output_0' else output_['name']}")
                        output_count = output_count + 1
        query_body = create_query_body_sql(type, name_space, hash_ids, current_min_ts, query_lines, table_name, arrays)
        separate_models = count_duplicates(table_name, evt_sub_name_counter)
        evt_sub_name_counter.append(table_name)
        create_dbt_sql_file(query_body, table_name, name_space, separate_models)
        count_5 = count_5 + 1

                
print("total models:")
print(count_5)

