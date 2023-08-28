import subprocess

# Define the number of model paths to use
num_model_paths = 10

# Define the path to your dbt project and the name of the target to run
target_name = 'prod'

# Run dbt clean
print('Running dbt clean...')
subprocess.run(['dbt', 'clean', f'--target={target_name}'])
print('dbt clean complete')


with open('dbt_project.yml', 'r') as f:
    contents = f.read()
updated_contents = contents.replace(f'model-paths: ["models_persistent"]', f'model-paths: ["models_0"]')
with open('dbt_project.yml', 'w') as f:
    f.write(updated_contents)

# Loop through the model paths
for i in range(num_model_paths):
    # Update the dbt_project.yml file to use the current model path
    print(f'Updating dbt_project.yml to use models_{i}...')
    with open('dbt_project.yml', 'r') as f:
        contents = f.read()
    updated_contents = contents.replace(f'model-paths: ["models_{i - 1 if i > 0 else i}"]', f'model-paths: ["models_{i}"]')
    with open('dbt_project.yml', 'w') as f:
        f.write(updated_contents)
    
    # Run dbt with the updated model path
    print(f'Running dbt with models_{i}...')
    final_i = i
    subprocess.run(['dbt', 'run', f'--target={target_name}', f'--exclude=AggregationRouterV4_call_uniswapV3Swap.sql', f'--exclude=models_0/oneinch_ethereum/AggregationRouterV5_call_unoswap.sql'])


with open('dbt_project.yml', 'r') as f:
    contents = f.read()
updated_contents = contents.replace(f'model-paths: ["models_{final_i}"]', f'model-paths: ["models_persistent"]')
with open('dbt_project.yml', 'w') as f:
    f.write(updated_contents)

print('Done!')

