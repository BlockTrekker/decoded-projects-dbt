import subprocess
import re

# Define the model paths to use
model_paths = ["models_persistent", "models_persistent_1", "models_persistent_2"]

# Define the path to your dbt project and the name of the target to run
target_name = 'prod'

# Run dbt clean
print('Running dbt clean...')
subprocess.run(['dbt', 'clean', f'--target={target_name}'])
print('dbt clean complete')

# Read the dbt_project.yml file
with open('dbt_project.yml', 'r') as f:
    contents = f.read()

# Loop through the model paths
for i, model_path in enumerate(model_paths):
    # Update the dbt_project.yml file to use the current model path
    updated_contents = re.sub(r'model-paths: \[".*?"\]', f'model-paths: ["{model_path}"]', contents)
    with open('dbt_project.yml', 'w') as f:
        f.write(updated_contents)

    # Run dbt with the updated model path
    print(f'Running dbt with {model_path}...')
    subprocess.run(['dbt', 'run', f'--target={target_name}'])

# Change the model path back to models_persistent
with open('dbt_project.yml', 'r') as f:
    contents = f.read()
updated_contents = re.sub(r'model-paths: \[".*?"\]', 'model-paths: ["models_persistent"]', contents)
with open('dbt_project.yml', 'w') as f:
    f.write(updated_contents)