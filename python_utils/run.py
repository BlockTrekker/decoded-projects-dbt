import shutil
import subprocess
import os

print("This script will populate a Big Query table with decoded contract data from Ethereum.")
print("it will take a few hours to run to completion.")

while True:
    user_input = input("Do you want to continue? (Y/N)   ").lower()
    if user_input == "y":
        break
    elif user_input == "n":
        print("Breaking the loop.")
        quit()
    else:
        print("Invalid input. Please enter Y or N.")

# Step 1: Empty the 'models' directory and refill with 'models_persistent'
print("ok... Starting now")
try:
    for i in range(160):
        if os.path.isdir(f'models_{i}'):
            shutil.rmtree(f'models_{i}')
    print("emptied the 'models' directories")

    for i in range(160):
        os.makedirs(f'models_{i}')
        for file in os.listdir('models_persistent'):
            if file == "sources.yml" :
               shutil.copy(f'models_persistent/{file}', f'models_{i}/{file}')
    print("refilled the 'models' directory")
except:
    print("Error removing the 'models' directory")
    exit(1)


# Step 2: Run 'python_utils/model_persistent_change_script.py'
while True:
    user_input = input("Do you want to run the model_persistent_change_script? (Y/N)   ").lower()
    if user_input == "y":
        print("running model_persistent_change_script.py")
        try:
            subprocess.run(['python', 'python_utils/model_persistent_change_script.py'], check=True)
            break
        except subprocess.CalledProcessError as e:
            print(f"Error running python: {e}")
            exit(1)
    elif user_input == "n":
        print("Skipping the model_persistent_change_script.")
        break
    else:
        print("Invalid input. Please enter Y or N.")

# Step 3: Run 'python_utils/decoded_contracts_table_creator_fresh_json.py'
print("running decoded_contracts_table_creator_fresh_json_simplify.py")
print("this will move the query results to a bucket, then download csv files to the 'static_data' directory in batches")
print("it will then create 300k+ dbt model files in the models directory")
try:
    subprocess.run(['python', 'python_utils/decoded_contracts_table_creator_fresh_json_simplify.py'], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running decoded_contracts_table_creator_fresh.py: {e}")
    exit(1)

# Step 4: Run 'dbt run --target prod' through the model_change_script.py
while True:
    user_input = input("Do you want to run the model_change_script? (Y/N)   ").lower()
    if user_input == "y":
        print("running dbt run --target prod to populate the new models in BigQuery")
        print("it will take a bit to compile the new models after printing 'Running with dbt=1.4.5' It's not frozen!")
        print("the actual running of the models in dbt will take a few hours")
        try:
            subprocess.run(['python', 'python_utils/model_change_script.py'], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running model_change_script.py: {e}")
            exit(1)
        break
    elif user_input == "n":
        print("Skipping the model_change_script.")
        break
    else:
        print("Invalid input. Please enter Y or N.")

