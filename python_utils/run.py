import shutil
import subprocess
import os


# Step 1: Run 'python_utils/model_persistent_change_script.py'
try:
    subprocess.run(['python', 'python_utils/model_persistent_change_script.py'], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running python: {e}")
    exit(1)
   
# Step 2: Run 'dbt run --target prod' through the model_change_script.py
try:
    subprocess.run(['python', 'python_utils/model_change_script.py'], check=True)
except subprocess.CalledProcessError as e:
    print(f"Error running model_change_script.py: {e}")
    exit(1)
