import os

# Define the directory to count files in
directory_path = "models"

# Define a function to count the number of files in a directory and its subdirectories
def count_files(directory_path):
    file_count = 0
    for root, dirs, files in os.walk(directory_path):
        file_count += len(files)
    return file_count

# Call the count_files() function and print the result
num_files = count_files(directory_path)
print(f"Number of files in directory '{directory_path}': {num_files}")