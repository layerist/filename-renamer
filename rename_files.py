import os

def rename_files_in_directory(directory_path):
    # List all files in the directory
    for filename in os.listdir(directory_path):
        # Create a new filename by replacing spaces with underscores
        new_filename = filename.replace(' ', '_')
        # Construct full file paths
        old_file_path = os.path.join(directory_path, filename)
        new_file_path = os.path.join(directory_path, new_filename)
        # Rename the file
        os.rename(old_file_path, new_file_path)
        print(f'Renamed: {filename} to {new_filename}')

# Specify the directory path
directory_path = 'path_to_your_directory'

# Call the function to rename files
rename_files_in_directory(directory_path)
