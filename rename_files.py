import os
import logging

def rename_files_in_directory(directory_path, dry_run=False, recursive=False):
    """
    Renames files in the specified directory by replacing spaces with underscores.

    Parameters:
    - directory_path (str): The path to the directory.
    - dry_run (bool): If True, only logs the changes without renaming files.
    - recursive (bool): If True, renames files in subdirectories as well.
    """
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if not os.path.exists(directory_path):
        logging.error(f'The directory {directory_path} does not exist.')
        return

    for root, _, files in os.walk(directory_path):
        for filename in files:
            new_filename = filename.replace(' ', '_')
            old_file_path = os.path.join(root, filename)
            new_file_path = os.path.join(root, new_filename)

            if dry_run:
                logging.info(f'Dry run - would rename: {old_file_path} to {new_file_path}')
            else:
                try:
                    os.rename(old_file_path, new_file_path)
                    logging.info(f'Renamed: {old_file_path} to {new_file_path}')
                except Exception as e:
                    logging.error(f'Error renaming {old_file_path} to {new_file_path}: {e}')

        if not recursive:
            break

# Example usage
if __name__ == "__main__":
    directory_path = 'path_to_your_directory'
    rename_files_in_directory(directory_path, dry_run=True, recursive=True)
