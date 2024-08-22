import os
import logging
import argparse

def setup_logging(level=logging.INFO):
    """Configures logging settings."""
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

def rename_file(old_path, new_path, dry_run):
    """
    Renames a file from old_path to new_path.

    Parameters:
    - old_path (str): The current path of the file.
    - new_path (str): The new path of the file.
    - dry_run (bool): If True, only logs the changes without renaming files.
    """
    if dry_run:
        logging.info(f'[Dry Run] Would rename: {old_path} to {new_path}')
    else:
        try:
            os.rename(old_path, new_path)
            logging.info(f'Successfully renamed: {old_path} to {new_path}')
        except FileNotFoundError:
            logging.error(f'File not found: {old_path}')
        except PermissionError:
            logging.error(f'Permission denied: {old_path}')
        except Exception as e:
            logging.error(f'Error renaming {old_path} to {new_path}: {e}')

def process_directory(directory_path, dry_run=False, recursive=False):
    """
    Processes a directory, renaming files by replacing spaces with underscores.

    Parameters:
    - directory_path (str): The path to the directory.
    - dry_run (bool): If True, only logs the changes without renaming files.
    - recursive (bool): If True, renames files in subdirectories as well.
    """
    if not os.path.isdir(directory_path):
        logging.error(f'The directory {directory_path} does not exist or is not a directory.')
        return

    for root, _, files in os.walk(directory_path):
        for filename in files:
            new_filename = filename.replace(' ', '_')
            old_file_path = os.path.join(root, filename)
            new_file_path = os.path.join(root, new_filename)

            if old_file_path != new_file_path:
                rename_file(old_file_path, new_file_path, dry_run)

        if not recursive:
            break

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Rename files by replacing spaces with underscores.")
    parser.add_argument('directory', type=str, help="Path to the directory to process.")
    parser.add_argument('--dry-run', action='store_true', help="Log the changes without actually renaming files.")
    parser.add_argument('--recursive', action='store_true', help="Recursively rename files in subdirectories.")

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    setup_logging()

    # Process the specified directory
    process_directory(args.directory, dry_run=args.dry_run, recursive=args.recursive)
