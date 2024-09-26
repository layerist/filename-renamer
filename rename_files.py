import os
import logging
import argparse
from typing import Optional

class DirectoryProcessingError(Exception):
    """Custom exception for directory processing errors."""
    pass

def setup_logging(level: int = logging.INFO) -> None:
    """Configures logging settings."""
    logging.basicConfig(level=level, format='%(asctime)s - %(levelname)s - %(message)s')

def rename_file(old_path: str, new_path: str, dry_run: bool) -> None:
    """
    Renames a file from old_path to new_path.

    Parameters:
    - old_path (str): The current path of the file.
    - new_path (str): The new path of the file.
    - dry_run (bool): If True, only logs the changes without renaming files.
    """
    if old_path == new_path:
        logging.debug(f'No need to rename: {old_path}')
        return
    
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
        except OSError as e:
            logging.error(f'OS error when renaming {old_path} to {new_path}: {e}')
        except Exception as e:
            logging.exception(f'Unexpected error renaming {old_path} to {new_path}: {e}')

def process_directory(directory_path: str, dry_run: bool = False, recursive: bool = False) -> None:
    """
    Processes a directory, renaming files by replacing spaces with underscores.

    Parameters:
    - directory_path (str): The path to the directory.
    - dry_run (bool): If True, only logs the changes without renaming files.
    - recursive (bool): If True, renames files in subdirectories as well.
    """
    directory_path = os.path.abspath(directory_path)
    
    if not os.path.isdir(directory_path):
        raise DirectoryProcessingError(f'The directory {directory_path} does not exist or is not a directory.')

    logging.info(f'Starting processing directory: {directory_path}, Recursive: {recursive}, Dry run: {dry_run}')

    for root, _, files in os.walk(directory_path):
        for filename in files:
            new_filename = filename.replace(' ', '_')
            old_file_path = os.path.join(root, filename)
            new_file_path = os.path.join(root, new_filename)

            if old_file_path != new_file_path:
                rename_file(old_file_path, new_file_path, dry_run)

        if not recursive:
            logging.debug('Stopping further recursion as per configuration.')
            break

    logging.info(f'Finished processing directory: {directory_path}')

def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description="Rename files by replacing spaces with underscores.")
    parser.add_argument('directory', type=str, help="Path to the directory to process.")
    parser.add_argument('--dry-run', action='store_true', help="Log the changes without actually renaming files.")
    parser.add_argument('--recursive', action='store_true', help="Recursively rename files in subdirectories.")

    return parser.parse_args()

def main() -> None:
    """Main entry point of the script."""
    args = parse_arguments()
    setup_logging()

    try:
        process_directory(args.directory, dry_run=args.dry_run, recursive=args.recursive)
    except DirectoryProcessingError as e:
        logging.error(f'Error processing directory: {e}')
    except Exception as e:
        logging.exception(f'Unexpected error: {e}')

if __name__ == "__main__":
    main()
