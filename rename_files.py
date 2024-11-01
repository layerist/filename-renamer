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
    Logs the operation based on dry_run flag.
    """
    if old_path == new_path:
        logging.debug(f'No renaming needed for: {old_path}')
        return
    
    try:
        if dry_run:
            logging.info(f'[Dry Run] Would rename: {old_path} -> {new_path}')
        else:
            os.rename(old_path, new_path)
            logging.info(f'Renamed: {old_path} -> {new_path}')
    except FileNotFoundError:
        logging.error(f'File not found: {old_path}')
    except PermissionError:
        logging.error(f'Permission denied: {old_path}')
    except OSError as e:
        logging.error(f'OS error on renaming {old_path} -> {new_path}: {e}')

def process_directory(directory_path: str, dry_run: bool = False, recursive: bool = False, file_type: Optional[str] = None) -> None:
    """
    Processes a directory, renaming files by replacing spaces with underscores.
    Logs skipped files and only renames those matching file_type if specified.
    """
    directory_path = os.path.abspath(directory_path)
    
    if not os.path.isdir(directory_path):
        raise DirectoryProcessingError(f'Directory does not exist: {directory_path}')

    logging.info(f'Processing directory: {directory_path} (Recursive: {recursive}, Dry run: {dry_run}, File type: {file_type or "all"})')

    for root, _, files in os.walk(directory_path):
        if not files:
            logging.debug(f'No files in: {root}')
            continue
        
        for filename in files:
            if file_type and not filename.endswith(file_type):
                logging.debug(f'Skipped (type mismatch): {filename}')
                continue
            
            new_filename = filename.replace(' ', '_')
            old_file_path = os.path.join(root, filename)
            new_file_path = os.path.join(root, new_filename)

            if old_file_path != new_file_path:
                rename_file(old_file_path, new_file_path, dry_run)

        if not recursive:
            break

    logging.info(f'Completed processing: {directory_path}')

def parse_arguments() -> argparse.Namespace:
    """Parses command-line arguments with validation for log level."""
    parser = argparse.ArgumentParser(description="Rename files by replacing spaces with underscores.")
    parser.add_argument('directory', type=str, help="Path to the directory to process.")
    parser.add_argument('--dry-run', action='store_true', help="Log changes without renaming files.")
    parser.add_argument('--recursive', action='store_true', help="Recursively rename files in subdirectories.")
    parser.add_argument('--log-level', type=str, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO', help="Set the logging level.")
    parser.add_argument('--file-type', type=str, help="Only rename files of the specified type (e.g., '.txt').")

    return parser.parse_args()

def main() -> None:
    """Main entry point of the script."""
    args = parse_arguments()
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    setup_logging(log_level)
    
    try:
        process_directory(args.directory, dry_run=args.dry_run, recursive=args.recursive, file_type=args.file_type)
    except DirectoryProcessingError as e:
        logging.error(f'Directory error: {e}')
    except Exception as e:
        logging.exception(f'Unexpected error: {e}')

if __name__ == "__main__":
    main()
