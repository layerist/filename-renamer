import os
import logging
import argparse
import time
from typing import Optional


class DirectoryProcessingError(Exception):
    """Custom exception for errors during directory processing."""
    pass


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configures the logging system.
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def rename_file(old_path: str, new_path: str, dry_run: bool) -> None:
    """
    Renames a file, replacing spaces with underscores, and logs the action.
    """
    if old_path == new_path:
        logging.debug(f"Skipping (No change needed): {old_path}")
        return

    try:
        if dry_run:
            logging.info(f"[Dry Run] Rename: {old_path} -> {new_path}")
        else:
            os.rename(old_path, new_path)
            logging.info(f"Renamed: {old_path} -> {new_path}")
    except OSError as e:
        logging.error(f"Error renaming {old_path} -> {new_path}: {e}")


def process_directory(directory_path: str, dry_run: bool = False, recursive: bool = False, file_type: Optional[str] = None) -> None:
    """
    Processes a directory to rename files by replacing spaces with underscores.
    """
    start_time = time.time()
    directory_path = os.path.abspath(directory_path)

    if not os.path.isdir(directory_path):
        raise DirectoryProcessingError(f"Invalid directory path: {directory_path}")

    file_type = file_type.lstrip('.') if file_type else None

    logging.info(f"Processing: {directory_path} (Recursive: {recursive}, Dry run: {dry_run}, File type: {file_type or 'All'})")

    for root, _, files in os.walk(directory_path):
        for filename in files:
            if filename.startswith('.') or (file_type and not filename.endswith(f'.{file_type}')):
                logging.debug(f"Skipping: {filename}")
                continue

            new_filename = filename.replace(" ", "_")
            old_file_path = os.path.join(root, filename)
            new_file_path = os.path.join(root, new_filename)

            rename_file(old_file_path, new_file_path, dry_run)

        if not recursive:
            break

    logging.info(f"Finished processing in {time.time() - start_time:.2f}s")


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Batch rename files by replacing spaces with underscores.")
    parser.add_argument('directory', type=str, help="Path to the directory to process.")
    parser.add_argument('--dry-run', action='store_true', help="Simulate renaming without making changes.")
    parser.add_argument('--recursive', action='store_true', help="Process files in subdirectories recursively.")
    parser.add_argument('--log-level', type=str.upper, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='INFO', help="Set the logging level (default: INFO).")
    parser.add_argument('--file-type', type=str, help="Restrict renaming to files of this type (e.g., 'txt' for .txt files).")
    return parser.parse_args()


def main() -> None:
    """
    Main function to run the script.
    """
    args = parse_arguments()
    setup_logging(getattr(logging, args.log_level, logging.INFO))

    try:
        process_directory(args.directory, args.dry_run, args.recursive, args.file_type)
    except DirectoryProcessingError as e:
        logging.error(f"Directory error: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
