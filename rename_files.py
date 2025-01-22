import os
import logging
import argparse
from typing import Optional


class DirectoryProcessingError(Exception):
    """Custom exception for errors during directory processing."""
    pass


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configures the logging system.

    Args:
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def rename_file(old_path: str, new_path: str, dry_run: bool) -> None:
    """
    Renames a file, replacing spaces with underscores, and logs the action.

    Args:
        old_path (str): Original file path.
        new_path (str): Desired file path.
        dry_run (bool): If True, simulates the action without making changes.
    """
    if old_path == new_path:
        logging.debug(f"Skipping: No renaming needed for {old_path}")
        return

    try:
        if dry_run:
            logging.info(f"[Dry Run] Would rename: {old_path} -> {new_path}")
        else:
            os.rename(old_path, new_path)
            logging.info(f"Renamed: {old_path} -> {new_path}")
    except OSError as e:
        logging.error(f"Error renaming {old_path} -> {new_path}: {e}")


def process_directory(
    directory_path: str,
    dry_run: bool = False,
    recursive: bool = False,
    file_type: Optional[str] = None
) -> None:
    """
    Processes a directory to rename files by replacing spaces with underscores.

    Args:
        directory_path (str): Path to the directory to process.
        dry_run (bool): If True, logs actions without performing them.
        recursive (bool): If True, processes subdirectories recursively.
        file_type (Optional[str]): Restrict renaming to files with this extension.
    """
    directory_path = os.path.abspath(directory_path)

    if not os.path.exists(directory_path):
        raise DirectoryProcessingError(f"Path does not exist: {directory_path}")
    if not os.path.isdir(directory_path):
        raise DirectoryProcessingError(f"Not a directory: {directory_path}")

    logging.info(
        f"Starting directory processing: {directory_path} "
        f"(Recursive: {recursive}, Dry run: {dry_run}, File type: {file_type or 'all'})"
    )

    for root, _, files in os.walk(directory_path):
        for filename in files:
            if filename.startswith('.'):
                logging.debug(f"Skipping hidden file: {filename}")
                continue

            if file_type and not filename.endswith(file_type):
                logging.debug(f"Skipping due to file type mismatch: {filename}")
                continue

            new_filename = filename.replace(' ', '_')
            old_file_path = os.path.join(root, filename)
            new_file_path = os.path.join(root, new_filename)

            rename_file(old_file_path, new_file_path, dry_run)

        if not recursive:
            break

    logging.info(f"Finished processing directory: {directory_path}")


def parse_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments for the script.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Rename files by replacing spaces with underscores."
    )
    parser.add_argument(
        'directory',
        type=str,
        help="Path to the directory to process."
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help="Simulate renaming without making changes."
    )
    parser.add_argument(
        '--recursive',
        action='store_true',
        help="Process files in subdirectories recursively."
    )
    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help="Set the logging level (default: INFO)."
    )
    parser.add_argument(
        '--file-type',
        type=str,
        help="Restrict renaming to files of this type (e.g., '.txt')."
    )

    return parser.parse_args()


def main() -> None:
    """
    Main function to run the script.
    """
    args = parse_arguments()
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    setup_logging(log_level)

    try:
        process_directory(
            directory_path=args.directory,
            dry_run=args.dry_run,
            recursive=args.recursive,
            file_type=args.file_type
        )
    except DirectoryProcessingError as e:
        logging.error(f"Directory error: {e}")
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
