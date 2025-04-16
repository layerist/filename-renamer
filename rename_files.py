import argparse
import logging
import time
from pathlib import Path
from typing import Optional


class DirectoryProcessingError(Exception):
    """Custom exception for directory processing errors."""
    pass


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure the logging system.
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def rename_file(file_path: Path, dry_run: bool) -> None:
    """
    Rename a file by replacing spaces with underscores.
    """
    new_name = file_path.name.replace(" ", "_")
    new_path = file_path.with_name(new_name)

    if file_path == new_path:
        logging.debug(f"Skipping (no change): {file_path}")
        return

    if dry_run:
        logging.info(f"[Dry Run] Rename: {file_path} -> {new_path}")
    else:
        try:
            file_path.rename(new_path)
            logging.info(f"Renamed: {file_path} -> {new_path}")
        except OSError as e:
            logging.error(f"Failed to rename {file_path}: {e}")


def process_directory(directory: Path, dry_run: bool = False, recursive: bool = False, file_type: Optional[str] = None) -> None:
    """
    Process a directory and rename files by replacing spaces with underscores.
    """
    start_time = time.time()

    if not directory.is_dir():
        raise DirectoryProcessingError(f"Invalid directory: {directory}")

    file_type = file_type.lstrip('.').lower() if file_type else None
    logging.info(f"Processing: {directory} (Recursive: {recursive}, Dry run: {dry_run}, File type: {file_type or 'All'})")

    files = directory.rglob('*') if recursive else directory.glob('*')
    for file_path in files:
        if not file_path.is_file() or file_path.name.startswith('.'):
            continue

        if file_type and file_path.suffix.lower() != f'.{file_type}':
            logging.debug(f"Skipping (file type mismatch): {file_path.name}")
            continue

        rename_file(file_path, dry_run)

    elapsed = time.time() - start_time
    logging.info(f"Finished processing in {elapsed:.2f}s")


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Batch rename files by replacing spaces with underscores.")
    parser.add_argument('directory', type=Path, help="Directory path to process.")
    parser.add_argument('--dry-run', action='store_true', help="Simulate renaming without making changes.")
    parser.add_argument('--recursive', action='store_true', help="Recursively process subdirectories.")
    parser.add_argument('--log-level', type=str.upper, choices=logging._nameToLevel.keys(), default='INFO', help="Logging level (default: INFO).")
    parser.add_argument('--file-type', type=str, help="Filter files by extension (e.g., 'txt').")
    return parser.parse_args()


def main() -> None:
    """
    Entry point of the script.
    """
    args = parse_arguments()
    setup_logging(logging._nameToLevel.get(args.log_level, logging.INFO))

    try:
        process_directory(args.directory, args.dry_run, args.recursive, args.file_type)
    except DirectoryProcessingError as e:
        logging.error(e)
    except Exception as e:
        logging.exception("Unexpected error occurred")


if __name__ == "__main__":
    main()
