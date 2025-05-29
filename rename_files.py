import argparse
import logging
import time
from pathlib import Path
from typing import Optional


class DirectoryProcessingError(Exception):
    """Custom exception for directory validation errors."""


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure logging with a standardized format and level.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def rename_file(file_path: Path, dry_run: bool = False) -> None:
    """
    Rename a file by replacing spaces with underscores.

    Args:
        file_path: Path to the original file.
        dry_run: If True, simulates the renaming without applying changes.
    """
    new_name = file_path.name.replace(" ", "_")
    new_path = file_path.with_name(new_name)

    if file_path == new_path:
        logging.debug(f"Skipped (no change needed): {file_path}")
        return

    if new_path.exists():
        logging.warning(f"Skipped (target exists): {new_path}")
        return

    if dry_run:
        logging.info(f"[Dry Run] Would rename: {file_path} -> {new_path}")
    else:
        try:
            file_path.rename(new_path)
            logging.info(f"Renamed: {file_path} -> {new_path}")
        except OSError as e:
            logging.error(f"Failed to rename {file_path}: {e}")


def process_directory(directory: Path, dry_run: bool = False, recursive: bool = False, file_type: Optional[str] = None) -> None:
    """
    Process and rename files in a directory.

    Args:
        directory: Directory to process.
        dry_run: If True, simulates the process.
        recursive: Whether to process subdirectories recursively.
        file_type: Optional file extension filter (e.g., "jpg").
    """
    if not directory.is_dir():
        raise DirectoryProcessingError(f"Invalid directory: {directory}")

    start_time = time.time()
    extension_filter = f".{file_type.lower()}" if file_type else None
    search_pattern = "**/*" if recursive else "*"
    files = directory.rglob("*") if recursive else directory.glob("*")

    logging.info(f"Processing directory: {directory}")
    logging.info(f"Options -> Recursive: {recursive}, Dry Run: {dry_run}, Filter: {extension_filter or 'All'}")

    for file_path in files:
        if not file_path.is_file():
            continue
        if file_path.name.startswith('.'):
            logging.debug(f"Skipped (hidden): {file_path}")
            continue
        if extension_filter and file_path.suffix.lower() != extension_filter:
            logging.debug(f"Skipped (extension mismatch): {file_path}")
            continue
        rename_file(file_path, dry_run)

    elapsed_time = time.time() - start_time
    logging.info(f"Processing completed in {elapsed_time:.2f} seconds.")


def parse_arguments() -> argparse.Namespace:
    """
    Parse and return the command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Batch rename files by replacing spaces with underscores."
    )
    parser.add_argument("directory", type=Path, help="Path to the target directory.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate renaming without actual changes.")
    parser.add_argument("--recursive", action="store_true", help="Recursively process subdirectories.")
    parser.add_argument("--log-level", type=str.upper, choices=logging._nameToLevel.keys(),
                        default="INFO", help="Set logging level (e.g., INFO, DEBUG).")
    parser.add_argument("--file-type", type=str, help="Only rename files with this extension (e.g., jpg).")
    return parser.parse_args()


def main() -> None:
    """
    Main entry point of the script.
    """
    args = parse_arguments()
    setup_logging(logging._nameToLevel.get(args.log_level, logging.INFO))

    try:
        process_directory(args.directory, args.dry_run, args.recursive, args.file_type)
    except DirectoryProcessingError as e:
        logging.error(e)
    except Exception as e:
        logging.exception(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
