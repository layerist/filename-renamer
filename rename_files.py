import argparse
import logging
import time
from pathlib import Path
from typing import Optional


class DirectoryProcessingError(Exception):
    """Raised when directory validation fails."""
    pass


def setup_logging(level: int = logging.INFO) -> None:
    """
    Configure logging with a standard format and level.
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def rename_file(file_path: Path, dry_run: bool) -> None:
    """
    Rename a file by replacing spaces in the name with underscores.

    Args:
        file_path: The original file path.
        dry_run: If True, simulate the rename without applying changes.
    """
    new_name = file_path.name.replace(" ", "_")
    new_path = file_path.with_name(new_name)

    if file_path == new_path:
        logging.debug(f"Skipped (no change needed): {file_path}")
        return

    if new_path.exists():
        logging.warning(f"Skipped (target already exists): {new_path}")
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
    Process all files in a directory, renaming them if needed.

    Args:
        directory: The target directory path.
        dry_run: Simulate the renaming process.
        recursive: Include files in subdirectories.
        file_type: File extension to filter by (e.g., "txt").
    """
    if not directory.is_dir():
        raise DirectoryProcessingError(f"Invalid directory: {directory}")

    start_time = time.time()
    extension = f".{file_type.lower()}" if file_type else None
    pattern = "**/*" if recursive else "*"

    logging.info(f"Scanning directory: {directory} | Recursive: {recursive} | Dry run: {dry_run} | Filter: {extension or 'All'}")

    for file_path in directory.glob(pattern):
        if not file_path.is_file():
            continue

        if file_path.name.startswith('.'):
            logging.debug(f"Skipped (hidden): {file_path.name}")
            continue

        if extension and file_path.suffix.lower() != extension:
            logging.debug(f"Skipped (extension mismatch): {file_path.name}")
            continue

        rename_file(file_path, dry_run)

    elapsed = time.time() - start_time
    logging.info(f"Processing completed in {elapsed:.2f} seconds.")


def parse_arguments() -> argparse.Namespace:
    """
    Parse and return command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Batch rename files by replacing spaces with underscores.")
    parser.add_argument("directory", type=Path, help="Path to the directory to process.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without renaming files.")
    parser.add_argument("--recursive", action="store_true", help="Recursively process subdirectories.")
    parser.add_argument("--log-level", type=str.upper, choices=logging._nameToLevel.keys(), default="INFO", help="Logging level.")
    parser.add_argument("--file-type", type=str, help="Only rename files with this extension (e.g., 'jpg').")
    return parser.parse_args()


def main() -> None:
    """
    Script entry point.
    """
    args = parse_arguments()
    setup_logging(logging._nameToLevel.get(args.log_level, logging.INFO))

    try:
        process_directory(args.directory, args.dry_run, args.recursive, args.file_type)
    except DirectoryProcessingError as e:
        logging.error(e)
    except Exception as e:
        logging.exception("An unexpected error occurred.")


if __name__ == "__main__":
    main()
