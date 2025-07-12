import argparse
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DirectoryProcessingError(Exception):
    """Raised when the provided path is not a valid directory."""


def setup_logging(level: int = logging.INFO) -> None:
    """
    Set up logging with a standardized format.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def rename_file(file_path: Path, dry_run: bool = False) -> bool:
    """
    Rename a file by replacing spaces with underscores.

    Args:
        file_path: The original file path.
        dry_run: If True, perform a dry run without renaming.

    Returns:
        True if the file was renamed or would be renamed in dry run, else False.
    """
    new_name = file_path.name.replace(" ", "_")
    if new_name == file_path.name:
        logger.debug(f"Skipped (no rename needed): {file_path}")
        return False

    new_path = file_path.with_name(new_name)

    if new_path.exists():
        logger.warning(f"Skipped (target exists): {new_path}")
        return False

    if dry_run:
        logger.info(f"[Dry Run] Would rename: {file_path} -> {new_path}")
        return True

    try:
        file_path.rename(new_path)
        logger.info(f"Renamed: {file_path} -> {new_path}")
        return True
    except OSError as e:
        logger.error(f"Rename failed for {file_path}: {e}")
        return False


def process_directory(
    directory: Path,
    dry_run: bool = False,
    recursive: bool = False,
    file_type: Optional[str] = None
) -> None:
    """
    Rename files in a directory by replacing spaces with underscores.

    Args:
        directory: The target directory.
        dry_run: If True, simulate renaming without making changes.
        recursive: Whether to process subdirectories.
        file_type: Optional file extension to filter files.
    """
    if not directory.is_dir():
        raise DirectoryProcessingError(f"Invalid directory: {directory}")

    start_time = time.time()
    renamed_count = 0
    extension_filter = f".{file_type.lower()}" if file_type else None
    files = directory.rglob("*") if recursive else directory.glob("*")

    logger.info(f"Processing directory: {directory}")
    logger.info(f"Options -> Dry Run: {dry_run}, Recursive: {recursive}, Filter: {extension_filter or 'All'}")

    for file_path in files:
        if not file_path.is_file():
            continue
        if file_path.name.startswith("."):
            logger.debug(f"Skipped (hidden file): {file_path}")
            continue
        if extension_filter and file_path.suffix.lower() != extension_filter:
            logger.debug(f"Skipped (extension mismatch): {file_path}")
            continue

        if rename_file(file_path, dry_run):
            renamed_count += 1

    duration = time.time() - start_time
    logger.info(f"Finished. Files renamed: {renamed_count}, Time elapsed: {duration:.2f} seconds.")


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description="Batch rename files by replacing spaces with underscores."
    )
    parser.add_argument("directory", type=Path, help="Target directory.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without renaming.")
    parser.add_argument("--recursive", action="store_true", help="Process subdirectories recursively.")
    parser.add_argument("--log-level", type=str.upper, choices=logging._nameToLevel.keys(),
                        default="INFO", help="Logging level (e.g., DEBUG, INFO).")
    parser.add_argument("--file-type", type=str, help="Only process files with this extension (e.g., jpg).")
    return parser.parse_args()


def main() -> None:
    """
    Main entry point.
    """
    args = parse_arguments()
    setup_logging(logging._nameToLevel.get(args.log_level, logging.INFO))

    try:
        process_directory(
            directory=args.directory,
            dry_run=args.dry_run,
            recursive=args.recursive,
            file_type=args.file_type
        )
    except DirectoryProcessingError as e:
        logger.error(e)
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
