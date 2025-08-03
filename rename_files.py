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
    Configure global logging settings.
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
        file_path: Path to the file to be renamed.
        dry_run: If True, log the renaming operation without performing it.

    Returns:
        True if the file was (or would be) renamed, False otherwise.
    """
    new_name = file_path.name.replace(" ", "_")
    if new_name == file_path.name:
        logger.debug(f"Skipped (no spaces): {file_path}")
        return False

    new_path = file_path.with_name(new_name)

    if new_path.exists():
        logger.warning(f"Skipped (target already exists): {new_path}")
        return False

    if dry_run:
        logger.info(f"[Dry Run] Would rename: {file_path} -> {new_path}")
        return True

    try:
        file_path.rename(new_path)
        logger.info(f"Renamed: {file_path} -> {new_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to rename {file_path}: {e}")
        return False


def process_directory(
    directory: Path,
    dry_run: bool = False,
    recursive: bool = False,
    file_type: Optional[str] = None
) -> None:
    """
    Process a directory, renaming files with spaces in their names.

    Args:
        directory: Path to the directory.
        dry_run: If True, simulate file renaming.
        recursive: If True, include subdirectories.
        file_type: Optional file extension filter (e.g., "jpg").
    """
    if not directory.is_dir():
        raise DirectoryProcessingError(f"Invalid directory: {directory}")

    start_time = time.time()
    renamed_count = 0
    extension_filter = f".{file_type.lower()}" if file_type else None
    file_iterator = directory.rglob("*") if recursive else directory.glob("*")

    logger.info(f"Processing directory: {directory}")
    logger.info(f"Options -> Dry Run: {dry_run}, Recursive: {recursive}, Filter: {extension_filter or 'All files'}")

    for file_path in file_iterator:
        if not file_path.is_file():
            continue
        if file_path.name.startswith("."):
            logger.debug(f"Skipped (hidden): {file_path}")
            continue
        if extension_filter and file_path.suffix.lower() != extension_filter:
            logger.debug(f"Skipped (wrong extension): {file_path}")
            continue

        if rename_file(file_path, dry_run):
            renamed_count += 1

    elapsed = time.time() - start_time
    logger.info(f"Completed. Files renamed: {renamed_count}. Elapsed time: {elapsed:.2f} seconds.")


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description="Batch rename files by replacing spaces with underscores."
    )
    parser.add_argument("directory", type=Path, help="Target directory to process.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without modifying files.")
    parser.add_argument("--recursive", action="store_true", help="Process files recursively.")
    parser.add_argument("--file-type", type=str, help="Filter files by extension (e.g., 'jpg').")
    parser.add_argument("--log-level", type=str.upper, choices=logging._nameToLevel.keys(),
                        default="INFO", help="Set log level (e.g., DEBUG, INFO, WARNING).")
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
            file_type=args.file_type,
        )
    except DirectoryProcessingError as e:
        logger.error(str(e))
    except Exception as e:
        logger.exception(f"Unhandled error: {e}")


if __name__ == "__main__":
    main()
