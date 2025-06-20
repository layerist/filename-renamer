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
    Configure logging with a standardized format.
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
        file_path: Path to the original file.
        dry_run: If True, simulate the operation.

    Returns:
        True if renamed or would rename (dry run), False otherwise.
    """
    new_name = file_path.name.replace(" ", "_")
    new_path = file_path.with_name(new_name)

    if file_path == new_path:
        logger.debug(f"Skipped (no change): {file_path}")
        return False

    if new_path.exists():
        logger.warning(f"Skipped (target exists): {new_path}")
        return False

    if dry_run:
        logger.info(f"[Dry Run] Would rename: {file_path} -> {new_path}")
    else:
        try:
            file_path.rename(new_path)
            logger.info(f"Renamed: {file_path} -> {new_path}")
        except OSError as e:
            logger.error(f"Rename failed for {file_path}: {e}")
            return False

    return True


def process_directory(
    directory: Path,
    dry_run: bool = False,
    recursive: bool = False,
    file_type: Optional[str] = None
) -> None:
    """
    Process and rename files in a directory.

    Args:
        directory: Directory to process.
        dry_run: If True, simulate changes.
        recursive: Whether to include subdirectories.
        file_type: Optional file extension filter (e.g., "jpg").
    """
    if not directory.is_dir():
        raise DirectoryProcessingError(f"Not a valid directory: {directory}")

    start_time = time.time()
    extension_filter = f".{file_type.lower()}" if file_type else None
    search_pattern = "**/*" if recursive else "*"
    files = directory.glob(search_pattern)

    logger.info(f"Starting directory processing: {directory}")
    logger.info(f"Options -> Recursive: {recursive}, Dry Run: {dry_run}, Filter: {extension_filter or 'All'}")

    renamed_count = 0

    for file_path in files:
        if not file_path.is_file():
            continue
        if file_path.name.startswith('.'):
            logger.debug(f"Skipped (hidden file): {file_path}")
            continue
        if extension_filter and file_path.suffix.lower() != extension_filter:
            logger.debug(f"Skipped (extension mismatch): {file_path}")
            continue

        if rename_file(file_path, dry_run):
            renamed_count += 1

    elapsed = time.time() - start_time
    logger.info(f"Completed. Files processed: {renamed_count}, Time taken: {elapsed:.2f} seconds.")


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Batch rename files by replacing spaces with underscores."
    )
    parser.add_argument("directory", type=Path, help="Target directory.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate renaming without changes.")
    parser.add_argument("--recursive", action="store_true", help="Process subdirectories recursively.")
    parser.add_argument("--log-level", type=str.upper, choices=logging._nameToLevel.keys(),
                        default="INFO", help="Logging level (e.g., DEBUG, INFO).")
    parser.add_argument("--file-type", type=str, help="Only rename files with this extension (e.g., jpg).")
    return parser.parse_args()


def main() -> None:
    """
    Main script entry point.
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
