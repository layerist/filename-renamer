import argparse
import logging
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


class DirectoryProcessingError(Exception):
    """Raised when the provided path is not a valid directory."""


def setup_logging(level: str = "INFO") -> None:
    """
    Configure global logging settings.
    """
    numeric_level = logging.getLevelName(level.upper())
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def sanitize_filename(name: str, replace_space: bool = True) -> str:
    """
    Sanitize file name (replace spaces, trim).
    """
    new_name = name
    if replace_space:
        new_name = new_name.replace(" ", "_")
    return new_name.strip()


def rename_file(file_path: Path, dry_run: bool = False) -> bool:
    """
    Rename a file by sanitizing its name.

    Args:
        file_path: Path to the file to be renamed.
        dry_run: If True, log the renaming operation without performing it.

    Returns:
        True if the file was (or would be) renamed, False otherwise.
    """
    new_name = sanitize_filename(file_path.name)
    if new_name == file_path.name:
        logger.debug(f"Skipped (already clean): {file_path}")
        return False

    new_path = file_path.with_name(new_name)

    if new_path.exists():
        logger.warning(f"Skipped (target exists): {new_path}")
        return False

    if dry_run:
        logger.info(f"[Dry Run] {file_path} -> {new_path}")
        return True

    try:
        file_path.rename(new_path)
        logger.info(f"Renamed: {file_path} -> {new_path}")
        return True
    except PermissionError:
        logger.error(f"Permission denied: {file_path}")
    except OSError as e:
        logger.error(f"OS error renaming {file_path}: {e}")
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
    directory = directory.resolve()
    if not directory.is_dir():
        raise DirectoryProcessingError(f"Invalid directory: {directory}")

    start_time = time.time()
    renamed_count = 0
    scanned_count = 0

    extension_filter = f".{file_type.lower()}" if file_type else None
    file_iterator = directory.rglob("*") if recursive else directory.glob("*")

    logger.info(f"Processing directory: {directory}")
    logger.info(
        f"Options -> Dry Run: {dry_run}, Recursive: {recursive}, Filter: {extension_filter or 'All files'}"
    )

    for file_path in file_iterator:
        if not file_path.is_file():
            continue
        if file_path.name.startswith("."):
            logger.debug(f"Skipped (hidden): {file_path}")
            continue
        if extension_filter and file_path.suffix.lower() != extension_filter:
            logger.debug(f"Skipped (wrong extension): {file_path}")
            continue

        scanned_count += 1
        if rename_file(file_path, dry_run):
            renamed_count += 1

    elapsed = time.time() - start_time
    logger.info(
        f"Completed. Scanned: {scanned_count}, Renamed: {renamed_count}, Time: {elapsed:.2f}s"
    )


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Batch rename files by replacing spaces with underscores."
    )
    parser.add_argument("directory", type=Path, help="Target directory to process.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without modifying files.")
    parser.add_argument("--recursive", action="store_true", help="Process files recursively.")
    parser.add_argument("--file-type", type=str, help="Filter files by extension (e.g., 'jpg').")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Set log level (DEBUG, INFO, WARNING, ERROR).")
    return parser.parse_args()


def main() -> None:
    """
    Main entry point.
    """
    args = parse_arguments()
    setup_logging(args.log_level)

    try:
        process_directory(
            directory=args.directory,
            dry_run=args.dry_run,
            recursive=args.recursive,
            file_type=args.file_type,
        )
    except DirectoryProcessingError as e:
        logger.error(str(e))
    except Exception:
        logger.exception("Unhandled error")


if __name__ == "__main__":
    main()
