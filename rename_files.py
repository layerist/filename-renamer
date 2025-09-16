import argparse
import logging
import time
from pathlib import Path
from typing import Optional, Sequence

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

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


def sanitize_filename(name: str, replacement: str = "_") -> str:
    """
    Sanitize a filename by replacing spaces and trimming.
    """
    return name.replace(" ", replacement).strip()


def rename_file(file_path: Path, replacement: str = "_", dry_run: bool = False) -> bool:
    """
    Rename a file by sanitizing its name.

    Returns:
        True if the file was (or would be) renamed, False otherwise.
    """
    new_name = sanitize_filename(file_path.name, replacement)
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
    file_types: Optional[Sequence[str]] = None,
    replacement: str = "_",
    case_insensitive: bool = True,
) -> None:
    """
    Process a directory, renaming files with spaces in their names.
    """
    directory = directory.resolve()
    if not directory.is_dir():
        raise DirectoryProcessingError(f"Invalid directory: {directory}")

    start_time = time.time()
    renamed_count = 0
    scanned_count = 0

    if file_types:
        file_types = [f".{ft.lstrip('.').lower()}" for ft in file_types]

    file_iterator = directory.rglob("*") if recursive else directory.glob("*")
    iterable = tqdm(file_iterator, desc="Processing") if tqdm else file_iterator

    logger.info(f"Processing directory: {directory}")
    logger.info(
        f"Options -> Dry Run: {dry_run}, Recursive: {recursive}, "
        f"Filter: {', '.join(file_types) if file_types else 'All files'}"
    )

    for file_path in iterable:
        if not file_path.is_file():
            continue
        if file_path.name.startswith("."):
            logger.debug(f"Skipped (hidden): {file_path}")
            continue

        if file_types:
            suffix = file_path.suffix.lower() if case_insensitive else file_path.suffix
            if suffix not in file_types:
                logger.debug(f"Skipped (wrong extension): {file_path}")
                continue

        scanned_count += 1
        if rename_file(file_path, replacement=replacement, dry_run=dry_run):
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
        description="Batch rename files by replacing spaces in filenames."
    )
    parser.add_argument("directory", type=Path, help="Target directory to process.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Simulate changes without modifying files."
    )
    parser.add_argument("--recursive", action="store_true", help="Process files recursively.")
    parser.add_argument(
        "--file-types",
        type=lambda s: [ft.strip() for ft in s.split(",")],
        help="Filter files by extension(s), e.g., 'jpg,png,txt'.",
    )
    parser.add_argument(
        "--replacement",
        type=str,
        default="_",
        help="Replacement character for spaces (default: '_').",
    )
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Make file extension filtering case-sensitive.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Set log level (DEBUG, INFO, WARNING, ERROR).",
    )
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
            file_types=args.file_types,
            replacement=args.replacement,
            case_insensitive=not args.case_sensitive,
        )
    except DirectoryProcessingError as e:
        logger.error(str(e))
    except Exception:
        logger.exception("Unhandled error")


if __name__ == "__main__":
    main()
