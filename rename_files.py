#!/usr/bin/env python3
"""
File Sanitizer — A fast, multithreaded batch file renamer.

Features:
  • Removes or replaces illegal characters across OSes
  • Supports recursive processing and extension filters
  • Optional dry-run and backup modes
  • Colored logging and progress bar (tqdm)
  • Thread-safe parallel renaming

Usage example:
  python file_sanitizer.py ./downloads --recursive --file-types jpg,png --dry-run
"""

import argparse
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Sequence, List

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
except ImportError:
    class Dummy:
        RESET_ALL = YELLOW = RED = GREEN = CYAN = ""
    Fore = Style = Dummy()

logger = logging.getLogger("file_sanitizer")


# ===========================
# Utility Functions
# ===========================

def setup_logging(level: str = "INFO") -> None:
    """Configure logging with color support."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format=f"%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def sanitize_filename(name: str, replacement: str = "_") -> str:
    """Return a sanitized version of a filename (cross-platform safe)."""
    illegal_chars = r'<>:"/\\|?*'
    sanitized = "".join(
        replacement if ch in illegal_chars else ch for ch in name
    ).replace(" ", replacement).strip()
    return sanitized


def rename_file(file_path: Path, replacement: str = "_", dry_run: bool = False, backup: bool = False) -> bool:
    """Rename a file by sanitizing its name. Returns True if changed."""
    new_name = sanitize_filename(file_path.name, replacement)
    if new_name == file_path.name:
        return False

    new_path = file_path.with_name(new_name)
    if new_path.exists():
        logger.warning(f"{Fore.YELLOW}Skipped (target exists): {new_path}")
        return False

    if dry_run:
        logger.info(f"[Dry Run] {file_path.name} → {new_name}")
        return True

    try:
        if backup:
            backup_path = file_path.with_suffix(file_path.suffix + ".bak")
            file_path.rename(backup_path)
            backup_path.rename(new_path)
        else:
            file_path.rename(new_path)

        logger.info(f"{Fore.GREEN}Renamed: {file_path.name} → {new_name}")
        return True

    except PermissionError:
        logger.error(f"{Fore.RED}Permission denied: {file_path}")
    except OSError as e:
        logger.error(f"{Fore.RED}OS error renaming {file_path}: {e}")
    return False


# ===========================
# Core Directory Processing
# ===========================

def collect_files(
    directory: Path,
    recursive: bool,
    file_types: Optional[Sequence[str]],
    case_insensitive: bool
) -> List[Path]:
    """Collect and filter files for renaming."""
    file_iterator = directory.rglob("*") if recursive else directory.glob("*")
    files = [f for f in file_iterator if f.is_file() and not f.name.startswith(".")]

    if file_types:
        normalized = [f".{ft.lstrip('.').lower()}" for ft in file_types]
        files = [
            f for f in files
            if (f.suffix.lower() if case_insensitive else f.suffix) in normalized
        ]
    return files


def process_directory(
    directory: Path,
    dry_run: bool = False,
    recursive: bool = False,
    file_types: Optional[Sequence[str]] = None,
    replacement: str = "_",
    case_insensitive: bool = True,
    max_workers: int = 4,
    backup: bool = False,
) -> None:
    """Process all files in the directory and rename as needed."""
    directory = directory.resolve()
    if not directory.is_dir():
        raise ValueError(f"Invalid directory: {directory}")

    files = collect_files(directory, recursive, file_types, case_insensitive)
    total = len(files)

    if total == 0:
        logger.info(f"{Fore.YELLOW}No matching files found in {directory}.")
        return

    logger.info(f"Found {total} file(s) to process in: {directory}")
    start_time = time.time()
    renamed_count = 0

    progress = tqdm(total=total, desc="Renaming", unit="file") if tqdm else None

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(rename_file, f, replacement, dry_run, backup): f
                for f in files
            }
            for future in as_completed(futures):
                if progress:
                    progress.update(1)
                try:
                    if future.result():
                        renamed_count += 1
                except Exception as e:
                    logger.error(f"{Fore.RED}Error processing {futures[future]}: {e}")
    except KeyboardInterrupt:
        logger.warning(f"\n{Fore.YELLOW}Operation cancelled by user.")
        return
    finally:
        if progress:
            progress.close()

    elapsed = time.time() - start_time
    logger.info(
        f"{Fore.CYAN}Completed — Total: {total}, Renamed: {renamed_count}, Time: {elapsed:.2f}s"
    )


# ===========================
# CLI Interface
# ===========================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch rename files by sanitizing filenames (spaces, invalid chars, etc.)"
    )
    parser.add_argument("directory", type=Path, help="Target directory to process.")
    parser.add_argument("--dry-run", action="store_true", help="Simulate changes without renaming files.")
    parser.add_argument("--recursive", action="store_true", help="Process files recursively.")
    parser.add_argument("--file-types", type=lambda s: [ft.strip() for ft in s.split(",")],
                        help="Filter by file extensions, e.g. 'jpg,png,txt'.")
    parser.add_argument("--replacement", type=str, default="_",
                        help="Replacement character for spaces/invalid chars (default: '_').")
    parser.add_argument("--case-sensitive", action="store_true",
                        help="Make extension filtering case-sensitive.")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Log level (DEBUG, INFO, WARNING, ERROR).")
    parser.add_argument("--threads", type=int, default=4,
                        help="Number of parallel workers (default: 4).")
    parser.add_argument("--backup", action="store_true",
                        help="Create .bak backup before renaming.")
    return parser.parse_args()


def main() -> None:
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
            max_workers=args.threads,
            backup=args.backup,
        )
    except Exception as e:
        logger.error(f"{Fore.RED}Fatal error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
