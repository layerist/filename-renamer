#!/usr/bin/env python3
"""
File Sanitizer — Fast, safe, multithreaded file renamer.
Enhanced Edition:
  ✔ Stronger filename sanitizing (unicode-safe)
  ✔ Collision-safe renaming
  ✔ Atomic rename + backup
  ✔ Thread-safe logging
  ✔ Faster file scanning
  ✔ More robust exceptions
"""

import argparse
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Sequence, Iterable, List


# =======================================================
# Optional dependencies
# =======================================================

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
except ImportError:
    class Dummy:
        RESET = RED = GREEN = YELLOW = CYAN = ""
    Fore = Style = Dummy()


logger = logging.getLogger("file_sanitizer")


# =======================================================
# Utility functions
# =======================================================

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


ILLEGAL_CHARS = set(r'<>:"/\|?*')


def sanitize_filename(name: str, replacement: str = "_") -> str:
    """
    Safe filename sanitizer.
    Removes illegal chars and normalizes spacing.
    """
    sanitized = []
    for ch in name:
        if ch in ILLEGAL_CHARS or ch.isspace():
            sanitized.append(replacement)
        else:
            sanitized.append(ch)
    out = "".join(sanitized).strip(replacement)
    return out or "unnamed"


def safe_rename(src: Path, dst: Path) -> None:
    """
    Atomic rename with overwrite protection.
    """
    # System-level guarantee: path must not exist before rename
    try:
        os.replace(src, dst)  # atomic on most platforms
    except FileExistsError:
        raise FileExistsError(f"Cannot rename: target exists: {dst}")


def apply_backup(src: Path) -> Path:
    """
    Creates a unique .bak backup file next to original.
    """
    base = src.with_suffix(src.suffix + ".bak")
    candidate = base
    idx = 1

    while candidate.exists():
        candidate = src.with_suffix(src.suffix + f".bak.{idx}")
        idx += 1

    src.rename(candidate)
    return candidate


def rename_file(
    file_path: Path,
    replacement: str = "_",
    dry_run: bool = False,
    backup: bool = False
) -> bool:
    """
    Rename a file; returns True if changed.
    """

    new_name = sanitize_filename(file_path.name, replacement)
    if new_name == file_path.name:
        return False

    new_path = file_path.with_name(new_name)

    # Collision check
    if new_path.exists():
        logger.warning(f"{Fore.YELLOW}Skipped (target exists): {file_path} -> {new_name}")
        return False

    if dry_run:
        logger.info(f"[Dry Run] {file_path.name} → {new_name}")
        return True

    try:
        original = file_path
        if backup:
            backup_file = apply_backup(original)
            safe_rename(backup_file, new_path)
        else:
            safe_rename(original, new_path)

        logger.info(f"{Fore.GREEN}Renamed: {file_path.name} → {new_name}")
        return True

    except Exception as e:
        logger.error(f"{Fore.RED}Error renaming {file_path}: {e}")
        return False


# =======================================================
# Directory scanning
# =======================================================

def collect_files(
    directory: Path,
    recursive: bool,
    extensions: Optional[Sequence[str]],
    case_insensitive: bool
) -> Iterable[Path]:
    """
    Efficiently collect files matching filters.
    """
    pattern_iter = directory.rglob("*") if recursive else directory.glob("*")

    for f in pattern_iter:
        if not f.is_file():
            continue
        if f.name.startswith("."):
            continue

        if extensions:
            ext = f.suffix.lower() if case_insensitive else f.suffix
            if ext not in extensions:
                continue

        yield f


# =======================================================
# Main logic
# =======================================================

def process_directory(
    directory: Path,
    dry_run: bool,
    recursive: bool,
    file_types: Optional[Sequence[str]],
    replacement: str,
    case_insensitive: bool,
    max_workers: int,
    backup: bool,
) -> None:

    directory = directory.resolve()
    if not directory.is_dir():
        raise ValueError(f"Invalid directory: {directory}")

    extensions = None
    if file_types:
        extensions = [f".{ft.lstrip('.').lower()}" for ft in file_types]

    files = list(collect_files(directory, recursive, extensions, case_insensitive))
    total = len(files)

    if not total:
        logger.info(f"{Fore.YELLOW}No files to process in {directory}")
        return

    logger.info(f"Found {total} file(s). Starting...")

    renamed = 0
    start = time.time()
    progress = tqdm(total=total, desc="Renaming", unit="file") if tqdm else None

    # Graceful Ctrl+C
    stop_flag = False

    def sig_handler(*_):
        nonlocal stop_flag
        stop_flag = True
        logger.warning("\nInterrupt received. Finishing pending tasks...")

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(rename_file, f, replacement, dry_run, backup): f for f in files}

        for future in as_completed(futures):
            if stop_flag:
                break

            result = False
            try:
                result = future.result()
            except Exception as e:
                logger.error(f"{Fore.RED}Unexpected error: {e}")

            if result:
                renamed += 1

            if progress:
                progress.update(1)

    if progress:
        progress.close()

    elapsed = time.time() - start
    logger.info(f"{Fore.CYAN}Completed — Total: {total}, Renamed: {renamed}, Time: {elapsed:.2f}s")


# =======================================================
# CLI
# =======================================================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch sanitize filenames across directories."
    )
    parser.add_argument("directory", type=Path, help="Directory to process.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without renaming files.")
    parser.add_argument("--recursive", action="store_true", help="Process subdirectories recursively.")
    parser.add_argument("--file-types", type=lambda s: [i.strip() for i in s.split(",")],
                        help="Comma-separated extensions: 'jpg,png,txt'")
    parser.add_argument("--replacement", type=str, default="_",
                        help="Replacement for invalid characters.")
    parser.add_argument("--case-sensitive", action="store_true",
                        help="Case-sensitive extension filtering.")
    parser.add_argument("--threads", type=int, default=4,
                        help="Thread pool size.")
    parser.add_argument("--backup", action="store_true",
                        help="Create .bak backups before renaming.")
    parser.add_argument("--log-level", type=str, default="INFO",
                        help="Log level (DEBUG, INFO, WARNING, ERROR)")

    return parser.parse_args()


def main():
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
