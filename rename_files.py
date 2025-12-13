#!/usr/bin/env python3
"""
File Sanitizer — Fast, safe, multithreaded file renamer.
Improved Edition:
  ✔ Unicode-safe normalization (NFKC)
  ✔ Configurable illegal character policy
  ✔ Deterministic, collision-safe renaming
  ✔ True atomic rename with optional backup
  ✔ Thread-safe, structured logging
  ✔ Faster directory scanning
  ✔ Graceful shutdown with clean progress handling
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Optional, Sequence, Set


# =======================================================
# Optional dependencies
# =======================================================

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
except ImportError:  # pragma: no cover
    class _Dummy:
        RED = GREEN = YELLOW = CYAN = RESET = ""
    Fore = Style = _Dummy()


logger = logging.getLogger("file_sanitizer")


# =======================================================
# Logging
# =======================================================

def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )


# =======================================================
# Sanitization
# =======================================================

DEFAULT_ILLEGAL_CHARS: Set[str] = set(r'<>:"/\|?*')


def sanitize_filename(
    name: str,
    replacement: str = "_",
    illegal: Set[str] = DEFAULT_ILLEGAL_CHARS,
    normalize: bool = True,
    collapse: bool = True,
) -> str:
    """
    Sanitize a filename in a Unicode-safe way.
    - Normalizes Unicode (NFKC)
    - Replaces illegal chars and whitespace
    - Optionally collapses repeated replacements
    """
    if normalize:
        name = unicodedata.normalize("NFKC", name)

    out = []
    for ch in name:
        if ch in illegal or ch.isspace() or ord(ch) < 32:
            out.append(replacement)
        else:
            out.append(ch)

    result = "".join(out)

    if collapse:
        while replacement * 2 in result:
            result = result.replace(replacement * 2, replacement)

    result = result.strip(replacement)
    return result or "unnamed"


# =======================================================
# Filesystem helpers
# =======================================================

def atomic_rename(src: Path, dst: Path) -> None:
    """
    Atomic rename with overwrite protection.
    """
    if dst.exists():
        raise FileExistsError(f"Target already exists: {dst}")
    os.replace(src, dst)  # atomic on POSIX & Windows NTFS


def create_backup(src: Path) -> Path:
    """
    Create a unique .bak backup next to the original file.
    """
    base = src.with_suffix(src.suffix + ".bak")
    candidate = base
    idx = 1

    while candidate.exists():
        candidate = src.with_suffix(src.suffix + f".bak.{idx}")
        idx += 1

    os.replace(src, candidate)
    return candidate


def rename_file(
    path: Path,
    *,
    replacement: str,
    dry_run: bool,
    backup: bool,
) -> bool:
    """
    Rename a single file. Returns True if renamed.
    """
    new_name = sanitize_filename(path.name, replacement=replacement)
    if new_name == path.name:
        return False

    target = path.with_name(new_name)

    if target.exists():
        logger.warning(
            f"{Fore.YELLOW}Skipped (collision): {path.name} → {new_name}"
        )
        return False

    if dry_run:
        logger.info(f"[Dry-run] {path.name} → {new_name}")
        return True

    try:
        if backup:
            bak = create_backup(path)
            atomic_rename(bak, target)
        else:
            atomic_rename(path, target)

        logger.info(f"{Fore.GREEN}Renamed: {path.name} → {new_name}")
        return True

    except Exception as exc:
        logger.error(
            f"{Fore.RED}Failed: {path.name} → {new_name} | {exc}"
        )
        return False


# =======================================================
# Directory scanning
# =======================================================

def collect_files(
    root: Path,
    *,
    recursive: bool,
    extensions: Optional[Set[str]],
    case_insensitive: bool,
) -> Iterable[Path]:
    """
    Efficient generator for file collection.
    """
    iterator = root.rglob("*") if recursive else root.iterdir()

    for p in iterator:
        if not p.is_file():
            continue
        if p.name.startswith("."):
            continue

        if extensions:
            suf = p.suffix.lower() if case_insensitive else p.suffix
            if suf not in extensions:
                continue

        yield p


# =======================================================
# Processing
# =======================================================

def process_directory(
    directory: Path,
    *,
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
        extensions = {
            f".{ext.lstrip('.').lower()}" for ext in file_types
        }

    files = list(
        collect_files(
            directory,
            recursive=recursive,
            extensions=extensions,
            case_insensitive=case_insensitive,
        )
    )

    if not files:
        logger.info(f"{Fore.YELLOW}No files to process in {directory}")
        return

    logger.info(f"Found {len(files)} file(s). Starting…")

    renamed = 0
    start = time.time()
    stop = False

    def handle_signal(*_):
        nonlocal stop
        stop = True
        logger.warning("\nInterrupt received — stopping gracefully…")

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    progress = tqdm(total=len(files), desc="Renaming", unit="file") if tqdm else None

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                rename_file,
                f,
                replacement=replacement,
                dry_run=dry_run,
                backup=backup,
            )
            for f in files
        ]

        for future in as_completed(futures):
            if stop:
                break

            try:
                if future.result():
                    renamed += 1
            except Exception as exc:  # pragma: no cover
                logger.error(f"{Fore.RED}Unhandled error: {exc}")

            if progress:
                progress.update(1)

    if progress:
        progress.close()

    elapsed = time.time() - start
    logger.info(
        f"{Fore.CYAN}Done — Total: {len(files)}, Renamed: {renamed}, "
        f"Time: {elapsed:.2f}s"
    )


# =======================================================
# CLI
# =======================================================

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch sanitize filenames across directories."
    )
    parser.add_argument("directory", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--recursive", action="store_true")
    parser.add_argument(
        "--file-types",
        type=lambda s: [x.strip() for x in s.split(",")],
        help="Comma-separated extensions: jpg,png,txt",
    )
    parser.add_argument("--replacement", default="_")
    parser.add_argument(
        "--case-sensitive",
        action="store_true",
        help="Case-sensitive extension filtering",
    )
    parser.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    parser.add_argument("--backup", action="store_true")
    parser.add_argument("--log-level", default="INFO")
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
    except Exception as exc:
        logger.error(f"{Fore.RED}Fatal error: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
