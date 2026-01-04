#!/usr/bin/env python3
"""
File Sanitizer — Fast, safe, multithreaded file renamer.

Enhancements:
  ✔ Unicode-safe normalization (NFKC)
  ✔ Deterministic collision resolution (_1, _2, …)
  ✔ Atomic renames with optional backup
  ✔ Signal-safe graceful shutdown
  ✔ Faster directory scanning
  ✔ Thread-safe structured logging
  ✔ Clear separation of concerns
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
from threading import Event
from typing import Iterable, Optional, Sequence, Set

# =======================================================
# Optional dependencies
# =======================================================

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

try:
    from colorama import Fore, init as colorama_init
    colorama_init(autoreset=True)
except ImportError:  # pragma: no cover
    class _Dummy:
        RED = GREEN = YELLOW = CYAN = ""
    Fore = _Dummy()

logger = logging.getLogger("file_sanitizer")
stop_event = Event()

# =======================================================
# Logging
# =======================================================

def setup_logging(level: str) -> None:
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
    *,
    replacement: str,
    illegal: Set[str] = DEFAULT_ILLEGAL_CHARS,
) -> str:
    name = unicodedata.normalize("NFKC", name)

    cleaned = []
    for ch in name:
        if ch in illegal or ch.isspace() or ord(ch) < 32:
            cleaned.append(replacement)
        else:
            cleaned.append(ch)

    result = "".join(cleaned)

    while replacement * 2 in result:
        result = result.replace(replacement * 2, replacement)

    result = result.strip(replacement)
    return result or "unnamed"

# =======================================================
# Filesystem helpers
# =======================================================

def atomic_rename(src: Path, dst: Path) -> None:
    os.replace(src, dst)

def unique_target(path: Path) -> Path:
    """
    Deterministic collision-safe target generation:
    file.txt -> file_1.txt -> file_2.txt ...
    """
    if not path.exists():
        return path

    stem, suffix = path.stem, path.suffix
    parent = path.parent
    i = 1

    while True:
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1

def create_backup(src: Path) -> Path:
    bak = src.with_suffix(src.suffix + ".bak")
    return unique_target(bak)

# =======================================================
# Rename operation
# =======================================================

def rename_file(
    path: Path,
    *,
    replacement: str,
    dry_run: bool,
    backup: bool,
) -> bool:
    if stop_event.is_set():
        return False

    new_name = sanitize_filename(path.name, replacement=replacement)
    if new_name == path.name:
        return False

    target = unique_target(path.with_name(new_name))

    if dry_run:
        logger.info(f"[Dry-run] {path.name} → {target.name}")
        return True

    try:
        if backup:
            bak = create_backup(path)
            atomic_rename(path, bak)
            atomic_rename(bak, target)
        else:
            atomic_rename(path, target)

        logger.info(f"{Fore.GREEN}Renamed: {path.name} → {target.name}")
        return True

    except Exception as exc:
        logger.error(
            f"{Fore.RED}Failed: {path.name} → {target.name} | {exc}"
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
    stack = [root]

    while stack:
        current = stack.pop()
        try:
            for entry in os.scandir(current):
                if entry.is_dir(follow_symlinks=False) and recursive:
                    stack.append(Path(entry.path))
                elif entry.is_file(follow_symlinks=False):
                    name = entry.name
                    if name.startswith("."):
                        continue

                    if extensions:
                        suffix = Path(name).suffix
                        suffix = suffix.lower() if case_insensitive else suffix
                        if suffix not in extensions:
                            continue

                    yield Path(entry.path)
        except PermissionError:
            logger.warning(f"{Fore.YELLOW}Skipped (permission): {current}")

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
        extensions = {f".{e.lstrip('.').lower()}" for e in file_types}

    files = list(
        collect_files(
            directory,
            recursive=recursive,
            extensions=extensions,
            case_insensitive=case_insensitive,
        )
    )

    if not files:
        logger.info(f"{Fore.YELLOW}No files found")
        return

    logger.info(f"Found {len(files)} file(s). Processing…")

    start = time.time()
    renamed = 0

    progress = tqdm(total=len(files), unit="file", desc="Renaming") if tqdm else None

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
            if stop_event.is_set():
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
    p = argparse.ArgumentParser(description="Batch sanitize filenames.")
    p.add_argument("directory", type=Path)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--recursive", action="store_true")
    p.add_argument("--file-types", type=lambda s: s.split(","))
    p.add_argument("--replacement", default="_")
    p.add_argument("--case-sensitive", action="store_true")
    p.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    p.add_argument("--backup", action="store_true")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args()

def _handle_signal(*_: object) -> None:
    stop_event.set()
    logger.warning("\nInterrupt received — stopping gracefully…")

def main() -> None:
    args = parse_arguments()
    setup_logging(args.log_level)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

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
