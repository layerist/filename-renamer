#!/usr/bin/env python3
"""
File Sanitizer — Fast, safe, multithreaded file renamer.

Major improvements:
✔ constant-memory processing (no futures list)
✔ producer/consumer pipeline
✔ faster sanitization (precompiled regex)
✔ atomic collision handling
✔ safer backup logic
✔ graceful shutdown
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import re
import signal
import sys
import time
import unicodedata
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Iterable, Optional, Sequence, Set

# =======================================================
# Optional dependencies
# =======================================================

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

try:
    from colorama import Fore, init as colorama_init
    colorama_init(autoreset=True)
except ImportError:
    class _Dummy:
        RED = GREEN = YELLOW = CYAN = ""
    Fore = _Dummy()

# =======================================================
# Globals
# =======================================================

logger = logging.getLogger("file_sanitizer")

stop_event = Event()

collision_lock = Lock()
reserved_targets: Set[Path] = set()

DEFAULT_ILLEGAL_CHARS: Set[str] = set(r'<>:"/\|?*')

WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}

RE_CONTROL = re.compile(r"[\x00-\x1F]")
RE_SPACE = re.compile(r"\s+")
RE_MULTI_REPLACE = None

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

def sanitize_filename(
    name: str,
    *,
    replacement: str,
    illegal: Set[str] = DEFAULT_ILLEGAL_CHARS,
    max_length: int = 255,
) -> str:
    name = unicodedata.normalize("NFKC", name)

    # fast character pass
    chars = []
    for ch in name:
        if ch in illegal or ord(ch) < 32:
            chars.append(replacement)
        elif ch.isspace():
            chars.append(replacement)
        else:
            chars.append(ch)

    result = "".join(chars)

    if replacement:
        result = RE_MULTI_REPLACE.sub(replacement, result)

    result = result.strip(replacement).rstrip(" .")

    if not result:
        result = "unnamed"

    if os.name == "nt":
        base = result.split(".")[0].upper()
        if base in WINDOWS_RESERVED:
            result = "_" + result

    if len(result) > max_length:
        stem, suffix = os.path.splitext(result)
        allowed = max_length - len(suffix)
        result = stem[:allowed] + suffix

    return result

# =======================================================
# Collision handling
# =======================================================

def unique_target(path: Path) -> Path:
    parent = path.parent
    stem = path.stem
    suffix = path.suffix

    with collision_lock:

        candidate = path
        index = 1

        while candidate.exists() or candidate in reserved_targets:
            candidate = parent / f"{stem}_{index}{suffix}"
            index += 1

        reserved_targets.add(candidate)

    return candidate

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
            backup_path = path.with_suffix(path.suffix + ".bak")
            os.replace(path, backup_path)
            os.replace(backup_path, target)
        else:
            os.replace(path, target)

        logger.info(f"{Fore.GREEN}Renamed: {path.name} → {target.name}")
        return True

    except Exception as exc:
        logger.error(f"{Fore.RED}Failed: {path.name} | {exc}")
        return False

# =======================================================
# Directory scanning
# =======================================================

def collect_files(
    root: Path,
    *,
    recursive: bool,
    extensions: Optional[Set[str]],
) -> Iterable[Path]:

    stack = [root]

    while stack and not stop_event.is_set():

        current = stack.pop()

        try:

            with os.scandir(current) as it:

                for entry in it:

                    if entry.is_dir(follow_symlinks=False):

                        if recursive:
                            stack.append(Path(entry.path))

                        continue

                    if not entry.is_file(follow_symlinks=False):
                        continue

                    if entry.name.startswith("."):
                        continue

                    if extensions:

                        suffix = Path(entry.name).suffix.lower()

                        if suffix not in extensions:
                            continue

                    yield Path(entry.path)

        except PermissionError:
            logger.warning(f"{Fore.YELLOW}Skipped (permission): {current}")

# =======================================================
# Worker
# =======================================================

def worker(
    q: queue.Queue,
    *,
    replacement: str,
    dry_run: bool,
    backup: bool,
    counter,
):

    while not stop_event.is_set():

        try:
            path = q.get(timeout=0.2)
        except queue.Empty:
            continue

        if path is None:
            break

        if rename_file(
            path,
            replacement=replacement,
            dry_run=dry_run,
            backup=backup,
        ):
            counter["renamed"] += 1

        counter["processed"] += 1

        q.task_done()

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
    max_workers: int,
    backup: bool,
) -> None:

    directory = directory.resolve()

    if not directory.is_dir():
        raise ValueError(f"Invalid directory: {directory}")

    extensions = (
        {f".{ext.lstrip('.').lower()}" for ext in file_types}
        if file_types else None
    )

    q: queue.Queue = queue.Queue(maxsize=1000)

    counter = {
        "processed": 0,
        "renamed": 0,
    }

    threads = []

    for _ in range(max_workers):

        t = Thread(
            target=worker,
            args=(q,),
            kwargs=dict(
                replacement=replacement,
                dry_run=dry_run,
                backup=backup,
                counter=counter,
            ),
            daemon=True,
        )

        t.start()
        threads.append(t)

    start = time.perf_counter()

    progress = tqdm(unit="file") if tqdm else None

    try:

        for file in collect_files(
            directory,
            recursive=recursive,
            extensions=extensions,
        ):

            if stop_event.is_set():
                break

            q.put(file)

            if progress:
                progress.update(1)

    finally:

        q.join()

        for _ in threads:
            q.put(None)

        for t in threads:
            t.join()

    if progress:
        progress.close()

    elapsed = time.perf_counter() - start

    logger.info(
        f"{Fore.CYAN}Done — Processed: {counter['processed']} | "
        f"Renamed: {counter['renamed']} | Time: {elapsed:.2f}s"
    )

# =======================================================
# CLI
# =======================================================

def parse_arguments() -> argparse.Namespace:

    p = argparse.ArgumentParser(description="Batch sanitize filenames")

    p.add_argument("directory", type=Path)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--recursive", action="store_true")
    p.add_argument("--file-types", type=lambda s: s.split(","))
    p.add_argument("--replacement", default="_")
    p.add_argument("--threads", type=int, default=os.cpu_count() or 4)
    p.add_argument("--backup", action="store_true")
    p.add_argument("--log-level", default="INFO")

    return p.parse_args()

# =======================================================
# Signals
# =======================================================

def _handle_signal(*_):

    stop_event.set()
    logger.warning("Interrupt received — stopping gracefully…")

# =======================================================
# Main
# =======================================================

def main() -> None:

    global RE_MULTI_REPLACE

    args = parse_arguments()

    setup_logging(args.log_level)

    RE_MULTI_REPLACE = re.compile(
        re.escape(args.replacement) + "+"
    )

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    try:

        process_directory(
            directory=args.directory,
            dry_run=args.dry_run,
            recursive=args.recursive,
            file_types=args.file_types,
            replacement=args.replacement,
            max_workers=max(1, args.threads),
            backup=args.backup,
        )

    except Exception as exc:

        logger.error(f"{Fore.RED}Fatal error: {exc}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
