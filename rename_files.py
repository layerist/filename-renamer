#!/usr/bin/env python3
"""
File Sanitizer — fast, no-overwrite, multithreaded filename sanitizer.

What it does:
- scans a directory for regular files;
- sanitizes unsafe filenames;
- renames files without overwriting existing files;
- supports dry-run, backups, extension filters, recursion and progress bar;
- handles Ctrl+C gracefully.
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import re
import shutil
import signal
import stat
import sys
import threading
import time
import unicodedata
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterator, Optional, Sequence, Set

try:
    from tqdm import tqdm
except ImportError:  # pragma: no cover
    tqdm = None

try:
    from colorama import Fore, init as colorama_init

    colorama_init(autoreset=True)
except ImportError:  # pragma: no cover
    class _Dummy:
        RED = GREEN = YELLOW = CYAN = RESET = ""

    Fore = _Dummy()

logger = logging.getLogger("file_sanitizer")
stop_event = threading.Event()

DEFAULT_ILLEGAL_CHARS: Set[str] = set(r'<>:"/\\|?*')
WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *(f"COM{i}" for i in range(1, 10)),
    *(f"LPT{i}" for i in range(1, 10)),
}
RE_SPACES = re.compile(r"\s+")


@dataclass(frozen=True)
class SanitizerConfig:
    replacement: str = "_"
    max_length: int = 255
    keep_hidden: bool = False
    normalize_unicode: bool = True


@dataclass(frozen=True)
class RenameResult:
    changed: bool
    old_path: str
    new_path: str


class Counter:
    __slots__ = ("processed", "renamed", "failed", "_lock")

    def __init__(self) -> None:
        self.processed = 0
        self.renamed = 0
        self.failed = 0
        self._lock = threading.Lock()

    def flush(self, processed_delta: int, renamed_delta: int, failed_delta: int) -> None:
        if not processed_delta and not renamed_delta and not failed_delta:
            return
        with self._lock:
            self.processed += processed_delta
            self.renamed += renamed_delta
            self.failed += failed_delta


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )


def validate_replacement(value: str) -> str:
    if any(ch in DEFAULT_ILLEGAL_CHARS for ch in value):
        raise ValueError(
            f"Replacement must not contain illegal filename chars: {sorted(DEFAULT_ILLEGAL_CHARS)}"
        )
    if any(ord(ch) < 32 for ch in value):
        raise ValueError("Replacement must not contain control characters")
    return value


def build_translation_table(replacement: str) -> dict[int, str]:
    table = {ord(ch): replacement for ch in DEFAULT_ILLEGAL_CHARS}
    table.update({i: replacement for i in range(32)})
    return table


def normalize_extensions(file_types: Optional[Sequence[str]]) -> Optional[Set[str]]:
    if not file_types:
        return None

    result: Set[str] = set()
    for item in file_types:
        for ext in item.split(","):
            ext = ext.strip().lower()
            if not ext:
                continue
            result.add("." + ext.lstrip("."))
    return result or None


def make_candidate_name(stem: str, suffix: str, index: int, max_length: int) -> str:
    extra = f"_{index}"
    allowed_stem_len = max(1, max_length - len(suffix) - len(extra))
    return f"{stem[:allowed_stem_len]}{extra}{suffix}"


def _sanitize_uncached(name: str, cfg: SanitizerConfig, translation_table: dict[int, str]) -> str:
    result = unicodedata.normalize("NFKC", name) if cfg.normalize_unicode else name
    result = result.translate(translation_table)
    result = RE_SPACES.sub(cfg.replacement, result)

    if cfg.replacement:
        result = re.sub(re.escape(cfg.replacement) + r"+", cfg.replacement, result)
        result = result.strip(cfg.replacement)

    result = result.rstrip(" .")

    if not result:
        result = "unnamed"

    stem, suffix = os.path.splitext(result)
    if os.name == "nt" and stem.upper() in WINDOWS_RESERVED:
        result = f"_{result}"
        stem, suffix = os.path.splitext(result)

    if len(result) > cfg.max_length:
        allowed_stem_len = max(1, cfg.max_length - len(suffix))
        result = f"{stem[:allowed_stem_len]}{suffix}"

    return result


@lru_cache(maxsize=200_000)
def _sanitize_cached(
    name: str,
    replacement: str,
    max_length: int,
    keep_hidden: bool,
    normalize_unicode: bool,
    translation_items: tuple[tuple[int, str], ...],
) -> str:
    cfg = SanitizerConfig(
        replacement=replacement,
        max_length=max_length,
        keep_hidden=keep_hidden,
        normalize_unicode=normalize_unicode,
    )
    return _sanitize_uncached(name, cfg, dict(translation_items))


def sanitize_filename(name: str, cfg: SanitizerConfig, translation_table: dict[int, str]) -> str:
    return _sanitize_cached(
        name,
        cfg.replacement,
        cfg.max_length,
        cfg.keep_hidden,
        cfg.normalize_unicode,
        tuple(sorted(translation_table.items())),
    )


def link_then_unlink(src: str, dst: str) -> None:
    """
    No-overwrite rename for regular files on the same filesystem.

    os.link() atomically creates the destination only if it does not exist.
    Then os.unlink() removes the old directory entry. This avoids the main
    os.replace()/POSIX os.rename() problem: silent overwrite of existing files.
    """
    os.link(src, dst)
    try:
        os.unlink(src)
    except Exception:
        # Roll back the newly created hardlink if the old name could not be removed.
        try:
            os.unlink(dst)
        finally:
            raise


def safe_rename_no_overwrite(src: str, dst: str, *, max_attempts: int = 10_000) -> str:
    directory = os.path.dirname(dst)
    filename = os.path.basename(dst)
    stem, suffix = os.path.splitext(filename)

    candidate = dst
    for index in range(max_attempts + 1):
        if index:
            candidate = os.path.join(
                directory,
                make_candidate_name(stem, suffix, index, 255),
            )

        try:
            link_then_unlink(src, candidate)
            return candidate
        except FileExistsError:
            continue
        except OSError as exc:
            # Windows / FS fallback: os.rename() refuses to overwrite on Windows.
            # On POSIX, use this fallback only after an explicit existence check.
            if os.name == "nt" and getattr(exc, "winerror", None) in {183}:
                continue
            if not os.path.exists(candidate):
                try:
                    os.rename(src, candidate)
                    return candidate
                except FileExistsError:
                    continue
            raise

    raise FileExistsError(f"Could not find free destination name for: {dst}")


def make_backup(filepath: str) -> str:
    backup_path = f"{filepath}.bak"
    if not os.path.exists(backup_path):
        shutil.copy2(filepath, backup_path)
        return backup_path

    dirname = os.path.dirname(backup_path)
    filename = os.path.basename(backup_path)
    stem, suffix = os.path.splitext(filename)

    for index in range(1, 10_000):
        candidate = os.path.join(dirname, f"{stem}_{index}{suffix}")
        if not os.path.exists(candidate):
            shutil.copy2(filepath, candidate)
            return candidate

    raise FileExistsError(f"Could not create backup for: {filepath}")


def rename_file(
    filepath: str,
    *,
    cfg: SanitizerConfig,
    translation_table: dict[int, str],
    dry_run: bool,
    backup: bool,
) -> RenameResult:
    dirname = os.path.dirname(filepath)
    old_name = os.path.basename(filepath)
    new_name = sanitize_filename(old_name, cfg, translation_table)

    if old_name == new_name:
        return RenameResult(False, filepath, filepath)

    target = os.path.join(dirname, new_name)

    try:
        if os.path.samefile(filepath, target):
            return RenameResult(False, filepath, filepath)
    except OSError:
        pass

    if dry_run:
        logger.info("[Dry-run] %s → %s", old_name, new_name)
        return RenameResult(True, filepath, target)

    if backup:
        make_backup(filepath)

    final_path = safe_rename_no_overwrite(filepath, target)
    logger.info("%sRenamed: %s → %s", Fore.GREEN, old_name, os.path.basename(final_path))
    return RenameResult(True, filepath, final_path)


def collect_files(
    root: str,
    *,
    recursive: bool,
    extensions: Optional[Set[str]],
    keep_hidden: bool,
) -> Iterator[str]:
    stack = [root]

    while stack and not stop_event.is_set():
        current = stack.pop()

        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    if stop_event.is_set():
                        break

                    name = entry.name
                    if not keep_hidden and name.startswith("."):
                        continue

                    try:
                        mode = entry.stat(follow_symlinks=False).st_mode
                    except OSError:
                        continue

                    if stat.S_ISDIR(mode):
                        if recursive:
                            stack.append(entry.path)
                        continue

                    if not stat.S_ISREG(mode):
                        continue

                    if extensions:
                        ext = os.path.splitext(name)[1].lower()
                        if ext not in extensions:
                            continue

                    yield entry.path
        except PermissionError:
            logger.warning("%sSkipped, no permission: %s", Fore.YELLOW, current)
        except FileNotFoundError:
            logger.warning("%sSkipped, disappeared: %s", Fore.YELLOW, current)
        except OSError as exc:
            logger.warning("%sSkipped: %s | %s", Fore.YELLOW, current, exc)


def worker(
    q: queue.Queue[Optional[str]],
    *,
    cfg: SanitizerConfig,
    translation_table: dict[int, str],
    dry_run: bool,
    backup: bool,
    counter: Counter,
) -> None:
    local_processed = 0
    local_renamed = 0
    local_failed = 0

    while True:
        try:
            item = q.get(timeout=0.2)
        except queue.Empty:
            if stop_event.is_set():
                continue
            continue

        if item is None:
            q.task_done()
            break

        try:
            result = rename_file(
                item,
                cfg=cfg,
                translation_table=translation_table,
                dry_run=dry_run,
                backup=backup,
            )
            local_processed += 1
            local_renamed += int(result.changed)
        except Exception as exc:
            local_processed += 1
            local_failed += 1
            logger.error("%sFailed: %s | %s", Fore.RED, item, exc)
        finally:
            q.task_done()

        if local_processed >= 100:
            counter.flush(local_processed, local_renamed, local_failed)
            local_processed = local_renamed = local_failed = 0

    counter.flush(local_processed, local_renamed, local_failed)


def process_directory(
    directory: str,
    *,
    dry_run: bool,
    recursive: bool,
    file_types: Optional[Sequence[str]],
    cfg: SanitizerConfig,
    max_workers: int,
    backup: bool,
) -> None:
    directory = os.path.abspath(directory)
    if not os.path.isdir(directory):
        raise ValueError(f"Invalid directory: {directory}")

    extensions = normalize_extensions(file_types)
    translation_table = build_translation_table(cfg.replacement)
    queue_size = max(5_000, max_workers * 1_000)
    q: queue.Queue[Optional[str]] = queue.Queue(maxsize=queue_size)
    counter = Counter()

    workers = [
        threading.Thread(
            target=worker,
            daemon=False,
            kwargs={
                "q": q,
                "cfg": cfg,
                "translation_table": translation_table,
                "dry_run": dry_run,
                "backup": backup,
                "counter": counter,
            },
        )
        for _ in range(max_workers)
    ]

    for thread in workers:
        thread.start()

    progress = tqdm(unit="file", desc="Scanning", dynamic_ncols=True) if tqdm else None
    start = time.perf_counter()

    try:
        for filepath in collect_files(
            directory,
            recursive=recursive,
            extensions=extensions,
            keep_hidden=cfg.keep_hidden,
        ):
            if stop_event.is_set():
                break
            q.put(filepath)
            if progress:
                progress.update(1)
    finally:
        for _ in workers:
            q.put(None)
        q.join()
        for thread in workers:
            thread.join()
        if progress:
            progress.close()

    elapsed = time.perf_counter() - start
    logger.info(
        "%sDone — Processed: %s | Renamed: %s | Failed: %s | Time: %.2fs",
        Fore.CYAN,
        f"{counter.processed:,}",
        f"{counter.renamed:,}",
        f"{counter.failed:,}",
        elapsed,
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Batch sanitize filenames safely")
    parser.add_argument("directory", help="Directory to process")
    parser.add_argument("--recursive", action="store_true", help="Process subdirectories")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without renaming")
    parser.add_argument("--backup", action="store_true", help="Create .bak copy before rename")
    parser.add_argument("--replacement", default="_", help="Replacement for illegal chars/spaces")
    parser.add_argument("--threads", type=int, default=min(32, (os.cpu_count() or 4) * 2))
    parser.add_argument("--file-types", type=lambda s: s.split(","), help="Comma-separated extensions: jpg,png,pdf")
    parser.add_argument("--max-length", type=int, default=255, help="Maximum filename length")
    parser.add_argument("--keep-hidden", action="store_true", help="Do not skip dotfiles")
    parser.add_argument("--no-unicode-normalize", action="store_true", help="Disable Unicode NFKC normalization")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser.parse_args()


def handle_signal(signum: int, *_: object) -> None:
    stop_event.set()
    logger.warning("Stopping gracefully after signal %s...", signum)


def main() -> int:
    args = parse_arguments()
    setup_logging(args.log_level)

    try:
        replacement = validate_replacement(args.replacement)
        if args.max_length < 16:
            raise ValueError("--max-length must be at least 16")

        cfg = SanitizerConfig(
            replacement=replacement,
            max_length=args.max_length,
            keep_hidden=args.keep_hidden,
            normalize_unicode=not args.no_unicode_normalize,
        )

        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)

        process_directory(
            directory=args.directory,
            dry_run=args.dry_run,
            recursive=args.recursive,
            file_types=args.file_types,
            cfg=cfg,
            max_workers=max(1, args.threads),
            backup=args.backup,
        )
        return 0
    except Exception as exc:
        logger.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
