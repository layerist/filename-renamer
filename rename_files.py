#!/usr/bin/env python3
"""
File Sanitizer v2 — safe and predictable batch filename sanitizer.

Key properties:
- never intentionally overwrites an existing file;
- plans collision-free names before execution, so dry-run matches real output;
- supports recursive scan, extension filters, backups and parallel execution;
- preserves file extensions while enforcing a configurable filename limit;
- handles Windows reserved names and trailing spaces/dots;
- stops cleanly on Ctrl+C and returns a meaningful exit code.

Python: 3.9+
Optional: tqdm, colorama
"""

from __future__ import annotations

import argparse
import errno
import logging
import os
import re
import shutil
import signal
import stat
import sys
import threading
import time
import unicodedata
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional, Sequence

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

LOGGER = logging.getLogger("file_sanitizer")
STOP_EVENT = threading.Event()

ILLEGAL_CHARS = frozenset('<>:"/\\|?*')
WINDOWS_RESERVED = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{i}" for i in range(1, 10)}
    | {f"LPT{i}" for i in range(1, 10)}
)
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class SanitizerConfig:
    replacement: str = "_"
    max_length: int = 255
    normalize_unicode: bool = True
    preserve_leading_dot: bool = False
    case_insensitive_collisions: bool = os.name == "nt"


@dataclass(frozen=True)
class RenamePlan:
    source: Path
    target: Path


@dataclass(frozen=True)
class RenameResult:
    source: Path
    target: Path
    renamed: bool
    backup_path: Optional[Path] = None
    error: Optional[str] = None


@dataclass
class Statistics:
    discovered: int = 0
    unchanged: int = 0
    planned: int = 0
    renamed: int = 0
    failed: int = 0


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )


def validate_replacement(value: str) -> str:
    if any(ch in ILLEGAL_CHARS for ch in value):
        raise argparse.ArgumentTypeError(
            "replacement contains an illegal filename character"
        )
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise argparse.ArgumentTypeError(
            "replacement must not contain control characters"
        )
    if value in {".", ".."}:
        raise argparse.ArgumentTypeError("replacement cannot be '.' or '..'")
    return value


def normalize_extensions(values: Optional[Sequence[str]]) -> Optional[frozenset[str]]:
    if not values:
        return None

    result: set[str] = set()
    for value in values:
        for item in value.split(","):
            item = item.strip().casefold()
            if item:
                result.add("." + item.lstrip("."))
    return frozenset(result) or None


def split_filename(name: str) -> tuple[str, str]:
    """Split a filename while treating dotfiles such as '.env' as extensionless."""
    if name.startswith(".") and name.count(".") == 1:
        return name, ""
    stem, suffix = os.path.splitext(name)
    return stem, suffix


def truncate_filename(stem: str, suffix: str, max_length: int, extra: str = "") -> str:
    """Apply a character limit while retaining suffix and collision marker."""
    suffix_budget = len(suffix) + len(extra)
    if suffix_budget >= max_length:
        # Extremely long extension: keep a minimal stem and the rightmost suffix part.
        suffix = suffix[-max(0, max_length - len(extra) - 1):]
        return f"x{extra}{suffix}"[:max_length]

    allowed = max_length - suffix_budget
    return f"{stem[:allowed]}{extra}{suffix}"


def sanitize_filename(name: str, cfg: SanitizerConfig) -> str:
    leading_dot = cfg.preserve_leading_dot and name.startswith(".")
    working = name[1:] if leading_dot else name

    if cfg.normalize_unicode:
        working = unicodedata.normalize("NFKC", working)

    translated = []
    for char in working:
        codepoint = ord(char)
        if char in ILLEGAL_CHARS or codepoint < 32 or codepoint == 127:
            translated.append(cfg.replacement)
        else:
            translated.append(char)
    working = "".join(translated)
    working = WHITESPACE_RE.sub(cfg.replacement, working)

    if cfg.replacement:
        working = re.sub(
            f"(?:{re.escape(cfg.replacement)})+",
            cfg.replacement,
            working,
        ).strip(cfg.replacement)

    # Windows forbids trailing spaces and dots; applying this everywhere makes
    # output portable and avoids surprising names.
    working = working.rstrip(" .")
    if not working:
        working = "unnamed"

    stem, suffix = split_filename(working)
    if stem.upper() in WINDOWS_RESERVED:
        stem = f"_{stem}"

    result = truncate_filename(stem, suffix, cfg.max_length)
    if leading_dot:
        # The leading dot counts towards the limit.
        result = "." + result[: max(1, cfg.max_length - 1)]

    if result in {"", ".", ".."}:
        result = "unnamed"
    return result


def collision_key(path: Path, case_insensitive: bool) -> str:
    value = os.path.abspath(os.fspath(path))
    return value.casefold() if case_insensitive else value


def candidate_name(base_name: str, index: int, max_length: int) -> str:
    stem, suffix = split_filename(base_name)
    return truncate_filename(stem, suffix, max_length, extra=f"_{index}")


def iter_files(
    root: Path,
    *,
    recursive: bool,
    extensions: Optional[frozenset[str]],
    include_hidden: bool,
    follow_directory_symlinks: bool,
) -> Iterator[Path]:
    stack = [root]
    visited_dirs: set[tuple[int, int]] = set()

    while stack and not STOP_EVENT.is_set():
        current = stack.pop()
        try:
            if follow_directory_symlinks:
                current_stat = current.stat()
                directory_id = (current_stat.st_dev, current_stat.st_ino)
                if directory_id in visited_dirs:
                    LOGGER.warning("%sSkipped directory cycle: %s", Fore.YELLOW, current)
                    continue
                visited_dirs.add(directory_id)

            with os.scandir(current) as entries:
                for entry in entries:
                    if STOP_EVENT.is_set():
                        return
                    if not include_hidden and entry.name.startswith("."):
                        continue

                    try:
                        if entry.is_dir(follow_symlinks=follow_directory_symlinks):
                            if recursive:
                                stack.append(Path(entry.path))
                            continue
                        mode = entry.stat(follow_symlinks=False).st_mode
                    except OSError as exc:
                        LOGGER.warning("%sSkipped: %s | %s", Fore.YELLOW, entry.path, exc)
                        continue

                    if not stat.S_ISREG(mode):
                        continue

                    if extensions is not None:
                        extension = Path(entry.name).suffix.casefold()
                        if extension not in extensions:
                            continue
                    yield Path(entry.path)
        except (PermissionError, FileNotFoundError) as exc:
            LOGGER.warning("%sSkipped directory: %s | %s", Fore.YELLOW, current, exc)
        except OSError as exc:
            LOGGER.warning("%sSkipped directory: %s | %s", Fore.YELLOW, current, exc)


def reserve_target(
    source: Path,
    desired_name: str,
    *,
    reserved: set[str],
    cfg: SanitizerConfig,
    max_attempts: int,
) -> Path:
    for index in range(max_attempts + 1):
        name = desired_name if index == 0 else candidate_name(desired_name, index, cfg.max_length)
        candidate = source.with_name(name)
        key = collision_key(candidate, cfg.case_insensitive_collisions)
        if _same_file(source, candidate):
            return candidate
        if key in reserved:
            continue
        if candidate.exists():
            reserved.add(key)
            continue
        reserved.add(key)
        return candidate
    raise FileExistsError(f"No free destination name for {source}")


def _same_file(first: Path, second: Path) -> bool:
    try:
        return os.path.samefile(first, second)
    except OSError:
        return False


def build_plan(
    files: Iterable[Path],
    *,
    cfg: SanitizerConfig,
    max_attempts: int,
    stats: Statistics,
) -> list[RenamePlan]:
    file_list = list(files)
    stats.discovered = len(file_list)

    # Reserve all original paths first. This prevents a rename from taking the
    # current name of another file that is scheduled to be renamed later.
    reserved = {
        collision_key(path, cfg.case_insensitive_collisions)
        for path in file_list
    }
    plans: list[RenamePlan] = []

    for source in file_list:
        desired_name = sanitize_filename(source.name, cfg)
        if desired_name == source.name:
            stats.unchanged += 1
            continue

        # Keep every original path reserved throughout planning. This avoids
        # order-dependent plans where one file targets another source's current
        # name and then fails when renames execute concurrently.
        target = reserve_target(
            source,
            desired_name,
            reserved=reserved,
            cfg=cfg,
            max_attempts=max_attempts,
        )
        plans.append(RenamePlan(source=source, target=target))

    stats.planned = len(plans)
    return plans


def atomic_copy_backup(source: Path, *, max_attempts: int) -> Path:
    """Create a backup using exclusive creation, avoiding check-then-copy races."""
    base = source.with_name(source.name + ".bak")
    for index in range(max_attempts + 1):
        candidate = base if index == 0 else source.with_name(f"{source.name}.bak_{index}")
        try:
            with source.open("rb") as src, candidate.open("xb") as dst:
                shutil.copyfileobj(src, dst, length=1024 * 1024)
            try:
                shutil.copystat(source, candidate, follow_symlinks=False)
            except OSError:
                LOGGER.debug("Could not copy all metadata to %s", candidate, exc_info=True)
            return candidate
        except FileExistsError:
            continue
        except Exception:
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                pass
            raise
    raise FileExistsError(f"Could not create backup for {source}")


def link_then_unlink(source: Path, target: Path) -> None:
    os.link(source, target)
    try:
        source.unlink()
    except Exception:
        try:
            target.unlink()
        except OSError:
            LOGGER.critical("Rollback failed; both names may exist: %s and %s", source, target)
        raise


def rename_no_overwrite(source: Path, target: Path) -> None:
    """Rename a regular file without intentionally replacing an existing path."""
    if os.name == "nt":
        # On Windows os.rename does not overwrite an existing destination.
        os.rename(source, target)
        return

    try:
        # Atomic destination creation on POSIX for regular files.
        link_then_unlink(source, target)
    except OSError as exc:
        if exc.errno in {errno.EPERM, errno.EOPNOTSUPP, errno.ENOTSUP}:
            # Some filesystems do not support hard links. There is no portable
            # atomic NOREPLACE rename in Python's stdlib, so fail safely instead
            # of risking an overwrite with os.rename().
            raise OSError(
                exc.errno,
                "filesystem does not support safe no-overwrite renaming",
                os.fspath(source),
            ) from exc
        raise


def execute_plan(
    plan: RenamePlan,
    *,
    backup: bool,
    max_attempts: int,
) -> RenameResult:
    if STOP_EVENT.is_set():
        return RenameResult(plan.source, plan.target, renamed=False, error="cancelled")

    backup_path: Optional[Path] = None
    try:
        if not plan.source.is_file():
            raise FileNotFoundError(f"source disappeared or is no longer regular: {plan.source}")
        if plan.target.exists() and not _same_file(plan.source, plan.target):
            raise FileExistsError(f"destination appeared after planning: {plan.target}")
        if backup:
            backup_path = atomic_copy_backup(plan.source, max_attempts=max_attempts)
        rename_no_overwrite(plan.source, plan.target)
        return RenameResult(plan.source, plan.target, renamed=True, backup_path=backup_path)
    except Exception as exc:
        return RenameResult(
            plan.source,
            plan.target,
            renamed=False,
            backup_path=backup_path,
            error=str(exc),
        )


def process_directory(
    directory: Path,
    *,
    dry_run: bool,
    recursive: bool,
    file_types: Optional[Sequence[str]],
    cfg: SanitizerConfig,
    workers: int,
    backup: bool,
    include_hidden: bool,
    follow_directory_symlinks: bool,
    max_attempts: int,
) -> Statistics:
    root = directory.expanduser().resolve()
    if not root.is_dir():
        raise ValueError(f"Invalid directory: {root}")

    extensions = normalize_extensions(file_types)
    stats = Statistics()
    started = time.perf_counter()

    files = iter_files(
        root,
        recursive=recursive,
        extensions=extensions,
        include_hidden=include_hidden,
        follow_directory_symlinks=follow_directory_symlinks,
    )
    plans = build_plan(
        files,
        cfg=cfg,
        max_attempts=max_attempts,
        stats=stats,
    )

    if dry_run:
        for plan in plans:
            LOGGER.info("[Dry-run] %s → %s", plan.source, plan.target.name)
        elapsed = time.perf_counter() - started
        LOGGER.info(
            "%sDry-run complete — Found: %s | Unchanged: %s | Would rename: %s | Time: %.2fs",
            Fore.CYAN,
            f"{stats.discovered:,}",
            f"{stats.unchanged:,}",
            f"{stats.planned:,}",
            elapsed,
        )
        return stats

    progress = tqdm(total=len(plans), unit="file", desc="Renaming", dynamic_ncols=True) if tqdm else None
    futures: list[Future[RenameResult]] = []

    try:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="sanitize") as executor:
            for plan in plans:
                if STOP_EVENT.is_set():
                    break
                futures.append(
                    executor.submit(
                        execute_plan,
                        plan,
                        backup=backup,
                        max_attempts=max_attempts,
                    )
                )

            for future in as_completed(futures):
                result = future.result()
                if result.renamed:
                    stats.renamed += 1
                    LOGGER.info(
                        "%sRenamed: %s → %s%s",
                        Fore.GREEN,
                        result.source.name,
                        result.target.name,
                        f" | backup: {result.backup_path.name}" if result.backup_path else "",
                    )
                else:
                    stats.failed += 1
                    LOGGER.error(
                        "%sFailed: %s → %s | %s",
                        Fore.RED,
                        result.source,
                        result.target.name,
                        result.error or "unknown error",
                    )
                if progress:
                    progress.update(1)
    finally:
        if progress:
            progress.close()

    elapsed = time.perf_counter() - started
    LOGGER.info(
        "%sDone — Found: %s | Unchanged: %s | Planned: %s | Renamed: %s | Failed: %s | Time: %.2fs",
        Fore.CYAN,
        f"{stats.discovered:,}",
        f"{stats.unchanged:,}",
        f"{stats.planned:,}",
        f"{stats.renamed:,}",
        f"{stats.failed:,}",
        elapsed,
    )
    return stats


def positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def parse_arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-sanitize filenames without overwriting existing files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("directory", type=Path, help="directory to process")
    parser.add_argument("-r", "--recursive", action="store_true", help="process subdirectories")
    parser.add_argument("-n", "--dry-run", action="store_true", help="show the exact rename plan")
    parser.add_argument("--backup", action="store_true", help="create a backup before each rename")
    parser.add_argument("--replacement", type=validate_replacement, default="_", help="replacement for unsafe characters and whitespace")
    parser.add_argument("--threads", type=positive_int, default=min(16, (os.cpu_count() or 4) * 2), help="parallel rename workers")
    parser.add_argument("--file-types", action="append", help="extensions, comma-separated; may be repeated")
    parser.add_argument("--max-length", type=positive_int, default=255, help="maximum filename length in characters")
    parser.add_argument("--include-hidden", action="store_true", help="include dotfiles and hidden dot-directories")
    parser.add_argument("--preserve-leading-dot", action="store_true", help="keep a leading dot when sanitizing dotfiles")
    parser.add_argument("--follow-directory-symlinks", action="store_true", help="follow symlinked directories with cycle detection")
    parser.add_argument("--case-sensitive-collisions", action="store_true", help="treat case-only names as distinct even on Windows")
    parser.add_argument("--no-unicode-normalize", action="store_true", help="disable Unicode NFKC normalization")
    parser.add_argument("--max-attempts", type=positive_int, default=10_000, help="maximum collision suffix attempts")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    args = parser.parse_args(argv)
    if args.max_length < 16:
        parser.error("--max-length must be at least 16")
    return args


def handle_signal(signum: int, *_: object) -> None:
    if STOP_EVENT.is_set():
        raise KeyboardInterrupt
    STOP_EVENT.set()
    LOGGER.warning("Stopping after signal %s; already-running renames will finish...", signum)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_arguments(argv)
    setup_logging(args.log_level)
    STOP_EVENT.clear()

    signal.signal(signal.SIGINT, handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_signal)

    cfg = SanitizerConfig(
        replacement=args.replacement,
        max_length=args.max_length,
        normalize_unicode=not args.no_unicode_normalize,
        preserve_leading_dot=args.preserve_leading_dot,
        case_insensitive_collisions=(os.name == "nt" and not args.case_sensitive_collisions),
    )

    try:
        stats = process_directory(
            args.directory,
            dry_run=args.dry_run,
            recursive=args.recursive,
            file_types=args.file_types,
            cfg=cfg,
            workers=args.threads,
            backup=args.backup,
            include_hidden=args.include_hidden,
            follow_directory_symlinks=args.follow_directory_symlinks,
            max_attempts=args.max_attempts,
        )
        if STOP_EVENT.is_set():
            return 130
        return 2 if stats.failed else 0
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted by user")
        return 130
    except Exception as exc:
        LOGGER.exception("Fatal error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
