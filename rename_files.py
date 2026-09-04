#!/usr/bin/env python3
"""
File Sanitizer v4 — safer deterministic batch filename sanitizer.

Main guarantees:
- never intentionally overwrites an existing path;
- dry-run uses the same precomputed rename/backup plan as real execution;
- original filenames, rename targets, and backup paths are reserved together;
- broken symlinks count as occupied destinations;
- regular-file identity is captured during planning and checked again before rename;
- filename limits are enforced by characters and, on POSIX, filesystem byte limits;
- supports recursion, extension filters, backups, parallel execution, and Ctrl+C;
- Windows reserved names / trailing spaces and dots are handled portably;
- planning order is deterministic even if os.scandir() order is not;
- Linux uses atomic renameat2(RENAME_NOREPLACE) when available;
- optional directory fsync can make completed renames more crash-durable.

Python: 3.9+
Optional: tqdm, colorama
"""

from __future__ import annotations

import argparse
import ctypes
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
from functools import lru_cache
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
WHITESPACE_RE = re.compile(r"\s+")

# Win32 device names are reserved even when followed by an extension.
WINDOWS_RESERVED = frozenset(
    {"CON", "PRN", "AUX", "NUL"}
    | {f"COM{i}" for i in range(1, 10)}
    | {f"LPT{i}" for i in range(1, 10)}
    | {"COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³"}
)


@dataclass(frozen=True)
class SanitizerConfig:
    replacement: str = "_"
    max_length: int = 255
    normalize_unicode: bool = True
    preserve_leading_dot: bool = False
    case_insensitive_collisions: bool = (os.name == "nt" or sys.platform == "darwin")


@dataclass(frozen=True)
class FileIdentity:
    device: int
    inode: int
    mode: int
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class RenamePlan:
    source: Path
    target: Path
    source_identity: FileIdentity
    backup_target: Optional[Path] = None


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
    cancelled: int = 0


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )


def validate_replacement(value: str) -> str:
    # Normalize the replacement itself. Otherwise a compatibility character
    # such as FULLWIDTH SOLIDUS could pass validation and later normalize into
    # an illegal character when names are processed elsewhere.
    normalized = unicodedata.normalize("NFKC", value)

    if len(normalized) > 32:
        raise argparse.ArgumentTypeError("replacement must be at most 32 characters")
    if any(ch in ILLEGAL_CHARS for ch in normalized):
        raise argparse.ArgumentTypeError(
            "replacement contains an illegal filename character"
        )
    if any(ord(ch) < 32 or ord(ch) == 127 for ch in normalized):
        raise argparse.ArgumentTypeError(
            "replacement must not contain control characters"
        )
    if normalized in {".", ".."}:
        raise argparse.ArgumentTypeError("replacement cannot be '.' or '..'")
    return normalized


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
    """Split a filename while treating '.env' as extensionless."""
    if name.startswith(".") and name.count(".") == 1:
        return name, ""
    return os.path.splitext(name)


def is_windows_reserved_stem(stem: str) -> bool:
    """Return True for Win32 device names, including trailing-dot/space forms."""
    normalized = unicodedata.normalize("NFKC", stem).rstrip(" .").upper()
    return normalized in WINDOWS_RESERVED


def path_lexists(path: Path) -> bool:
    """exists(), but broken symlinks are considered occupied."""
    return os.path.lexists(os.fspath(path))


@lru_cache(maxsize=1024)
def directory_name_max_bytes(directory: str) -> Optional[int]:
    """
    Return the filesystem component-name byte limit on POSIX.

    On Windows, Win32 naming rules are character-oriented enough for this
    sanitizer's configurable max_length, so no byte limit is applied here.
    """
    if os.name == "nt":
        return None
    try:
        value = os.pathconf(directory, "PC_NAME_MAX")
        return int(value) if value > 0 else None
    except (OSError, ValueError):
        return 255


def filesystem_encoded_length(value: str) -> int:
    encoding = sys.getfilesystemencoding() or "utf-8"
    return len(value.encode(encoding, errors="surrogateescape"))


def _fits_component(name: str, directory: Path, max_chars: int) -> bool:
    if len(name) > max_chars:
        return False
    byte_limit = directory_name_max_bytes(os.fspath(directory))
    return byte_limit is None or filesystem_encoded_length(name) <= byte_limit


def fit_filename(
    stem: str,
    suffix: str,
    *,
    directory: Path,
    max_chars: int,
    extra: str = "",
    prefix: str = "",
) -> str:
    """
    Fit a filename component while preferentially preserving:
    prefix + stem + collision marker + suffix.

    On POSIX this also respects the filesystem's byte-based NAME_MAX limit,
    which matters for multibyte Unicode filenames.
    """
    stem = stem or "x"

    def compose(s: str, x: str) -> str:
        return f"{prefix}{s}{extra}{x}"

    # Fast path.
    result = compose(stem, suffix)
    if _fits_component(result, directory, max_chars):
        return result

    # First shrink the stem, preserving at least one character.
    lo, hi = 1, len(stem)
    best_stem = stem[:1]
    while lo <= hi:
        mid = (lo + hi) // 2
        candidate_stem = stem[:mid]
        candidate = compose(candidate_stem, suffix)
        if _fits_component(candidate, directory, max_chars):
            best_stem = candidate_stem
            lo = mid + 1
        else:
            hi = mid - 1

    result = compose(best_stem, suffix)
    if _fits_component(result, directory, max_chars):
        return result

    # Extremely long suffix/marker: preserve the rightmost suffix characters.
    trimmed_suffix = suffix
    while trimmed_suffix:
        trimmed_suffix = trimmed_suffix[1:]
        result = compose("x", trimmed_suffix)
        if _fits_component(result, directory, max_chars):
            return result

    # A pathological replacement/extra may itself be huge. Trim stem-independent
    # material only as a final safety valve.
    result = f"{prefix}x{extra}"
    while result and not _fits_component(result, directory, max_chars):
        result = result[:-1]
    return result or "x"


def sanitize_filename(name: str, cfg: SanitizerConfig, directory: Path) -> str:
    leading_dot = cfg.preserve_leading_dot and name.startswith(".")
    working = name[1:] if leading_dot else name

    if cfg.normalize_unicode:
        working = unicodedata.normalize("NFKC", working)

    translated: list[str] = []
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

    # Portable Windows-compatible output.
    working = working.rstrip(" .")
    if not working:
        working = "unnamed"

    stem, suffix = split_filename(working)
    if is_windows_reserved_stem(stem):
        # Prefix the cleaned stem rather than the raw device spelling.
        stem = f"_{stem.rstrip(' .')}"

    result = fit_filename(
        stem,
        suffix,
        directory=directory,
        max_chars=cfg.max_length,
        prefix="." if leading_dot else "",
    )

    # A final portable cleanup in case truncation exposed an odd edge case.
    result = result.rstrip(" .")
    if result in {"", ".", ".."}:
        result = "unnamed"
    return result


def collision_key(path: Path, case_insensitive: bool) -> str:
    value = os.path.abspath(os.fspath(path))
    return value.casefold() if case_insensitive else value


def capture_identity(path: Path) -> FileIdentity:
    st = path.stat(follow_symlinks=False)
    return FileIdentity(
        device=st.st_dev,
        inode=st.st_ino,
        mode=st.st_mode,
        size=st.st_size,
        mtime_ns=st.st_mtime_ns,
    )


def same_identity(path: Path, expected: FileIdentity) -> bool:
    try:
        actual = capture_identity(path)
    except OSError:
        return False
    return (
        actual.device == expected.device
        and actual.inode == expected.inode
        and stat.S_IFMT(actual.mode) == stat.S_IFMT(expected.mode)
        and actual.size == expected.size
        and actual.mtime_ns == expected.mtime_ns
    )


def _same_file(first: Path, second: Path) -> bool:
    try:
        return os.path.samefile(first, second)
    except OSError:
        return False


def candidate_name(
    base_name: str,
    index: int,
    *,
    directory: Path,
    max_length: int,
) -> str:
    stem, suffix = split_filename(base_name)
    return fit_filename(
        stem,
        suffix,
        directory=directory,
        max_chars=max_length,
        extra=f"_{index}",
    )


def reserve_unique_path(
    source: Path,
    desired_name: str,
    *,
    reserved: set[str],
    cfg: SanitizerConfig,
    max_attempts: int,
    allow_same_source: bool,
) -> Path:
    directory = source.parent

    for index in range(max_attempts + 1):
        name = (
            desired_name
            if index == 0
            else candidate_name(
                desired_name,
                index,
                directory=directory,
                max_length=cfg.max_length,
            )
        )
        candidate = source.with_name(name)
        key = collision_key(candidate, cfg.case_insensitive_collisions)

        if allow_same_source and _same_file(source, candidate):
            return candidate
        if key in reserved:
            continue
        if path_lexists(candidate):
            reserved.add(key)
            continue

        reserved.add(key)
        return candidate

    raise FileExistsError(f"No free destination name for {source}")


def make_backup_basename(source: Path, cfg: SanitizerConfig) -> str:
    # Backups are newly-created names too, so sanitize them instead of carrying
    # unsafe POSIX-only characters (for example ':') into backup filenames.
    safe_name = sanitize_filename(source.name, cfg, source.parent)
    stem, suffix = split_filename(safe_name)
    # Keep the extension at the end: "report.bak.txt" remains recognizable by
    # extension-based tools.
    return fit_filename(
        stem,
        suffix,
        directory=source.parent,
        max_chars=cfg.max_length,
        extra=".bak",
    )


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
                    LOGGER.warning(
                        "%sSkipped directory cycle: %s",
                        Fore.YELLOW,
                        current,
                    )
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

                        st = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        LOGGER.warning(
                            "%sSkipped: %s | %s",
                            Fore.YELLOW,
                            entry.path,
                            exc,
                        )
                        continue

                    # Explicitly skip symlinks and every non-regular object.
                    if not stat.S_ISREG(st.st_mode):
                        continue

                    if extensions is not None:
                        extension = Path(entry.name).suffix.casefold()
                        if extension not in extensions:
                            continue

                    yield Path(entry.path)

        except (PermissionError, FileNotFoundError) as exc:
            LOGGER.warning(
                "%sSkipped directory: %s | %s",
                Fore.YELLOW,
                current,
                exc,
            )
        except OSError as exc:
            LOGGER.warning(
                "%sSkipped directory: %s | %s",
                Fore.YELLOW,
                current,
                exc,
            )


def build_plan(
    files: Iterable[Path],
    *,
    cfg: SanitizerConfig,
    max_attempts: int,
    backup: bool,
    stats: Statistics,
) -> list[RenamePlan]:
    file_list = list(files)
    # os.scandir() order is filesystem-dependent. Collision suffix assignment
    # must not depend on that order if dry-run and repeated executions are to be
    # deterministic.
    file_list.sort(
        key=lambda p: (
            collision_key(p, cfg.case_insensitive_collisions),
            os.fspath(p),
        )
    )
    stats.discovered = len(file_list)

    # Reserve every original source path, even unchanged files. This prevents
    # one rename from taking another file's current name.
    reserved = {
        collision_key(path, cfg.case_insensitive_collisions)
        for path in file_list
    }

    plans: list[RenamePlan] = []

    for source in file_list:
        try:
            identity = capture_identity(source)
        except OSError as exc:
            stats.failed += 1
            LOGGER.error("%sCould not stat %s | %s", Fore.RED, source, exc)
            continue

        if not stat.S_ISREG(identity.mode):
            stats.failed += 1
            LOGGER.error("%sSource is no longer a regular file: %s", Fore.RED, source)
            continue

        desired_name = sanitize_filename(source.name, cfg, source.parent)
        if desired_name == source.name:
            stats.unchanged += 1
            continue

        target = reserve_unique_path(
            source,
            desired_name,
            reserved=reserved,
            cfg=cfg,
            max_attempts=max_attempts,
            allow_same_source=True,
        )

        backup_target: Optional[Path] = None
        if backup:
            backup_name = make_backup_basename(source, cfg)
            backup_target = reserve_unique_path(
                source,
                backup_name,
                reserved=reserved,
                cfg=cfg,
                max_attempts=max_attempts,
                allow_same_source=False,
            )

        plans.append(
            RenamePlan(
                source=source,
                target=target,
                source_identity=identity,
                backup_target=backup_target,
            )
        )

    stats.planned = len(plans)
    return plans


def copy_backup_exclusive(
    source: Path,
    target: Path,
    *,
    fsync_backup: bool,
) -> Path:
    """
    Copy a backup to an already-planned unique path using exclusive creation.

    Exclusive 'xb' protects against an external process creating the same path
    after planning.
    """
    try:
        with source.open("rb") as src, target.open("xb") as dst:
            shutil.copyfileobj(src, dst, length=1024 * 1024)

            # Apply metadata before the optional fsync so durability includes
            # the metadata changes made by copystat as far as the platform
            # permits.
            try:
                shutil.copystat(source, target, follow_symlinks=False)
            except OSError:
                LOGGER.debug(
                    "Could not copy all metadata to %s",
                    target,
                    exc_info=True,
                )

            dst.flush()
            if fsync_backup:
                os.fsync(dst.fileno())

        return target

    except Exception:
        try:
            target.unlink(missing_ok=True)
        except OSError:
            LOGGER.exception("Could not remove incomplete backup: %s", target)
        raise


def link_then_unlink(source: Path, target: Path) -> None:
    """
    Portable POSIX no-overwrite fallback for regular files.

    os.link() atomically fails if target already exists. This is not a true
    atomic rename because both names briefly coexist, so Linux first attempts
    renameat2(RENAME_NOREPLACE).
    """
    os.link(source, target, follow_symlinks=False)
    try:
        source.unlink()
    except Exception:
        try:
            target.unlink()
        except OSError:
            LOGGER.critical(
                "Rollback failed; both names may exist: %s and %s",
                source,
                target,
            )
        raise


_RENAMEAT2_UNAVAILABLE_ERRNOS = {
    errno.ENOSYS,
    errno.EINVAL,
    errno.EOPNOTSUPP,
    getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
}
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100


def linux_rename_noreplace(source: Path, target: Path) -> bool:
    """
    Attempt Linux renameat2(..., RENAME_NOREPLACE).

    Return True on success and False only when the API/filesystem does not
    support the operation. Other errors are propagated.
    """
    if not sys.platform.startswith("linux"):
        return False

    try:
        libc = ctypes.CDLL(None, use_errno=True)
        renameat2 = libc.renameat2
    except (AttributeError, OSError):
        return False

    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int

    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(target),
        _RENAME_NOREPLACE,
    )
    if result == 0:
        return True

    error = ctypes.get_errno()
    if error in _RENAMEAT2_UNAVAILABLE_ERRNOS:
        return False
    raise OSError(error, os.strerror(error), os.fspath(source), os.fspath(target))


def rename_no_overwrite(source: Path, target: Path) -> None:
    """Rename a regular file without intentionally replacing an existing path."""
    if os.name == "nt":
        # Win32 os.rename() fails when the destination already exists.
        os.rename(source, target)
        return

    if linux_rename_noreplace(source, target):
        return

    try:
        link_then_unlink(source, target)
    except OSError as exc:
        if exc.errno in {
            errno.EXDEV,
            errno.EPERM,
            errno.EOPNOTSUPP,
            getattr(errno, "ENOTSUP", errno.EOPNOTSUPP),
        }:
            raise OSError(
                exc.errno,
                "filesystem does not support a safe no-overwrite rename fallback",
                os.fspath(source),
            ) from exc
        raise


def fsync_directory(directory: Path) -> None:
    """Best-effort directory fsync for rename/link durability on POSIX."""
    if os.name == "nt":
        return

    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY

    fd = os.open(directory, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def execute_plan(
    plan: RenamePlan,
    *,
    fsync_backup: bool,
    fsync_dir: bool,
) -> RenameResult:
    if STOP_EVENT.is_set():
        return RenameResult(
            plan.source,
            plan.target,
            renamed=False,
            error="cancelled",
        )

    backup_path: Optional[Path] = None

    try:
        if not same_identity(plan.source, plan.source_identity):
            raise RuntimeError(
                f"source changed, disappeared, or was replaced after planning: {plan.source}"
            )

        if path_lexists(plan.target) and not _same_file(plan.source, plan.target):
            raise FileExistsError(
                f"destination appeared after planning: {plan.target}"
            )

        if plan.backup_target is not None:
            if path_lexists(plan.backup_target):
                raise FileExistsError(
                    f"backup destination appeared after planning: {plan.backup_target}"
                )
            backup_path = copy_backup_exclusive(
                plan.source,
                plan.backup_target,
                fsync_backup=fsync_backup,
            )

            # Re-check after copying: a long backup operation gives external
            # processes time to replace the source path.
            if not same_identity(plan.source, plan.source_identity):
                raise RuntimeError(
                    f"source changed while backup was being created: {plan.source}"
                )

        rename_no_overwrite(plan.source, plan.target)

        # Verify that the object which arrived at the target is still the
        # object captured during planning. This cannot eliminate every hostile
        # TOCTOU race, but it catches accidental external replacement.
        if not same_identity(plan.target, plan.source_identity):
            raise RuntimeError(
                f"renamed target identity mismatch: {plan.target}"
            )

        if fsync_dir:
            fsync_directory(plan.target.parent)

        return RenameResult(
            plan.source,
            plan.target,
            renamed=True,
            backup_path=backup_path,
        )

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
    fsync_backup: bool,
    fsync_dir: bool,
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
        backup=backup,
        stats=stats,
    )

    if dry_run:
        for plan in plans:
            backup_info = (
                f" | backup: {plan.backup_target.name}"
                if plan.backup_target is not None
                else ""
            )
            LOGGER.info(
                "[Dry-run] %s → %s%s",
                plan.source,
                plan.target.name,
                backup_info,
            )

        elapsed = time.perf_counter() - started
        LOGGER.info(
            "%sDry-run complete — Found: %s | Unchanged: %s | "
            "Would rename: %s | Pre-plan failures: %s | Time: %.2fs",
            Fore.CYAN,
            f"{stats.discovered:,}",
            f"{stats.unchanged:,}",
            f"{stats.planned:,}",
            f"{stats.failed:,}",
            elapsed,
        )
        return stats

    progress = (
        tqdm(
            total=len(plans),
            unit="file",
            desc="Renaming",
            dynamic_ncols=True,
        )
        if tqdm
        else None
    )

    futures: dict[Future[RenameResult], RenamePlan] = {}

    try:
        with ThreadPoolExecutor(
            max_workers=workers,
            thread_name_prefix="sanitize",
        ) as executor:
            for plan in plans:
                if STOP_EVENT.is_set():
                    stats.cancelled += 1
                    continue

                future = executor.submit(
                    execute_plan,
                    plan,
                    fsync_backup=fsync_backup,
                    fsync_dir=fsync_dir,
                )
                futures[future] = plan

            for future in as_completed(futures):
                plan = futures[future]

                try:
                    result = future.result()
                except BaseException as exc:
                    stats.failed += 1
                    LOGGER.exception(
                        "%sWorker crashed: %s → %s | %s",
                        Fore.RED,
                        plan.source,
                        plan.target.name,
                        exc,
                    )
                    if progress:
                        progress.update(1)
                    continue

                if result.renamed:
                    stats.renamed += 1
                    LOGGER.info(
                        "%sRenamed: %s → %s%s",
                        Fore.GREEN,
                        result.source.name,
                        result.target.name,
                        (
                            f" | backup: {result.backup_path.name}"
                            if result.backup_path
                            else ""
                        ),
                    )
                elif result.error == "cancelled":
                    stats.cancelled += 1
                    LOGGER.warning(
                        "%sCancelled: %s",
                        Fore.YELLOW,
                        result.source,
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
        "%sDone — Found: %s | Unchanged: %s | Planned: %s | "
        "Renamed: %s | Failed: %s | Cancelled: %s | Time: %.2fs",
        Fore.CYAN,
        f"{stats.discovered:,}",
        f"{stats.unchanged:,}",
        f"{stats.planned:,}",
        f"{stats.renamed:,}",
        f"{stats.failed:,}",
        f"{stats.cancelled:,}",
        elapsed,
    )
    return stats


def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def parse_arguments(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch-sanitize filenames without overwriting existing paths",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "directory",
        type=Path,
        help="directory to process",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="process subdirectories",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="show the exact precomputed rename/backup plan",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="create a backup before each rename",
    )
    parser.add_argument(
        "--fsync-backup",
        action="store_true",
        help="fsync backup contents before rename (slower, stronger durability)",
    )
    parser.add_argument(
        "--fsync-directory",
        action="store_true",
        help="fsync the containing directory after each successful rename on POSIX",
    )
    parser.add_argument(
        "--replacement",
        type=validate_replacement,
        default="_",
        help="replacement for unsafe characters and whitespace",
    )
    parser.add_argument(
        "--threads",
        type=positive_int,
        default=min(16, (os.cpu_count() or 4) * 2),
        help="parallel rename workers",
    )
    parser.add_argument(
        "--file-types",
        action="append",
        help="extensions, comma-separated; may be repeated",
    )
    parser.add_argument(
        "--max-length",
        type=positive_int,
        default=255,
        help="maximum filename length in characters",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="include dotfiles and hidden dot-directories",
    )
    parser.add_argument(
        "--preserve-leading-dot",
        action="store_true",
        help="keep a leading dot when sanitizing dotfiles",
    )
    parser.add_argument(
        "--follow-directory-symlinks",
        action="store_true",
        help="follow symlinked directories with cycle detection",
    )
    parser.add_argument(
        "--case-sensitive-collisions",
        action="store_true",
        help=(
            "treat case-only paths as distinct during planning; "
            "unsafe on normal Windows and macOS default filesystems"
        ),
    )
    parser.add_argument(
        "--no-unicode-normalize",
        action="store_true",
        help="disable Unicode NFKC normalization",
    )
    parser.add_argument(
        "--max-attempts",
        type=positive_int,
        default=10_000,
        help="maximum collision suffix attempts",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )

    args = parser.parse_args(argv)

    if args.max_length < 16:
        parser.error("--max-length must be at least 16")

    if args.fsync_backup and not args.backup:
        parser.error("--fsync-backup requires --backup")

    return args


def handle_signal(signum: int, *_: object) -> None:
    if STOP_EVENT.is_set():
        raise KeyboardInterrupt

    STOP_EVENT.set()
    LOGGER.warning(
        "Stopping after signal %s; already-running filesystem operations will finish...",
        signum,
    )


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
        case_insensitive_collisions=(
            (os.name == "nt" or sys.platform == "darwin")
            and not args.case_sensitive_collisions
        ),
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
            fsync_backup=args.fsync_backup,
            fsync_dir=args.fsync_directory,
            include_hidden=args.include_hidden,
            follow_directory_symlinks=args.follow_directory_symlinks,
            max_attempts=args.max_attempts,
        )

        if STOP_EVENT.is_set() or stats.cancelled:
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
