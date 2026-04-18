from __future__ import annotations

"""
pipeline_common.py

Shared infrastructure helpers for repository ingestion pipelines.

This module intentionally contains only repository-agnostic concerns:
- timestamp utilities
- directory/bootstrap helpers
- logger setup
- streamed file download helper
- safe filesystem cleanup
- atomic JSON summary writing
- atomic project snapshot replace/purge orchestration

Repository-specific API calls, metadata extraction, scoring rules, and row
construction must remain in each repository pipeline script.
"""

import json
import logging
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set

import requests

from database_manager import DatabaseManagement


STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED_LOGIN_REQUIRED = "FAILED_LOGIN_REQUIRED"
STATUS_FAILED_TOO_LARGE = "FAILED_TOO_LARGE"
STATUS_FAILED_SERVER_UNRESPONSIVE = "FAILED_SERVER_UNRESPONSIVE"


@dataclass(frozen=True)
class RepoPaths:
    """
    Structured set of common filesystem paths for a repository pipeline.
    """

    repo_dir: str
    tmp_root: str
    log_dir: str
    summary_dir: str


def utc_now_iso() -> str:
    """
    Return the current UTC timestamp as an ISO 8601 string.
    """
    return datetime.utcnow().isoformat()


def normalize_search_terms(raw_value: Any, empty_value: str) -> List[str]:
    """
    Normalize repository search configuration into a list of search terms.

    Parameters
    ----------
    raw_value:
        Search configuration value from the repository script. This may be
        None, an empty list, a string, or another iterable of terms.
    empty_value:
        Repository-specific fallback value used when searching all items.

    Returns
    -------
    list[str]
        Normalized list of search terms.
    """
    if raw_value is None or raw_value == []:
        return [empty_value]

    if isinstance(raw_value, str):
        value = raw_value.strip()
        return [value] if value else [empty_value]

    terms = [str(term).strip() for term in raw_value if term is not None and str(term).strip()]
    return terms or [empty_value]


def ensure_directories(*paths: str) -> None:
    """
    Create one or more directories if they do not already exist.

    Parameters
    ----------
    *paths:
        Directory paths to create.
    """
    for path in paths:
        if path:
            Path(path).mkdir(parents=True, exist_ok=True)


def build_repo_paths(
    *,
    base_dir: str,
    repo_name: str,
    logs_dir: str = "logs",
    summaries_dir: str = "summaries",
) -> RepoPaths:
    """
    Build common directory paths for a repository pipeline and ensure they exist.

    Parameters
    ----------
    base_dir:
        Root download directory.
    repo_name:
        Repository-specific folder name.
    logs_dir:
        Shared logs directory.
    summaries_dir:
        Shared summaries directory.

    Returns
    -------
    RepoPaths
        Common path bundle for the repository.
    """
    repo_dir = str(Path(base_dir) / repo_name)
    tmp_root = str(Path(repo_dir) / "_tmp")
    log_dir = str(Path(logs_dir))
    summary_dir = str(Path(summaries_dir))

    ensure_directories(repo_dir, tmp_root, log_dir, summary_dir)

    return RepoPaths(
        repo_dir=repo_dir,
        tmp_root=tmp_root,
        log_dir=log_dir,
        summary_dir=summary_dir,
    )


def setup_logger(
    *,
    logger_name: str,
    log_file_path: str,
    console_level: int = logging.INFO,
    file_level: int = logging.DEBUG,
    propagate: bool = False,
) -> logging.Logger:
    """
    Configure and return a production-style logger.

    The logger writes concise progress logs to the console and detailed logs
    to a file. Repeated calls with the same logger name will not duplicate
    handlers.

    Parameters
    ----------
    logger_name:
        Logical logger name.
    log_file_path:
        Path to the log file.
    console_level:
        Log level for the console handler.
    file_level:
        Log level for the file handler.
    propagate:
        Whether logs should propagate to ancestor loggers.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    logger = logging.getLogger(logger_name)

    if getattr(logger, "_pipeline_common_configured", False):
        return logger

    ensure_directories(str(Path(log_file_path).parent))

    logger.setLevel(logging.DEBUG)
    logger.propagate = propagate
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger._pipeline_common_configured = True  # type: ignore[attr-defined]

    return logger


def safe_rmtree(path: str) -> None:
    """
    Safely remove a file or directory path if it exists.

    Parameters
    ----------
    path:
        File or directory path to remove.
    """
    target = Path(path)

    if target.is_dir():
        shutil.rmtree(target, ignore_errors=True)
    elif target.exists():
        try:
            target.unlink()
        except OSError:
            pass


def make_temp_dir(tmp_root: str, prefix: str, project_id: Any) -> str:
    """
    Create and return a unique temporary directory path for one project action.

    Parameters
    ----------
    tmp_root:
        Root temp directory for the repository.
    prefix:
        Prefix such as 'stage' or 'backup'.
    project_id:
        Repository-specific project identifier.

    Returns
    -------
    str
        Created temporary directory path.
    """
    ensure_directories(tmp_root)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    path = Path(tmp_root) / f"{prefix}_{project_id}_{timestamp}"
    path.mkdir(parents=True, exist_ok=True)
    return str(path)


def download_file(
    *,
    url: str,
    path: str,
    timeout: int,
    login_required_statuses: Optional[Set[int]] = None,
    too_large_statuses: Optional[Set[int]] = None,
    session: Optional[requests.Session] = None,
    logger: Optional[logging.Logger] = None,
) -> str:
    """
    Download a file with streaming and return a normalized pipeline status.

    Parameters
    ----------
    url:
        Source URL.
    path:
        Destination file path.
    timeout:
        Request timeout in seconds.
    login_required_statuses:
        HTTP status codes that should map to FAILED_LOGIN_REQUIRED.
    too_large_statuses:
        HTTP status codes that should map to FAILED_TOO_LARGE.
    session:
        Optional requests.Session for connection reuse.
    logger:
        Optional logger for detailed diagnostics.

    Returns
    -------
    str
        One of the normalized pipeline status strings.
    """
    login_required_statuses = set(login_required_statuses or set())
    too_large_statuses = set(too_large_statuses or set())

    destination = Path(path)
    ensure_directories(str(destination.parent))

    request_client = session or requests

    try:
        with request_client.get(url, stream=True, timeout=timeout) as response:
            status_code = response.status_code

            if status_code in login_required_statuses:
                if logger:
                    logger.warning(
                        "Download requires authentication: url=%s status=%s",
                        url,
                        status_code,
                    )
                safe_rmtree(str(destination))
                return STATUS_FAILED_LOGIN_REQUIRED

            if status_code in too_large_statuses:
                if logger:
                    logger.warning(
                        "Download rejected because file is too large: url=%s status=%s",
                        url,
                        status_code,
                    )
                safe_rmtree(str(destination))
                return STATUS_FAILED_TOO_LARGE

            if status_code != 200:
                if logger:
                    logger.error(
                        "Download failed with unexpected status: url=%s status=%s",
                        url,
                        status_code,
                    )
                safe_rmtree(str(destination))
                return STATUS_FAILED_SERVER_UNRESPONSIVE

            with destination.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        if logger:
            logger.debug("Downloaded file successfully: path=%s", destination)

        return STATUS_SUCCEEDED

    except requests.RequestException:
        safe_rmtree(str(destination))
        if logger:
            logger.exception("Network error while downloading file: url=%s", url)
        return STATUS_FAILED_SERVER_UNRESPONSIVE

    except Exception:
        safe_rmtree(str(destination))
        if logger:
            logger.exception("Unexpected error while downloading file: url=%s", url)
        return STATUS_FAILED_SERVER_UNRESPONSIVE


def write_json_summary(path: str, payload: Dict[str, Any]) -> None:
    """
    Write a JSON summary atomically.

    Parameters
    ----------
    path:
        Final JSON output path.
    payload:
        Summary payload to serialize.
    """
    target = Path(path)
    ensure_directories(str(target.parent))

    temp_name = f"{target.name}.tmp_{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}"
    temp_path = target.parent / temp_name

    with temp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)

    os.replace(temp_path, target)


def replace_project_snapshot(
    *,
    db: DatabaseManagement,
    project_key: Any,
    project_id: Any,
    repo_dir: str,
    tmp_root: str,
    current_folder_name: str,
    previous_folder_name: Optional[str],
    staged_project_dir: Optional[str],
    score_table: str,
    write_db_rows_fn: Callable[[], None],
    logger: logging.Logger,
    extra_tables: Optional[Sequence[str]] = None,
) -> bool:
    """
    Atomically replace a project's database rows and download folder.

    This function performs the shared orchestration only:
    - back up current project folder(s)
    - start transaction
    - delete existing project-linked rows
    - call repository-specific row writer
    - promote staged download directory
    - commit or roll back

    Parameters
    ----------
    db:
        Shared database manager.
    project_key:
        Repository-specific project key.
    project_id:
        Repository-specific project identifier.
    repo_dir:
        Repository download root.
    tmp_root:
        Repository temp root.
    current_folder_name:
        Folder name that should become the final active project folder.
    previous_folder_name:
        Existing folder name from the database, if any.
    staged_project_dir:
        Path to freshly staged files, or None if no staged files exist.
    score_table:
        Repository-specific score table name.
    write_db_rows_fn:
        Repository-specific callback that inserts fresh rows.
    logger:
        Configured logger.
    extra_tables:
        Optional extra project-linked tables to delete.

    Returns
    -------
    bool
        True if refresh succeeded, otherwise False.
    """
    final_project_dir = Path(repo_dir) / str(current_folder_name)
    staged_path = Path(staged_project_dir) if staged_project_dir else None

    folder_names_to_backup: List[str] = []
    if previous_folder_name:
        folder_names_to_backup.append(str(previous_folder_name))
    if str(current_folder_name) not in folder_names_to_backup:
        folder_names_to_backup.append(str(current_folder_name))

    backups: List[tuple[Path, Path]] = []
    promoted_new_dir = False

    try:
        for folder_name in folder_names_to_backup:
            original_path = Path(repo_dir) / folder_name
            if original_path.exists():
                backup_path = Path(make_temp_dir(tmp_root, "backup", project_id))
                safe_rmtree(str(backup_path))
                os.replace(original_path, backup_path)
                backups.append((original_path, backup_path))
                logger.debug(
                    "Backed up existing project folder: original=%s backup=%s",
                    original_path,
                    backup_path,
                )

        db.begin()
        db.delete_project_rows(project_key, score_table, extra_tables=extra_tables)
        write_db_rows_fn()

        if staged_path:
            os.replace(staged_path, final_project_dir)
            promoted_new_dir = True
            logger.debug(
                "Promoted staged project directory: staged=%s final=%s",
                staged_path,
                final_project_dir,
            )

        db.commit()
        logger.info("Project refresh succeeded: project_id=%s", project_id)

        for _, backup_path in backups:
            if backup_path.exists():
                safe_rmtree(str(backup_path))

        return True

    except Exception:
        logger.exception("Atomic project refresh failed: project_id=%s", project_id)
        db.rollback()

        if promoted_new_dir and final_project_dir.exists():
            safe_rmtree(str(final_project_dir))
        elif staged_path and staged_path.exists():
            safe_rmtree(str(staged_path))

        for original_path, backup_path in reversed(backups):
            if original_path.exists():
                safe_rmtree(str(original_path))
            if backup_path.exists():
                os.replace(backup_path, original_path)
                logger.debug(
                    "Restored backup project folder after rollback: original=%s backup=%s",
                    original_path,
                    backup_path,
                )

        return False


def purge_project_snapshot(
    *,
    db: DatabaseManagement,
    project_key: Any,
    project_id: Any,
    repo_dir: str,
    tmp_root: str,
    current_folder_name: Optional[str],
    score_table: str,
    logger: logging.Logger,
    extra_tables: Optional[Sequence[str]] = None,
) -> bool:
    """
    Atomically purge a project's local database rows and download folder.

    Parameters
    ----------
    db:
        Shared database manager.
    project_key:
        Repository-specific project key.
    project_id:
        Repository-specific project identifier.
    repo_dir:
        Repository download root.
    tmp_root:
        Repository temp root.
    current_folder_name:
        Existing active project folder name, if any.
    score_table:
        Repository-specific score table name.
    logger:
        Configured logger.
    extra_tables:
        Optional extra project-linked tables to delete.

    Returns
    -------
    bool
        True if purge succeeded, otherwise False.
    """
    backups: List[tuple[Path, Path]] = []

    try:
        if current_folder_name:
            current_folder_path = Path(repo_dir) / str(current_folder_name)
            if current_folder_path.exists():
                backup_path = Path(make_temp_dir(tmp_root, "backup", project_id))
                safe_rmtree(str(backup_path))
                os.replace(current_folder_path, backup_path)
                backups.append((current_folder_path, backup_path))
                logger.debug(
                    "Backed up project folder before purge: original=%s backup=%s",
                    current_folder_path,
                    backup_path,
                )

        db.begin()
        db.delete_project_rows(project_key, score_table, extra_tables=extra_tables)
        db.commit()
        logger.info("Project purge succeeded: project_id=%s", project_id)

        for _, backup_path in backups:
            if backup_path.exists():
                safe_rmtree(str(backup_path))

        return True

    except Exception:
        logger.exception("Project purge failed: project_id=%s", project_id)
        db.rollback()

        for original_path, backup_path in reversed(backups):
            if original_path.exists():
                safe_rmtree(str(original_path))
            if backup_path.exists():
                os.replace(backup_path, original_path)
                logger.debug(
                    "Restored project folder after failed purge: original=%s backup=%s",
                    original_path,
                    backup_path,
                )

        return False