from __future__ import annotations

import html
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import requests

from database_manager import DatabaseManagement
from pipeline_common import (
    STATUS_FAILED_LOGIN_REQUIRED,
    STATUS_FAILED_SERVER_UNRESPONSIVE,
    STATUS_FAILED_TOO_LARGE,
    STATUS_SUCCEEDED,
    RepoPaths,
    build_repo_paths,
    download_file,
    make_temp_dir,
    normalize_search_terms,
    safe_rmtree,
    setup_logger,
    utc_now_iso,
    write_json_summary,
)

ProjectDict = Dict[str, Any]
ScoreResult = Dict[str, Any]
FileRow = Tuple[Optional[str], Optional[str], str]

NEGATIVE_KIND_OF_DATA = {"numeric", "geospatial", "software", "interactive resource"}


@dataclass(frozen=True)
class PipelineConfig:
    api_base: str
    query: Any
    per_page: int
    timeout: int
    base_dir: str
    repo_name: str
    db_path: str
    repository_id: int
    repository_url: str
    download_method: str
    schema_path: str
    log_file_name: str
    score_table: str


def build_config() -> PipelineConfig:
    script_dir = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    return PipelineConfig(
        api_base="https://data.aussda.at/api",
        query=[],
        per_page=100,
        timeout=60,
        base_dir="downloads",
        repo_name="aussda",
        db_path="23041405-seeding.db",
        repository_id=12,
        repository_url="https://data.aussda.at",
        download_method="API-CALL",
        schema_path=str(script_dir / "schema.sql"),
        log_file_name="aussda.log",
        score_table="qualitative_scores_aussda",
    )


def fetch_search_page(
    *,
    session: requests.Session,
    config: PipelineConfig,
    start: int,
    query_string: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    url = f"{config.api_base}/search"
    params = {"q": query_string, "type": "dataset", "start": start, "per_page": config.per_page}
    logger.debug("Fetching search page: url=%s params=%s", url, params)
    response = session.get(url, params=params, timeout=config.timeout)
    response.raise_for_status()
    return response.json()


def fetch_dataset(
    *,
    session: requests.Session,
    config: PipelineConfig,
    persistent_id: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    url = f"{config.api_base}/datasets/:persistentId/"
    params = {"persistentId": persistent_id}
    logger.debug("Fetching dataset: url=%s params=%s", url, params)
    response = session.get(url, params=params, timeout=config.timeout)
    response.raise_for_status()
    return response.json()


def flatten_value(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(flatten_value(item))
        return out
    if isinstance(value, dict):
        return flatten_value(value["value"]) if "value" in value else []
    text = str(value).strip()
    return [text] if text else []


def normalize_text(value: Any) -> str:
    return " ".join(flatten_value(value)).lower().strip()


def any_contains(text: str, patterns: List[str]) -> bool:
    return any(pattern.lower() in text for pattern in patterns)


def get_field_value(fields: List[Dict[str, Any]], type_name: str) -> Any:
    for field in fields:
        if field.get("typeName") == type_name:
            return field.get("value")
    return None


def get_description(fields: List[Dict[str, Any]]) -> str:
    for field in fields:
        if field.get("typeName") == "dsDescription":
            values = field.get("value") or []
            parts: List[str] = []
            for item in values:
                if not isinstance(item, dict):
                    continue
                desc = item.get("dsDescriptionValue", {}).get("value")
                if desc:
                    parts.append(str(desc).strip())
            return " ".join(parts).strip()
    return ""


def get_authors(fields: List[Dict[str, Any]]) -> List[str]:
    authors: List[str] = []
    author_field = get_field_value(fields, "author")
    if isinstance(author_field, list):
        for item in author_field:
            if isinstance(item, dict):
                name = item.get("authorName", {}).get("value")
                if name:
                    authors.append(str(name).strip())
    return authors


def get_contacts(fields: List[Dict[str, Any]]) -> List[str]:
    contacts: List[str] = []
    contact_field = get_field_value(fields, "datasetContact")
    if isinstance(contact_field, list):
        for item in contact_field:
            if isinstance(item, dict):
                name = item.get("datasetContactName", {}).get("value")
                if name:
                    contacts.append(str(name).strip())
    return contacts


def get_depositor(fields: List[Dict[str, Any]]) -> str:
    values = flatten_value(get_field_value(fields, "depositor"))
    return values[0].strip() if values else ""


def get_field_text(fields: List[Dict[str, Any]], type_name: str) -> str:
    return " ".join(flatten_value(get_field_value(fields, type_name)))


def get_file_ext(name: str) -> str:
    return name.rsplit(".", 1)[-1].lower() if name and "." in name else "unknown"


def clean_license_text(value: Any) -> str:
    text = " ".join(flatten_value(value))
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip(" .;,-")


def normalize_license(value: Any) -> str:
    raw = clean_license_text(value)
    if not raw:
        return ""

    lower = raw.lower()
    exact_mappings = [
        ("aussda scientific use licence", "AUSSDA Scientific Use Licence"),
        ("terms of use agreement for gfk data", "GfK Terms of Use"),
        ("open government licence", "Open Government Licence"),
        ("odbl-1.0", "ODbL-1.0"),
        ("odbl", "ODbL"),
        ("odc-by-1.0", "ODC-By-1.0"),
        ("odc-by", "ODC-By"),
        ("pddl", "PDDL"),
        ("cc0", "CC0"),
    ]
    for pattern, normalized in exact_mappings:
        if pattern in lower:
            return normalized

    cc_patterns = [
        ("creative commons attribution-noncommercial-no derivatives", "CC BY-NC-ND"),
        ("creative commons attribution-noncommercial-noderivatives", "CC BY-NC-ND"),
        ("cc by-nc-nd", "CC BY-NC-ND"),
        ("creative commons attribution-no derivatives", "CC BY-ND"),
        ("creative commons attribution-noderivatives", "CC BY-ND"),
        ("cc by-nd", "CC BY-ND"),
        ("creative commons attribution-noncommercial-sharealike", "CC BY-NC-SA"),
        ("creative commons attribution-noncommercial-share alike", "CC BY-NC-SA"),
        ("cc by-nc-sa", "CC BY-NC-SA"),
        ("creative commons attribution-noncommercial", "CC BY-NC"),
        ("creative commons attribution-non commercial", "CC BY-NC"),
        ("cc by-nc", "CC BY-NC"),
        ("creative commons attribution-sharealike", "CC BY-SA"),
        ("creative commons attribution-share alike", "CC BY-SA"),
        ("cc by-sa", "CC BY-SA"),
        ("creative commons attribution", "CC BY"),
        ("cc by", "CC BY"),
    ]
    for pattern, base in cc_patterns:
        if pattern in lower:
            version_match = re.search(r"\b([1-9](?:\.\d+)?)\b", raw)
            return f"{base} {version_match.group(1)}" if version_match else base

    return raw


def has_files_text(value: Optional[bool]) -> str:
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "unknown"


def log_project_decision(
    logger: logging.Logger,
    *,
    project_id: str,
    action: str,
    has_files: Optional[bool],
    score: Optional[int] = None,
    label: Optional[str] = None,
    reason: Optional[str] = None,
    level: int = logging.INFO,
) -> None:
    parts = [f"project={project_id}", f"action={action}"]
    if reason:
        parts.append(f"reason={reason}")
    else:
        if score is not None:
            parts.append(f"score={score}")
        if label:
            parts.append(f"label={label}")
    parts.append(f"has_files={has_files_text(has_files)}")
    logger.log(level, " | ".join(parts))


def log_project_summary(logger: logging.Logger, project: ProjectDict) -> None:
    logger.debug(
        "Project metadata: source_project_id=%s title=%s kindOfData=%s collectionMode=%s files=%s file_categories=%s file_description=%s license=%s",
        project["source_project_id"],
        project["title"] or "<no-title>",
        project.get("kindOfData"),
        project.get("collectionMode"),
        len(project.get("files", [])),
        project.get("file_categories", []),
        project.get("file_description", []),
        project.get("license"),
    )


def log_score_summary(logger: logging.Logger, source_project_id: str, score_result: ScoreResult) -> None:
    d = score_result["details"]
    logger.debug(
        "Score breakdown: source_project_id=%s total=%s label=%s description=%s kindOfData=%s collectionMode=%s fileCategories=%s fileDescription=%s reasons=%s",
        source_project_id,
        score_result["total_score"],
        score_result["label"],
        d["description_score"],
        d["kind_of_data_score"],
        d["collection_mode_score"],
        d["file_categories_score"],
        d["file_description_score"],
        " | ".join(score_result["reasons"]),
    )


def score_description(description: Any) -> Tuple[int, List[str]]:
    text = normalize_text(description)
    if not text:
        return -2, ["Description missing"]
    if any_contains(text, ["qualitative", "interview", "transcript", "focus group"]):
        return 4, ["Description contains strong qualitative keywords"]
    return -1, ["Description does not contain strong qualitative keywords"]


def score_kind_of_data(kind_of_data: Any) -> Tuple[int, List[str]]:
    text = normalize_text(kind_of_data)
    if not text:
        return -1, ["KindOfData missing"]
    if any_contains(text, ["text"]):
        if any_contains(text, ["numeric", "still image"]):
            return 2, ["KindOfData is mixed and includes Text"]
        return 4, ["KindOfData = Text"]
    if any_contains(text, ["still image"]):
        return 1, ["KindOfData is a weak qualitative signal"]
    if any_contains(text, ["numeric", "geospatial", "software", "interactive resource"]):
        return -2, ["KindOfData strongly suggests non-qualitative data"]
    return -1, ["KindOfData does not match qualitative-oriented categories"]


def get_negative_kind_of_data_reasons(kind_of_data: Any) -> List[str]:
    text = normalize_text(kind_of_data)
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"[;,/|]+", text) if part.strip()]
    if not parts:
        return []
    unique_parts = list(dict.fromkeys(parts))
    return sorted(unique_parts) if all(part in NEGATIVE_KIND_OF_DATA for part in unique_parts) else []


def should_skip_kind_of_data(kind_of_data: Any) -> bool:
    return bool(get_negative_kind_of_data_reasons(kind_of_data))


def score_collection_mode(collection_mode: Any) -> Tuple[int, List[str]]:
    text = normalize_text(collection_mode)
    if not text:
        return -1, ["CollectionMode missing"]

    strong = ["interview", "focus group", "transcription"]
    medium = ["observation", "content coding", "content"]
    negative = ["questionnaire", "measurement", "measurements", "test", "tests", "experiment", "automated data extraction"]

    if any_contains(text, strong):
        return 4, ["CollectionMode contains strong qualitative methods"]
    if any_contains(text, medium):
        return 2, ["CollectionMode contains medium qualitative methods"]
    if any_contains(text, negative):
        return -2, ["CollectionMode suggests non-qualitative / survey / experimental data"]
    return -1, ["CollectionMode is not clearly qualitative"]


def score_file_categories(categories: Any) -> Tuple[int, List[str]]:
    text = normalize_text(categories)
    if not text:
        return -1, ["File categories missing"]

    strong_support = ["data", "rtf", "interview"]
    medium_support = [
        "documentation", "manual", "pdf", "word", "text", "txt",
        "questionnaire", "codebook", "method report", "field report",
        "research report", "interviewer manual",
    ]

    if any_contains(text, ["other"]):
        return -1, ["File categories contain Other"]

    score = -1
    reasons: List[str] = []
    if any_contains(text, strong_support):
        score = max(score, 2)
        reasons.append("File categories contain strong supportive terms")
    if any_contains(text, medium_support):
        score = max(score, 1)
        reasons.append("File categories contain supportive documentation terms")
    if score == -1:
        return -1, ["File categories do not provide qualitative evidence"]
    return score, reasons


def score_file_description(description: Any) -> Tuple[int, List[str]]:
    text = normalize_text(description)
    if not text:
        return -1, ["File description missing"]

    strong = ["interview", "interviews", "transcript", "transcripts", "focus group", "qualitative", "open answers"]
    medium = ["questionnaire", "manual", "report", "form", "scheme", "protocol", "plan", "ethical committee vote", "preparation"]
    negative = ["other", "numeric", "variables", "observations", "stata", "spss", "csv", "excel", "tabulation", "replication", "syntax", "code", "script", "weights"]

    if any_contains(text, strong):
        return 4, ["File description contains strong qualitative keywords"]
    if any_contains(text, medium):
        return 2, ["File description contains medium qualitative support terms"]
    if any_contains(text, negative):
        return -2, ["File description looks quantitative / technical / non-qualitative"]
    return -1, ["File description does not show clear qualitative evidence"]


def score_project(metadata: ProjectDict) -> ScoreResult:
    desc_score, desc_reason = score_description(metadata.get("description"))
    kind_score, kind_reason = score_kind_of_data(metadata.get("kindOfData"))
    mode_score, mode_reason = score_collection_mode(metadata.get("collectionMode"))
    cat_score, cat_reason = score_file_categories(metadata.get("file_categories"))
    file_desc_score, file_desc_reason = score_file_description(metadata.get("file_description"))

    total = desc_score + kind_score + mode_score + cat_score + file_desc_score
    label = "LIKELY_QUALITATIVE" if total >= 10 else "POSSIBLE_QUALITATIVE" if total >= 6 else "UNCERTAIN" if total >= 1 else "UNLIKELY"

    return {
        "total_score": total,
        "label": label,
        "details": {
            "description_score": desc_score,
            "kind_of_data_score": kind_score,
            "collection_mode_score": mode_score,
            "file_categories_score": cat_score,
            "file_description_score": file_desc_score,
        },
        "reasons": desc_reason + kind_reason + mode_reason + cat_reason + file_desc_reason,
    }


def extract_files(latest: Dict[str, Any], identifier: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], List[str]]:
    files: List[Dict[str, Any]] = []
    file_descs: List[str] = []
    file_cats: List[str] = []
    file_dirs: List[str] = []

    for item in latest.get("files", []) or []:
        file_description = item.get("description") or ""
        directory_label = item.get("directoryLabel") or ""
        categories = item.get("categories") or []
        data_file = item.get("dataFile", {}) or {}

        filename = data_file.get("filename") or item.get("label") or ""
        content_type = data_file.get("contentType") or ""
        file_id = data_file.get("id")

        file_descs.append(file_description)
        file_dirs.append(directory_label)
        file_cats.extend(str(x) for x in categories if x)

        files.append({
            "_project_folder": identifier,
            "file_id": file_id,
            "file_name": filename,
            "file_type": get_file_ext(filename) if filename else get_file_ext(content_type),
            "categories": "; ".join(str(x) for x in categories if x),
            "directory_label": directory_label,
            "description": file_description,
            "restricted": bool(item.get("restricted")),
            "file_access_request": bool(data_file.get("fileAccessRequest")),
            "friendly_type": data_file.get("friendlyType") or "",
            "content_type": content_type,
        })

    return files, file_descs, file_cats, file_dirs


def extract_metadata_keywords(citation_fields: List[Dict[str, Any]]) -> List[str]:
    keywords: List[str] = []
    kw_field = get_field_value(citation_fields, "keyword")
    if isinstance(kw_field, list):
        for item in kw_field:
            if isinstance(item, dict):
                value = item.get("keywordValue", {}).get("value")
                if value:
                    keywords.append(str(value).strip())
    return sorted(set(k for k in keywords if k))


def extract_project_metadata(dataset_json: Dict[str, Any], config: PipelineConfig, global_id: str) -> ProjectDict:
    data = dataset_json["data"]
    latest = data["latestVersion"]
    blocks = latest.get("metadataBlocks", {})
    citation_fields = (blocks.get("citation", {}) or {}).get("fields", [])
    social_fields = (blocks.get("socialscience", {}) or {}).get("fields", [])

    source_project_id = str(data["id"])
    identifier = data.get("identifier")
    persistent_url = data.get("persistentUrl") or ""
    authority = data.get("authority")
    project_url = f"{config.repository_url}/dataset.xhtml?persistentId=doi:{authority}/{identifier}" if authority else persistent_url

    version_number = latest.get("versionNumber")
    version_minor = latest.get("versionMinorNumber")
    version = f"{version_number}.{version_minor}" if version_number is not None and version_minor is not None else str(version_number or "")
    files, file_descs, file_cats, file_dirs = extract_files(latest, str(identifier))

    license_value = latest.get("license")
    if isinstance(license_value, dict):
        license_value = license_value.get("name")

    return {
        "source_project_id": source_project_id,
        "source_global_id": global_id,
        "query_string": "",
        "repository_id": config.repository_id,
        "repository_url": config.repository_url,
        "project_url": project_url,
        "version": version,
        "title": get_field_text(citation_fields, "title"),
        "description": get_description(citation_fields),
        "kindOfData": get_field_text(citation_fields, "kindOfData"),
        "collectionMode": get_field_text(social_fields, "collectionMode"),
        "language": get_field_text(citation_fields, "language"),
        "doi": persistent_url,
        "upload_date": latest.get("lastUpdateTime") or "",
        "download_date": utc_now_iso(),
        "download_repository_folder": config.repo_name,
        "download_project_folder": str(identifier),
        "download_version_folder": f"v{version}" if version else "",
        "download_method": config.download_method,
        "keywords": extract_metadata_keywords(citation_fields),
        "authors": get_authors(citation_fields),
        "contacts": get_contacts(citation_fields),
        "depositor": get_depositor(citation_fields),
        "license": normalize_license(license_value),
        "files": files,
        "file_categories": sorted(set(file_cats)),
        "file_description": sorted(set(file_descs)),
        "file_directoryLabel": sorted(set(file_dirs)),
    }


def get_existing_db_project_id(db: DatabaseManagement, *, repository_id: int, project_url: str) -> Optional[int]:
    row = db.execute(
        """
        SELECT id
        FROM projects
        WHERE repository_id = ? AND project_url = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (repository_id, project_url),
    ).fetchone()
    return int(row["id"]) if row else None


def get_project_folder_by_db_id(db: DatabaseManagement, db_project_id: Optional[int]) -> Optional[str]:
    if db_project_id is None:
        return None
    row = db.execute("SELECT download_project_folder FROM projects WHERE id = ?", (db_project_id,)).fetchone()
    return str(row["download_project_folder"]) if row and row["download_project_folder"] else None


def delete_project_rows_by_db_id(db: DatabaseManagement, *, db_project_id: int, score_table: str) -> None:
    for table in ["keywords", "licenses", "person_role", "files", score_table]:
        db.execute(f"DELETE FROM {table} WHERE project_id = ?", (db_project_id,))
    db.execute("DELETE FROM projects WHERE id = ?", (db_project_id,))


def purge_project_snapshot_aligned(
    *,
    db: DatabaseManagement,
    db_project_id: Optional[int],
    source_project_id: str,
    repo_dir: str,
    tmp_root: str,
    current_folder_name: Optional[str],
    score_table: str,
    logger: logging.Logger,
) -> bool:
    if db_project_id is None:
        logger.debug("No existing snapshot found to purge: source_project_id=%s", source_project_id)
        return True

    backups: List[Tuple[Path, Path]] = []
    try:
        if current_folder_name:
            current_folder_path = Path(repo_dir) / str(current_folder_name)
            if current_folder_path.exists():
                backup_path = Path(make_temp_dir(tmp_root, "backup", source_project_id))
                safe_rmtree(str(backup_path))
                logger.debug("Backing up current project folder before purge: original=%s backup=%s", current_folder_path, backup_path)
                current_folder_path.replace(backup_path)
                backups.append((current_folder_path, backup_path))

        db.begin()
        delete_project_rows_by_db_id(db, db_project_id=db_project_id, score_table=score_table)
        db.commit()

        for _, backup_path in backups:
            if backup_path.exists():
                safe_rmtree(str(backup_path))

        logger.debug("Project purge succeeded: source_project_id=%s", source_project_id)
        return True

    except Exception:
        logger.exception("Project purge failed: source_project_id=%s", source_project_id)
        db.rollback()
        for original_path, backup_path in reversed(backups):
            if original_path.exists():
                safe_rmtree(str(original_path))
            if backup_path.exists():
                backup_path.replace(original_path)
        return False


def replace_project_snapshot_aligned(
    *,
    db: DatabaseManagement,
    existing_db_project_id: Optional[int],
    source_project_id: str,
    repo_dir: str,
    tmp_root: str,
    current_folder_name: str,
    previous_folder_name: Optional[str],
    staged_project_dir: Optional[str],
    score_table: str,
    write_db_rows_fn,
    logger: logging.Logger,
) -> bool:
    final_project_dir = Path(repo_dir) / str(current_folder_name)
    staged_path = Path(staged_project_dir) if staged_project_dir else None

    folder_names_to_backup: List[str] = []
    if previous_folder_name:
        folder_names_to_backup.append(str(previous_folder_name))
    if str(current_folder_name) not in folder_names_to_backup:
        folder_names_to_backup.append(str(current_folder_name))

    backups: List[Tuple[Path, Path]] = []
    promoted_new_dir = False

    try:
        for folder_name in folder_names_to_backup:
            original_path = Path(repo_dir) / folder_name
            if original_path.exists():
                backup_path = Path(make_temp_dir(tmp_root, "backup", source_project_id))
                safe_rmtree(str(backup_path))
                logger.debug("Backing up project folder before replace: original=%s backup=%s", original_path, backup_path)
                original_path.replace(backup_path)
                backups.append((original_path, backup_path))

        db.begin()
        if existing_db_project_id is not None:
            delete_project_rows_by_db_id(db, db_project_id=existing_db_project_id, score_table=score_table)
        write_db_rows_fn()

        if staged_path:
            staged_path.replace(final_project_dir)
            promoted_new_dir = True
            logger.debug("Promoted staged directory: staged=%s final=%s", staged_path, final_project_dir)

        db.commit()

        for _, backup_path in backups:
            if backup_path.exists():
                safe_rmtree(str(backup_path))

        logger.debug("Project refresh succeeded: source_project_id=%s", source_project_id)
        return True

    except Exception:
        logger.exception("Atomic project refresh failed: source_project_id=%s", source_project_id)
        db.rollback()

        if promoted_new_dir and final_project_dir.exists():
            safe_rmtree(str(final_project_dir))
        elif staged_path and staged_path.exists():
            safe_rmtree(str(staged_path))

        for original_path, backup_path in reversed(backups):
            if original_path.exists():
                safe_rmtree(str(original_path))
            if backup_path.exists():
                backup_path.replace(original_path)

        return False


def insert_score(db: DatabaseManagement, *, db_project_id: int, project: ProjectDict, score_result: ScoreResult) -> None:
    db.execute("""
        INSERT INTO qualitative_scores_aussda (
            project_id,
            source_project_id,
            total_score,
            label,
            description_score,
            kind_of_data_score,
            collection_mode_score,
            file_categories_score,
            file_description_score,
            reasons,
            timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        db_project_id,
        project["source_project_id"],
        score_result["total_score"],
        score_result["label"],
        score_result["details"]["description_score"],
        score_result["details"]["kind_of_data_score"],
        score_result["details"]["collection_mode_score"],
        score_result["details"]["file_categories_score"],
        score_result["details"]["file_description_score"],
        " | ".join(score_result["reasons"]),
        utc_now_iso(),
    ))


def write_project_snapshot(db: DatabaseManagement, *, project: ProjectDict, score_result: ScoreResult, file_rows: List[FileRow]) -> int:
    cursor = db.execute("""
        INSERT INTO projects (
            query_string,
            repository_id,
            repository_url,
            project_url,
            version,
            title,
            description,
            language,
            doi,
            upload_date,
            download_date,
            download_repository_folder,
            download_project_folder,
            download_version_folder,
            download_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        project["query_string"],
        project["repository_id"],
        project["repository_url"],
        project["project_url"],
        project["version"],
        project["title"],
        project["description"],
        project["language"],
        project["doi"],
        project["upload_date"],
        project["download_date"],
        project["download_repository_folder"],
        project["download_project_folder"],
        project["download_version_folder"],
        project["download_method"],
    ))
    db_project_id = int(cursor.lastrowid)

    for keyword in sorted(set(project.get("keywords", []))):
        db.execute("INSERT INTO keywords (project_id, keyword) VALUES (?, ?)", (db_project_id, keyword))

    for author in sorted(set(project.get("authors", []))):
        db.execute("INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)", (db_project_id, author, "AUTHOR"))

    for contact in sorted(set(project.get("contacts", []))):
        db.execute("INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)", (db_project_id, contact, "OTHER"))

    if project.get("depositor"):
        db.execute("INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)", (db_project_id, project["depositor"], "UPLOADER"))

    if project.get("license"):
        db.execute("INSERT INTO licenses (project_id, license) VALUES (?, ?)", (db_project_id, project["license"]))

    for file_name, file_type, status in file_rows:
        db.execute("INSERT INTO files (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)", (db_project_id, file_name, file_type, status))

    insert_score(db, db_project_id=db_project_id, project=project, score_result=score_result)
    return db_project_id


def create_summary(config: PipelineConfig, search_terms: List[str]) -> Dict[str, Any]:
    return {
        "repository": config.repo_name,
        "repository_id": config.repository_id,
        "run_started_at": utc_now_iso(),
        "run_finished_at": None,
        "search_terms": search_terms,
        "bucket_rule_note": "For AUSSDA, projects_with_numeric_data follows should_skip_kind_of_data(kindOfData).",
        "counts": {
            "received_results": 0,
            "unique_projects_seen": 0,
            "analysed_projects": 0,
            "saved_projects": 0,
            "skipped_projects": 0,
            "failed_projects": 0,
        },
        "projects_with_numeric_data": 0,
        "projects_with_non_numeric_data": {
            "identified_qualitative_projects": {"projects_with_files": 0, "projects_with_no_files": 0},
            "identified_non_qualitative_projects": {"projects_with_files": 0, "projects_with_no_files": 0},
        },
    }


def increment_classification_bucket(summary: Dict[str, Any], *, is_qualitative: bool, has_files: bool) -> None:
    main_bucket = "identified_qualitative_projects" if is_qualitative else "identified_non_qualitative_projects"
    file_bucket = "projects_with_files" if has_files else "projects_with_no_files"
    summary["projects_with_non_numeric_data"][main_bucket][file_bucket] += 1


def stage_file_downloads(
    *,
    project: ProjectDict,
    config: PipelineConfig,
    paths: RepoPaths,
    session: requests.Session,
    logger: logging.Logger,
) -> Tuple[List[FileRow], Optional[str], Optional[str]]:
    file_rows: List[FileRow] = []
    staged_dir: Optional[str] = None
    used_names: Set[str] = set()

    for file_meta in project.get("files", []):
        file_name = file_meta.get("file_name") or None
        file_type = file_meta.get("file_type") or None
        file_id = file_meta.get("file_id")

        if file_name and file_name in used_names:
            logger.error("Duplicate filename detected during staging: source_project_id=%s filename=%s", project["source_project_id"], file_name)
            return [], None, f"Duplicate filename detected in project {project['source_project_id']}: {file_name}"

        if file_name:
            used_names.add(file_name)

        if not file_id:
            logger.warning("File metadata missing file_id: source_project_id=%s file_name=%s", project["source_project_id"], file_name)
            file_rows.append((file_name, file_type, STATUS_FAILED_SERVER_UNRESPONSIVE))
            continue

        if file_meta.get("restricted") or file_meta.get("file_access_request"):
            logger.warning("Restricted file encountered: source_project_id=%s file_name=%s", project["source_project_id"], file_name)
            file_rows.append((file_name, file_type, STATUS_FAILED_LOGIN_REQUIRED))
            continue

        if staged_dir is None:
            staged_dir = make_temp_dir(paths.tmp_root, "stage", project["source_project_id"])
            logger.debug("Created staging directory: source_project_id=%s staged_dir=%s", project["source_project_id"], staged_dir)

        download_name = file_name or f"file_{file_id}"
        path = str(Path(staged_dir) / download_name)
        status = download_file(
            url=f"{config.api_base}/access/datafile/{file_id}?format=original",
            path=path,
            timeout=config.timeout,
            login_required_statuses={401, 403},
            too_large_statuses={413},
            session=session,
            logger=logger,
        )

        if status == STATUS_SUCCEEDED:
            file_rows.append((file_name, file_type, status))
            continue

        if status in {STATUS_FAILED_LOGIN_REQUIRED, STATUS_FAILED_TOO_LARGE}:
            safe_rmtree(path)
            file_rows.append((file_name, file_type, status))
            continue

        if staged_dir:
            safe_rmtree(staged_dir)

        logger.error("Hard download failure during staging: source_project_id=%s file_name=%s status=%s", project["source_project_id"], download_name, status)
        return [], None, f"Failed to download file for project {project['source_project_id']}: {download_name}"

    return file_rows, staged_dir, None


def collect_project_queries(
    *,
    session: requests.Session,
    config: PipelineConfig,
    search_terms: List[str],
    logger: logging.Logger,
    summary: Dict[str, Any],
) -> Tuple[Dict[str, Set[str]], List[str]]:
    project_queries: Dict[str, Set[str]] = {}
    project_order: List[str] = []

    for term in search_terms:
        logger.info("Searching repository: query=%s", term if term != "*" else "ALL ITEMS")
        start = 0

        while True:
            try:
                search_data = fetch_search_page(session=session, config=config, start=start, query_string=term, logger=logger)
            except Exception:
                logger.exception("Search failed: query=%s start=%s", term, start)
                break

            items = search_data.get("data", {}).get("items", [])
            summary["counts"]["received_results"] += len(items)
            logger.debug("Search page received: query=%s start=%s items=%s", term, start, len(items))

            if not items:
                break

            for item in items:
                global_id = item.get("global_id")
                if not global_id:
                    logger.warning("Skipping search item without global_id: query=%s start=%s", term, start)
                    continue
                project_queries.setdefault(global_id, set()).add(term)
                if global_id not in project_order:
                    project_order.append(global_id)

            start += config.per_page

    summary["counts"]["unique_projects_seen"] = len(project_order)
    return project_queries, project_order


def process_project(
    *,
    global_id: str,
    project_queries: Dict[str, Set[str]],
    session: requests.Session,
    db: DatabaseManagement,
    config: PipelineConfig,
    paths: RepoPaths,
    logger: logging.Logger,
    summary: Dict[str, Any],
    allowed_labels: Set[str],
) -> None:
    dataset_json = fetch_dataset(session=session, config=config, persistent_id=global_id, logger=logger)
    project = extract_project_metadata(dataset_json, config, global_id)
    project["query_string"] = "|".join(sorted(project_queries.get(global_id, {"*"})))
    log_project_summary(logger, project)

    existing_db_project_id = get_existing_db_project_id(db, repository_id=project["repository_id"], project_url=project["project_url"])
    existing_folder_name = get_project_folder_by_db_id(db, existing_db_project_id)
    has_files = bool(project.get("files"))

    skip_reasons = get_negative_kind_of_data_reasons(project.get("kindOfData"))
    if skip_reasons:
        summary["counts"]["skipped_projects"] += 1
        summary["projects_with_numeric_data"] += 1

        if not purge_project_snapshot_aligned(
            db=db,
            db_project_id=existing_db_project_id,
            source_project_id=project["source_project_id"],
            repo_dir=paths.repo_dir,
            tmp_root=paths.tmp_root,
            current_folder_name=existing_folder_name,
            score_table=config.score_table,
            logger=logger,
        ):
            summary["counts"]["failed_projects"] += 1
            log_project_decision(
                logger,
                project_id=project["source_project_id"],
                action="failed",
                reason="purge-failed",
                has_files=has_files,
                level=logging.ERROR,
            )
            return

        log_project_decision(
            logger,
            project_id=project["source_project_id"],
            action="skipped",
            reason=",".join(skip_reasons),
            has_files=has_files,
        )
        return

    summary["counts"]["analysed_projects"] += 1
    score_result = score_project(project)
    log_score_summary(logger, project["source_project_id"], score_result)

    is_qualitative = score_result["label"] in allowed_labels
    increment_classification_bucket(summary, is_qualitative=is_qualitative, has_files=has_files)

    if not is_qualitative:
        summary["counts"]["skipped_projects"] += 1

        if not purge_project_snapshot_aligned(
            db=db,
            db_project_id=existing_db_project_id,
            source_project_id=project["source_project_id"],
            repo_dir=paths.repo_dir,
            tmp_root=paths.tmp_root,
            current_folder_name=existing_folder_name,
            score_table=config.score_table,
            logger=logger,
        ):
            summary["counts"]["failed_projects"] += 1
            log_project_decision(
                logger,
                project_id=project["source_project_id"],
                action="failed",
                reason="purge-failed",
                has_files=has_files,
                level=logging.ERROR,
            )
            return

        log_project_decision(
            logger,
            project_id=project["source_project_id"],
            action="skipped",
            score=score_result["total_score"],
            label=score_result["label"],
            has_files=has_files,
        )
        return

    file_rows, staged_project_dir, hard_error = stage_file_downloads(
        project=project,
        config=config,
        paths=paths,
        session=session,
        logger=logger,
    )
    if hard_error:
        summary["counts"]["skipped_projects"] += 1
        summary["counts"]["failed_projects"] += 1
        log_project_decision(
            logger,
            project_id=project["source_project_id"],
            action="failed",
            reason="staging-error",
            has_files=has_files,
            level=logging.ERROR,
        )
        logger.error("Staging detail: source_project_id=%s error=%s", project["source_project_id"], hard_error)
        return

    refreshed = replace_project_snapshot_aligned(
        db=db,
        existing_db_project_id=existing_db_project_id,
        source_project_id=project["source_project_id"],
        repo_dir=paths.repo_dir,
        tmp_root=paths.tmp_root,
        current_folder_name=str(project["download_project_folder"]),
        previous_folder_name=existing_folder_name,
        staged_project_dir=staged_project_dir,
        score_table=config.score_table,
        write_db_rows_fn=lambda: write_project_snapshot(db=db, project=project, score_result=score_result, file_rows=file_rows),
        logger=logger,
    )

    if refreshed:
        summary["counts"]["saved_projects"] += 1
        log_project_decision(
            logger,
            project_id=project["source_project_id"],
            action="saved",
            score=score_result["total_score"],
            label=score_result["label"],
            has_files=has_files,
        )
    else:
        summary["counts"]["skipped_projects"] += 1
        summary["counts"]["failed_projects"] += 1
        log_project_decision(
            logger,
            project_id=project["source_project_id"],
            action="failed",
            reason="refresh-failed",
            has_files=has_files,
            level=logging.ERROR,
        )


def run() -> None:
    config = build_config()
    paths = build_repo_paths(base_dir=config.base_dir, repo_name=config.repo_name)
    logger = setup_logger(
        logger_name="repo_aussda",
        log_file_path=str(Path(paths.log_dir) / config.log_file_name),
        console_level=logging.INFO,
        file_level=logging.DEBUG,
    )

    search_terms = normalize_search_terms(config.query, empty_value="*")
    summary = create_summary(config, search_terms)
    allowed_labels = {"LIKELY_QUALITATIVE", "POSSIBLE_QUALITATIVE"}

    logger.info("AUSSDA qualitative pipeline started")
    logger.debug("Pipeline configuration: %s", config)

    session = requests.Session()
    db = DatabaseManagement(db_path=config.db_path, schema_path=config.schema_path)

    try:
        project_queries, project_order = collect_project_queries(
            session=session,
            config=config,
            search_terms=search_terms,
            logger=logger,
            summary=summary,
        )

        for global_id in project_order:
            try:
                process_project(
                    global_id=global_id,
                    project_queries=project_queries,
                    session=session,
                    db=db,
                    config=config,
                    paths=paths,
                    logger=logger,
                    summary=summary,
                    allowed_labels=allowed_labels,
                )
            except Exception:
                summary["counts"]["skipped_projects"] += 1
                summary["counts"]["failed_projects"] += 1
                log_project_decision(
                    logger,
                    project_id=global_id,
                    action="failed",
                    reason="unhandled-exception",
                    has_files=None,
                    level=logging.ERROR,
                )
                logger.exception("Unhandled project processing error: global_id=%s", global_id)

    finally:
        summary["run_finished_at"] = utc_now_iso()
        run_tag = str(summary["run_started_at"]).replace(":", "").replace("-", "").replace(".", "").replace("T", "_")
        summary_path = str(Path(paths.summary_dir) / f"{config.repo_name}_summary_{run_tag}.json")
        write_json_summary(summary_path, summary)

        logger.info(
            "Pipeline finished: analysed=%s saved=%s skipped=%s received_results=%s summary=%s",
            summary["counts"]["analysed_projects"],
            summary["counts"]["saved_projects"],
            summary["counts"]["skipped_projects"],
            summary["counts"]["received_results"],
            summary_path,
        )

        session.close()
        db.close()


if __name__ == "__main__":
    run()