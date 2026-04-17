import os
import re
import shutil
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests

# -----------------------------------
# CONFIG
# -----------------------------------
API_BASE = "https://data.aussda.at/api"

# Provide a list of keywords, or set to None / [] to search all datasets with "*"
QUERY = []
PER_PAGE = 100
TIMEOUT = 60

BASE_DIR = "downloads"
REPO_NAME = "aussda"
REPO_DIR = os.path.join(BASE_DIR, REPO_NAME)
TMP_ROOT = os.path.join(REPO_DIR, "_tmp")

DB_PATH = "23041405-seeding.db"

REPOSITORY_ID = 12
REPOSITORY_URL = "https://data.aussda.at"
DOWNLOAD_METHOD = "API-CALL"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
SCHEMA_PATH = os.path.join(SCRIPT_DIR, "schema.sql")

os.makedirs(REPO_DIR, exist_ok=True)
os.makedirs(TMP_ROOT, exist_ok=True)

# -----------------------------------
# DATABASE SETUP
# -----------------------------------
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
    cur.executescript(f.read())
conn.commit()

# -----------------------------------
# HELPERS
# -----------------------------------
def utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def make_project_key(repository_id: int, project_id: int) -> str:
    return f"{repository_id}{project_id}"


def get_search_terms() -> List[str]:
    if QUERY is None or QUERY == []:
        return ["*"]
    if isinstance(QUERY, str):
        return [QUERY]
    return list(QUERY)


def fetch_search_page(start: int, query_string: str) -> Dict[str, Any]:
    url = f"{API_BASE}/search"
    params = {
        "q": query_string,
        "type": "dataset",
        "start": start,
        "per_page": PER_PAGE,
    }
    response = requests.get(url, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_dataset(persistent_id: str) -> Dict[str, Any]:
    url = f"{API_BASE}/datasets/:persistentId/"
    params = {"persistentId": persistent_id}
    response = requests.get(url, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def flatten_value(value: Any) -> List[str]:
    """
    Convert nested Dataverse values into a flat list of strings.
    """
    if value is None:
        return []

    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(flatten_value(item))
        return out

    if isinstance(value, dict):
        if "value" in value:
            return flatten_value(value["value"])
        return []

    text = str(value).strip()
    return [text] if text else []


def normalize_text(value: Any) -> str:
    return " ".join(flatten_value(value)).lower().strip()


def any_contains(text: str, patterns: List[str]) -> bool:
    return any(p.lower() in text for p in patterns)


def get_field_value(fields: List[Dict[str, Any]], type_name: str) -> Any:
    for field in fields:
        if field.get("typeName") == type_name:
            return field.get("value")
    return None


def get_description(fields: List[Dict[str, Any]]) -> str:
    for field in fields:
        if field.get("typeName") == "dsDescription":
            values = field.get("value") or []
            parts = []
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
    depositor = get_field_value(fields, "depositor")
    values = flatten_value(depositor)
    return values[0].strip() if values else ""


def get_field_text(fields: List[Dict[str, Any]], type_name: str) -> str:
    return " ".join(flatten_value(get_field_value(fields, type_name)))


def get_field_list(fields: List[Dict[str, Any]], type_name: str) -> List[str]:
    return flatten_value(get_field_value(fields, type_name))


def get_file_ext(name: str) -> str:
    if not name or "." not in name:
        return "unknown"
    return name.rsplit(".", 1)[-1].lower()


def log_info(message: str) -> None:
    print(message)


def save_failure_message(message: str) -> None:
    print(f"  ! {message}")


def safe_rmtree(path: str):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def make_temp_dir(prefix: str, project_id: int) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    path = os.path.join(TMP_ROOT, f"{prefix}_{project_id}_{timestamp}")
    os.makedirs(path, exist_ok=True)
    return path


def download_file(url: str, path: str) -> str:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with requests.get(url, stream=True, timeout=TIMEOUT) as response:
            if response.status_code in (401, 403):
                return "FAILED_LOGIN_REQUIRED"
            if response.status_code == 413:
                return "FAILED_TOO_LARGE"
            if response.status_code != 200:
                return "FAILED_SERVER_UNRESPONSIVE"

            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        return "SUCCEEDED"
    except requests.RequestException:
        return "FAILED_SERVER_UNRESPONSIVE"
    except Exception:
        return "FAILED_SERVER_UNRESPONSIVE"


def get_existing_project_folder(project_key: int) -> Optional[str]:
    cur.execute("SELECT download_project_folder FROM projects WHERE project_key = ?", (project_key,))
    row = cur.fetchone()
    if not row:
        return None
    folder = row[0]
    return str(folder) if folder else None


def delete_project_rows(project_key: int) -> None:
    tables = [
        "keywords",
        "licenses",
        "person_role",
        "files",
        "qualitative_scores_aussda",
        "projects",
    ]
    for table in tables:
        cur.execute(f"DELETE FROM {table} WHERE project_key = ?", (project_key,))


# -----------------------------------
# SCORING RULES
# -----------------------------------
def score_description(description: Any) -> Tuple[int, List[str]]:
    text = normalize_text(description)

    if not text:
        return -2, ["Description missing"]

    strong = ["qualitative", "interview", "transcript", "focus group"]
    if any_contains(text, strong):
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


NEGATIVE_KIND_OF_DATA = {
    "numeric",
    "geospatial",
    "software",
    "interactive resource",
}


def should_skip_kind_of_data(kind_of_data: Any) -> bool:
    text = normalize_text(kind_of_data)
    if not text:
        return False

    parts = re.split(r"[;,/|]+", text)
    parts = {p.strip() for p in parts if p.strip()}

    # Skip only if ALL values are negative
    return parts and all(part in NEGATIVE_KIND_OF_DATA for part in parts)


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
        "research report", "interviewer manual"
    ]
    negative = ["other"]

    if any_contains(text, negative):
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

    strong = [
        "interview",
        "interviews",
        "transcript",
        "transcripts",
        "focus group",
        "qualitative",
        "open answers",
    ]

    medium = [
        "questionnaire",
        "manual",
        "report",
        "form",
        "scheme",
        "protocol",
        "plan",
        "ethical committee vote",
        "preparation",
    ]

    negative = [
        "other",
        "numeric",
        "variables",
        "observations",
        "stata",
        "spss",
        "csv",
        "excel",
        "tabulation",
        "replication",
        "syntax",
        "code",
        "script",
        "weights",
    ]

    if any_contains(text, strong):
        return 4, ["File description contains strong qualitative keywords"]

    if any_contains(text, medium):
        return 2, ["File description contains medium qualitative support terms"]

    if any_contains(text, negative):
        return -2, ["File description looks quantitative / technical / non-qualitative"]

    return -1, ["File description does not show clear qualitative evidence"]


def score_project(metadata: Dict[str, Any]) -> Dict[str, Any]:
    desc_score, desc_reason = score_description(metadata.get("description"))
    kind_score, kind_reason = score_kind_of_data(metadata.get("kindOfData"))
    mode_score, mode_reason = score_collection_mode(metadata.get("collectionMode"))
    cat_score, cat_reason = score_file_categories(metadata.get("file_categories"))
    file_desc_score, file_desc_reason = score_file_description(metadata.get("file_description"))

    total = desc_score + kind_score + mode_score + cat_score + file_desc_score

    if total >= 10:
        label = "LIKELY_QUALITATIVE"
    elif total >= 6:
        label = "POSSIBLE_QUALITATIVE"
    elif total >= 1:
        label = "UNCERTAIN"
    else:
        label = "UNLIKELY"

    reasons = desc_reason + kind_reason + mode_reason + cat_reason + file_desc_reason

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
        "reasons": reasons,
    }


def insert_score(project: Dict[str, Any], score_result: Dict[str, Any]) -> None:
    cur.execute("""
        INSERT INTO qualitative_scores_aussda (
            project_key,
            project_id,
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
        project["project_key"],
        project["project_id"],
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


# -----------------------------------
# EXTRACTION
# -----------------------------------
def extract_files(latest: Dict[str, Any], identifier: str) -> Tuple[List[Dict[str, Any]], List[str], List[str], List[str]]:
    files: List[Dict[str, Any]] = []
    file_descs: List[str] = []
    file_cats: List[str] = []
    file_dirs: List[str] = []

    for f in latest.get("files", []) or []:
        file_description = f.get("description") or ""
        directory_label = f.get("directoryLabel") or ""
        categories = f.get("categories") or []
        data_file = f.get("dataFile", {}) or {}

        filename = data_file.get("filename") or f.get("label") or ""
        content_type = data_file.get("contentType") or ""
        friendly_type = data_file.get("friendlyType") or ""
        file_id = data_file.get("id")

        file_descs.append(file_description)
        file_dirs.append(directory_label)
        file_cats.extend([str(x) for x in categories if x])

        files.append({
            "_project_folder": identifier,
            "file_id": file_id,
            "file_name": filename,
            "file_type": get_file_ext(filename) if filename else get_file_ext(content_type),
            "categories": "; ".join([str(x) for x in categories if x]),
            "directory_label": directory_label,
            "description": file_description,
            "restricted": bool(f.get("restricted")),
            "file_access_request": bool(data_file.get("fileAccessRequest")),
            "friendly_type": friendly_type,
            "content_type": content_type,
        })

    return files, file_descs, file_cats, file_dirs


def extract_metadata_keywords(citation_fields: List[Dict[str, Any]]) -> List[str]:
    metadata_keywords: List[str] = []
    kw_field = get_field_value(citation_fields, "keyword")

    if isinstance(kw_field, list):
        for item in kw_field:
            if isinstance(item, dict):
                kv = item.get("keywordValue", {}).get("value")
                if kv:
                    metadata_keywords.append(str(kv).strip())

    return sorted(set([k for k in metadata_keywords if k]))


def extract_project_metadata(dataset_json: Dict[str, Any]) -> Dict[str, Any]:
    data = dataset_json["data"]
    latest = data["latestVersion"]
    blocks = latest.get("metadataBlocks", {})
    citation = blocks.get("citation", {})
    social = blocks.get("socialscience", {})

    citation_fields = citation.get("fields", [])
    social_fields = social.get("fields", [])

    project_id = int(data["id"])
    project_key = int(make_project_key(REPOSITORY_ID, project_id))
    identifier = data.get("identifier")
    persistent_url = data.get("persistentUrl") or ""
    project_url = f"{REPOSITORY_URL}/dataset.xhtml?persistentId=doi:{data.get('authority')}/{identifier}" if data.get("authority") else persistent_url
    title = get_field_text(citation_fields, "title")
    project_description = get_description(citation_fields)
    kind_of_data = get_field_text(citation_fields, "kindOfData")
    collection_mode = get_field_text(social_fields, "collectionMode")
    language = get_field_text(citation_fields, "language")
    version_number = latest.get("versionNumber")
    version_minor = latest.get("versionMinorNumber")
    version = f"{version_number}.{version_minor}" if version_number is not None and version_minor is not None else str(version_number or "")
    upload_date = latest.get("lastUpdateTime") or ""
    download_version_folder = f"v{version}" if version else ""
    metadata_keywords = extract_metadata_keywords(citation_fields)
    authors = get_authors(citation_fields)
    contacts = get_contacts(citation_fields)
    depositor = get_depositor(citation_fields)
    files, file_descs, file_cats, file_dirs = extract_files(latest, identifier)

    project = {
        "project_key": project_key,
        "project_id": project_id,
        "query_string": "",
        "repository_id": REPOSITORY_ID,
        "repository_url": REPOSITORY_URL,
        "project_url": project_url,
        "version": version,
        "title": title,
        "description": project_description,
        "kindOfData": kind_of_data,
        "collectionMode": collection_mode,
        "language": language,
        "doi": persistent_url,
        "upload_date": upload_date,
        "download_date": utc_now_iso(),
        "download_repository_folder": REPO_NAME,
        "download_project_folder": identifier,
        "download_version_folder": download_version_folder,
        "download_method": DOWNLOAD_METHOD,
        "keywords": metadata_keywords,
        "authors": authors,
        "contacts": contacts,
        "depositor": depositor,
        "license": (latest.get("license") or {}).get("name"),
        "files": files,
        "file_categories": list(set(file_cats)),
        "file_description": list(set(file_descs)),
        "file_directoryLabel": list(set(file_dirs)),
    }
    return project


# -----------------------------------
# STAGING + ATOMIC REFRESH
# -----------------------------------
def stage_file_downloads(project: Dict[str, Any]) -> Tuple[List[Tuple[Optional[str], Optional[str], str]], Optional[str], Optional[str]]:
    """
    Returns:
      - file rows to insert into DB
      - staged project directory containing fresh downloaded files, or None
      - hard error message if refresh must be aborted
    """
    file_rows: List[Tuple[Optional[str], Optional[str], str]] = []
    staged_dir: Optional[str] = None
    used_names: Set[str] = set()

    for f in project.get("files", []):
        file_name = f.get("file_name") or None
        file_type = f.get("file_type") or None
        file_id = f.get("file_id")

        if file_name and file_name in used_names:
            if staged_dir:
                safe_rmtree(staged_dir)
            return [], None, f"Duplicate filename detected in project {project['project_id']}: {file_name}"

        if file_name:
            used_names.add(file_name)

        if not file_id:
            file_rows.append((file_name, file_type, "FAILED_SERVER_UNRESPONSIVE"))
            continue

        if f.get("restricted") or f.get("file_access_request"):
            file_rows.append((file_name, file_type, "FAILED_LOGIN_REQUIRED"))
            continue

        if staged_dir is None:
            staged_dir = make_temp_dir("stage", project["project_id"])

        download_name = file_name or f"file_{file_id}"
        url = f"{API_BASE}/access/datafile/{file_id}?format=original"
        path = os.path.join(staged_dir, download_name)

        status = download_file(url, path)

        if status == "SUCCEEDED":
            file_rows.append((file_name, file_type, status))
            continue

        if status in {"FAILED_LOGIN_REQUIRED", "FAILED_TOO_LARGE"}:
            if os.path.exists(path):
                safe_rmtree(path)
            file_rows.append((file_name, file_type, status))
            continue

        if staged_dir:
            safe_rmtree(staged_dir)
        return [], None, f"Failed to download file for project {project['project_id']}: {download_name}"

    return file_rows, staged_dir, None


def write_project_snapshot(project: Dict[str, Any], score_result: Dict[str, Any], file_rows: List[Tuple[Optional[str], Optional[str], str]]) -> None:
    cur.execute("""
        INSERT INTO projects (
            project_key, project_id, query_string, repository_id, repository_url, project_url, version,
            title, description, language, doi, upload_date, download_date,
            download_repository_folder, download_project_folder, download_version_folder, download_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        project["project_key"],
        project["project_id"],
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

    for kw in sorted(set(project.get("keywords", []))):
        cur.execute(
            "INSERT INTO keywords (project_key, keyword) VALUES (?, ?)",
            (project["project_key"], kw)
        )

    for author in sorted(set(project.get("authors", []))):
        cur.execute(
            "INSERT INTO person_role (project_key, name, role) VALUES (?, ?, ?)",
            (project["project_key"], author, "AUTHOR")
        )

    for contact in sorted(set(project.get("contacts", []))):
        cur.execute(
            "INSERT INTO person_role (project_key, name, role) VALUES (?, ?, ?)",
            (project["project_key"], contact, "OTHER")
        )

    if project.get("depositor"):
        cur.execute(
            "INSERT INTO person_role (project_key, name, role) VALUES (?, ?, ?)",
            (project["project_key"], project["depositor"], "UPLOADER")
        )

    if project.get("license"):
        cur.execute(
            "INSERT INTO licenses (project_key, license) VALUES (?, ?)",
            (project["project_key"], project["license"])
        )

    for file_name, file_type, status in file_rows:
        cur.execute("""
            INSERT INTO files (project_key, file_name, file_type, status)
            VALUES (?, ?, ?, ?)
        """, (
            project["project_key"],
            file_name,
            file_type,
            status
        ))

    insert_score(project, score_result)


def replace_project_snapshot(
    project: Dict[str, Any],
    write_db_rows: Callable[[], None],
    staged_project_dir: Optional[str],
) -> bool:
    project_key = project["project_key"]
    current_folder_name = str(project["download_project_folder"])
    previous_folder_name = get_existing_project_folder(project_key)

    folder_names_to_backup: List[str] = []
    if previous_folder_name:
        folder_names_to_backup.append(previous_folder_name)
    if current_folder_name not in folder_names_to_backup:
        folder_names_to_backup.append(current_folder_name)

    backups: List[Tuple[str, str]] = []
    final_project_dir = os.path.join(REPO_DIR, current_folder_name)
    promoted_new_dir = False

    try:
        for folder_name in folder_names_to_backup:
            folder_path = os.path.join(REPO_DIR, folder_name)
            if os.path.exists(folder_path):
                backup_path = make_temp_dir("backup", project["project_id"])
                safe_rmtree(backup_path)
                os.replace(folder_path, backup_path)
                backups.append((folder_path, backup_path))

        conn.execute("BEGIN")
        delete_project_rows(project_key)
        write_db_rows()

        if staged_project_dir:
            os.replace(staged_project_dir, final_project_dir)
            promoted_new_dir = True

        conn.commit()

        for _, backup_path in backups:
            if os.path.exists(backup_path):
                safe_rmtree(backup_path)

        return True

    except Exception as e:
        conn.rollback()

        if promoted_new_dir and os.path.exists(final_project_dir):
            safe_rmtree(final_project_dir)
        elif staged_project_dir and os.path.exists(staged_project_dir):
            safe_rmtree(staged_project_dir)

        for original_path, backup_path in reversed(backups):
            if os.path.exists(original_path):
                safe_rmtree(original_path)
            if os.path.exists(backup_path):
                os.replace(backup_path, original_path)

        save_failure_message(f"Atomic refresh failed for project {project['project_id']}: {e}")
        return False


def purge_project_snapshot(project_key: int, project_id: int) -> bool:
    current_folder_name = get_existing_project_folder(project_key)
    backups: List[Tuple[str, str]] = []

    try:
        if current_folder_name:
            current_folder_path = os.path.join(REPO_DIR, current_folder_name)
            if os.path.exists(current_folder_path):
                backup_path = make_temp_dir("backup", project_id)
                safe_rmtree(backup_path)
                os.replace(current_folder_path, backup_path)
                backups.append((current_folder_path, backup_path))

        conn.execute("BEGIN")
        delete_project_rows(project_key)
        conn.commit()

        for _, backup_path in backups:
            if os.path.exists(backup_path):
                safe_rmtree(backup_path)

        return True

    except Exception as e:
        conn.rollback()

        for original_path, backup_path in reversed(backups):
            if os.path.exists(original_path):
                safe_rmtree(original_path)
            if os.path.exists(backup_path):
                os.replace(backup_path, original_path)

        save_failure_message(f"Failed to purge project snapshot for project {project_id}: {e}")
        return False


# -----------------------------------
# DISPLAY
# -----------------------------------
def print_project_summary(project: Dict[str, Any]) -> None:
    print("--------------------------------------------------")
    print(f"Project ID: {project['project_id']}")
    print(f"Title: {project['title']}")
    print(f"KindOfData: {project['kindOfData']}")
    print(f"CollectionMode: {project['collectionMode']}")
    print(f"Description preview: {project['description'][:180]}..." if project["description"] else "Description preview: ")
    print(f"File categories: {project.get('file_categories', [])}")
    print(f"File description: {project.get('file_description', [])}")
    print("--------------------------------------------------")


def print_score_summary(project_id: int, score_result: Dict[str, Any]) -> None:
    details = score_result["details"]
    print(f"Score summary for {project_id}:")
    print(f"  Total: {score_result['total_score']}")
    print(f"  Label: {score_result['label']}")
    print(f"  KindOfData: {details['kind_of_data_score']}")
    print(f"  CollectionMode: {details['collection_mode_score']}")
    print(f"  Description: {details['description_score']}")
    print(f"  File categories: {details['file_categories_score']}")
    print(f"  File description: {details['file_description_score']}")
    print(f"  Reasons: {' | '.join(score_result['reasons'])}")


# -----------------------------------
# PIPELINE
# -----------------------------------
def run():
    print("\n🚀 AUSSDA QUALITATIVE PIPELINE\n")

    analysed_projects = 0
    saved_projects = 0
    skipped_projects = 0
    received_results = 0

    allowed_labels = {"LIKELY_QUALITATIVE", "POSSIBLE_QUALITATIVE"}
    search_terms = get_search_terms()

    project_queries: Dict[str, Set[str]] = {}
    project_order: List[str] = []

    for kw in search_terms:
        print(f"\n🔍 {kw if kw != '*' else 'ALL ITEMS'}")
        start = 0

        while True:
            try:
                search_data = fetch_search_page(start, kw)
            except Exception as e:
                print(f"Search failed for query={kw} at start={start}: {e}")
                break

            items = search_data.get("data", {}).get("items", [])
            received_results += len(items)

            if not items:
                break

            for item in items:
                global_id = item.get("global_id")
                if not global_id:
                    continue

                if global_id not in project_queries:
                    project_queries[global_id] = set()
                    project_order.append(global_id)

                project_queries[global_id].add(kw)

            start += PER_PAGE

    for global_id in project_order:
        print(f"\n📌 {global_id}")

        try:
            dataset_json = fetch_dataset(global_id)
            project = extract_project_metadata(dataset_json)
            project["query_string"] = "|".join(sorted(project_queries.get(global_id, {"*"})))

            print_project_summary(project)

            if should_skip_kind_of_data(project.get("kindOfData")):
                skipped_projects += 1
                print(f"Skipped because non-qualitative KindOfData = {project.get('kindOfData')}")
                purge_project_snapshot(project["project_key"], project["project_id"])
                conn.commit()
                continue

            analysed_projects += 1
            score_result = score_project(project)
            print_score_summary(project["project_id"], score_result)

            if score_result["label"] not in allowed_labels:
                print(f"Label {score_result['label']} is not allowed, removing existing local snapshot if present...")
                purge_project_snapshot(project["project_key"], project["project_id"])
                conn.commit()
                continue

            file_rows, staged_project_dir, hard_error = stage_file_downloads(project)
            if hard_error:
                skipped_projects += 1
                save_failure_message(hard_error)
                conn.commit()
                continue

            refreshed = replace_project_snapshot(
                project=project,
                write_db_rows=lambda: write_project_snapshot(project, score_result, file_rows),
                staged_project_dir=staged_project_dir,
            )

            if refreshed:
                saved_projects += 1
            else:
                skipped_projects += 1

            conn.commit()

        except Exception as e:
            skipped_projects += 1
            print(f"Skipped {global_id}: {e}")
            conn.commit()

    print("\n🎉 DONE")
    print(f"Analysed projects: {analysed_projects}")
    print(f"Saved qualitative projects: {saved_projects}")
    print(f"Skipped projects: {skipped_projects}")
    print(f"Received search results: {received_results}")


if __name__ == "__main__":
    run()
    conn.close()