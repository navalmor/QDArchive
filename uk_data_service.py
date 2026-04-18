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

SEARCH_QUERY = """query GetStudyList($QueryString: String, $Start: Int, $Rows: Int, $Sort: Int, $Phrase: Boolean, $DateFrom: Int, $DateTo: Int, $FacetParams: String) {
  getStudyList(QueryString: $QueryString, Start: $Start, Rows: $Rows, Sort: $Sort, Phrase: $Phrase, DateFrom: $DateFrom, DateTo: $DateTo, FacetParams: $FacetParams) {
    Results {
      FriendlyId
      Title
      DataFormat { Value }
    }
  }
}"""

DETAIL_QUERY = """query GetStudyItem($FriendlyId: String) {
  getStudyItem(FriendlyId: $FriendlyId) {
    FriendlyId
    Title
    DOI
    TypeOfAccess
    AccessCondition
    Abstract
    LanguageOfStudyDescription
    PublicationDate
    Version
    KindOfData
    DataFormat { Id Value Comment }
    Documents { Description Name Size Type Uri }
    DataCollectionMethodology { Id Value }
    Creator { Organisations Individuals }
    Depositor { Organisations Individuals }
    DataCollector
    Keyword { Id Value }
    Subject
    MainTopics
    TimeMethod { Id Value Comment }
  }
}"""

FILE_LIST_QUERY = """query GetFileList($BucketName: String!, $FriendlyId: String!) {
  getFileList(BucketName: $BucketName, FriendlyId: $FriendlyId) {
    Format
    Key
  }
}"""

DOWNLOAD_QUERY = """query GetFileUrl($BucketName: String!, $Key: String!) {
  getFileUrl(BucketName: $BucketName, Key: $Key)
}"""


@dataclass(frozen=True)
class PipelineConfig:
    api_url: str
    headers: Dict[str, str]
    keywords: Any
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
        api_url="https://ohlhy6cg7nhwtpuer664aeok2i.appsync-api.eu-west-2.amazonaws.com/graphql",
        headers={"Content-Type": "application/json", "x-api-key": "da2-dbqlla2y3jf2vaqev4lcrpiq4a"},
        keywords=[],
        timeout=60,
        base_dir="downloads",
        repo_name="uk-data-service",
        db_path="23041405-seeding.db",
        repository_id=3,
        repository_url="https://datacatalogue.ukdataservice.ac.uk",
        download_method="API-CALL",
        schema_path=str(script_dir / "schema.sql"),
        log_file_name="uk_data_service.log",
        score_table="qualitative_scores_ukdata",
    )


def graphql(
    *,
    session: requests.Session,
    config: PipelineConfig,
    query: str,
    variables: Dict[str, Any],
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    try:
        logger.debug("GraphQL request: variables=%s", variables)
        response = session.post(
            config.api_url,
            headers=config.headers,
            json={"query": query, "variables": variables},
            timeout=config.timeout,
        )
        response.raise_for_status()
        data = response.json()
        if "errors" in data:
            logger.error("GraphQL returned errors: %s", data["errors"])
            return None
        return data
    except Exception:
        logger.exception("GraphQL request failed: variables=%s", variables)
        return None


def extract_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        parts = [str(value[k]) for k in ("Value", "Name", "Description") if value.get(k) is not None]
        return " ".join(parts) if parts else " ".join(str(v) for v in value.values() if v is not None)
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            if item is None:
                continue
            out.append(extract_text(item) if isinstance(item, dict) else str(item))
        return " ".join(out)
    return str(value)


def normalize_text(value: Any) -> str:
    return extract_text(value).lower().strip()


def any_contains(text: str, patterns: List[str]) -> bool:
    return any(pattern.lower() in text for pattern in patterns)


def get_ext(name: Optional[str]) -> str:
    return name.split(".")[-1].lower() if name and "." in name else "unknown"


def collect_people(metadata: Dict[str, Any]) -> List[str]:
    people: Set[str] = set()

    for block in (metadata.get("Creator") or {}, metadata.get("Depositor") or {}):
        for key in ("Organisations", "Individuals"):
            for name in block.get(key, []) or []:
                if name:
                    people.add(str(name).strip())

    collectors = metadata.get("DataCollector")
    if isinstance(collectors, list):
        for name in collectors:
            if name:
                people.add(str(name).strip())
    elif collectors:
        people.add(str(collectors).strip())

    return sorted(person for person in people if person)


def clean_html_text(value: Any) -> str:
    text = html.unescape(extract_text(value))
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.I)
    text = re.sub(r"</p\s*>", " ", text, flags=re.I)
    text = re.sub(r"<li\s*>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip(" .;,-")


def normalize_license(access_condition: Any, type_of_access: Any) -> str:
    raw = clean_html_text(access_condition)
    if not raw:
        return ""

    lower = raw.lower()

    cc_patterns = [
        ("creative commons attribution-noncommercial-no derivatives", "CC BY-NC-ND"),
        ("creative commons attribution-noncommercial-noderivatives", "CC BY-NC-ND"),
        ("creative commons attribution-no derivatives", "CC BY-ND"),
        ("creative commons attribution-noderivatives", "CC BY-ND"),
        ("creative commons attribution-noncommercial-sharealike", "CC BY-NC-SA"),
        ("creative commons attribution-noncommercial-share alike", "CC BY-NC-SA"),
        ("creative commons attribution-noncommercial", "CC BY-NC"),
        ("creative commons attribution-sharealike", "CC BY-SA"),
        ("creative commons attribution-share alike", "CC BY-SA"),
        ("creative commons attribution", "CC BY"),
    ]
    for pattern, base in cc_patterns:
        if pattern in lower:
            version = re.search(r"\b([1-9](?:\.\d+)?)\b", raw)
            return f"{base} {version.group(1)}" if version else base

    if "open government licence" in lower:
        return "Open Government Licence"
    if "end user licence agreement" in lower:
        return "End User Licence Agreement"

    negative_markers = [
        "the data collection is available to",
        "the data collection is to be made available",
        "commercial use",
        "users must",
        "registered users must",
        "use of the data requires approval",
        "approval from the data owner",
        "these data are available from",
        "these data are currently unavailable",
        "host archive conditions apply",
        "access can be arranged",
        "further information is available",
        "must be accessed via",
        "publications: permission",
        "principal investigator",
    ]
    if any(marker in lower for marker in negative_markers):
        return ""

    access = normalize_text(type_of_access)
    concise = len(raw) <= 120 and raw.count(".") <= 1 and raw.count(":") <= 1
    if concise and access not in {"controlled", "safeguarded", "open"}:
        return raw

    return ""


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


def log_study_summary(logger: logging.Logger, study_id: str, metadata: Dict[str, Any]) -> None:
    logger.debug(
        "Study metadata: source_project_id=%s title=%s kindOfData=%s dataFormat=%s documents=%s license=%s",
        study_id,
        metadata.get("Title") or "<no-title>",
        extract_text(metadata.get("KindOfData")),
        extract_text(metadata.get("DataFormat")),
        extract_text(metadata.get("Documents")),
        normalize_license(metadata.get("AccessCondition"), metadata.get("TypeOfAccess")),
    )
    logger.debug(
        "Study text preview: source_project_id=%s abstract_preview=%s methodology=%s",
        study_id,
        extract_text(metadata.get("Abstract"))[:180],
        extract_text(metadata.get("DataCollectionMethodology")),
    )


def log_score_summary(logger: logging.Logger, source_project_id: str, score_result: ScoreResult) -> None:
    details = score_result["details"]
    logger.debug(
        "Score breakdown: source_project_id=%s total=%s label=%s kindOfData=%s dataFormat=%s documents=%s abstract=%s methodology=%s reasons=%s",
        source_project_id,
        score_result["total_score"],
        score_result["label"],
        details["kind_of_data_score"],
        details["data_format_score"],
        details["documents_score"],
        details["abstract_score"],
        details["methodology_score"],
        " | ".join(score_result["reasons"]),
    )


def score_kind_of_data(kind_of_data: Any) -> Tuple[int, List[str]]:
    text = normalize_text(kind_of_data)
    if not text:
        return -1, ["KindOfData missing"]

    strong_positive = ["qualitative and mixed methods data"]
    medium_positive = ["cohort and longitudinal studies", "cross-national survey data", "historical data", "geospatial data"]
    weak_positive = ["other surveys", "teaching data", "time series data", "uk survey data"]
    strong_negative = [
        "administrative data",
        "business microdata",
        "census data",
        "experimental data",
        "international data access network",
        "international macrodata",
        "international microdata",
        "synthetic data",
    ]

    if any_contains(text, strong_positive):
        return 4, ["KindOfData indicates qualitative and mixed methods data"]
    if any_contains(text, medium_positive):
        return 2, ["KindOfData is a possible qualitative-related category"]
    if any_contains(text, weak_positive):
        return 1, ["KindOfData is a weak qualitative signal"]
    if any_contains(text, strong_negative):
        return -2, ["KindOfData strongly suggests non-qualitative data"]
    return -1, ["KindOfData does not match qualitative-oriented categories"]


def score_data_format(data_format: Any) -> Tuple[int, List[str]]:
    text = normalize_text(data_format)
    if not text:
        return -1, ["DataFormat missing"]

    strong_positive = [
        "qualitative",
        "transcript",
        "transcripts",
        "interview",
        "interviews",
        "semi structured interview",
        "conducted interview",
    ]
    weak_positive = ["text", "audio", "video", "image", "still image", "other", "various others"]

    if any_contains(text, strong_positive):
        return 2, ["DataFormat contains strong qualitative-related terms"]
    if any_contains(text, weak_positive):
        return 1, ["DataFormat contains a weak but relevant qualitative signal"]
    return -1, ["DataFormat does not show qualitative-related content"]


def score_documents(documents: Any) -> Tuple[int, List[str]]:
    if not documents:
        return -1, ["Documents missing"]

    text = normalize_text(documents if isinstance(documents, list) else [documents])
    score = -1
    reasons: List[str] = []

    if "ulist" in text:
        return 3, [f"Documents contains file list style file: {text}"]
    if any(ext in text for ext in [".rtf", ".doc", ".docx", ".txt"]):
        score = max(score, 2)
        reasons.append(f"Documents contains likely text data file: {text}")
    if ".pdf" in text:
        score = max(score, 1)
        reasons.append(f"Documents contains PDF document: {text}")
    if any_contains(text, ["interview", "transcript", "qualitative"]):
        score = max(score, 2)
        reasons.append(f"Documents metadata hints at qualitative content: {text}")

    if score == -1:
        return -1, ["Documents do not show clear qualitative evidence"]
    return score, list(dict.fromkeys(reasons))


def score_abstract(abstract: Any) -> Tuple[int, List[str]]:
    text = normalize_text(abstract)
    if not text:
        return -1, ["Abstract missing"]

    strong_positive = ["qualitative", "interview", "transcript", "focus group"]
    medium_positive = ["narrative", "lived experience", "ethnography"]

    if any_contains(text, strong_positive):
        return 3, ["Abstract contains strong qualitative keywords"]
    if any_contains(text, medium_positive):
        return 2, ["Abstract contains medium qualitative keywords"]
    return -1, ["Abstract does not contain qualitative keywords"]


def score_data_collection_methodology(methodology: Any) -> Tuple[int, List[str]]:
    text = normalize_text(methodology)
    if not text:
        return -1, ["DataCollectionMethodology missing"]

    strong_positive = ["interview", "focus group", "transcription"]
    medium_positive = ["oral history", "audio recording", "diary", "video recording", "recording"]

    if any_contains(text, strong_positive):
        return 3, ["DataCollectionMethodology contains strong qualitative methods"]
    if any_contains(text, medium_positive):
        return 2, ["DataCollectionMethodology contains qualitative-related methods"]
    return -1, ["DataCollectionMethodology does not indicate qualitative methods"]


def score_study(metadata: Dict[str, Any]) -> ScoreResult:
    kind_score, kind_reason = score_kind_of_data(metadata.get("KindOfData"))
    format_score, format_reason = score_data_format(metadata.get("DataFormat"))
    docs_score, docs_reason = score_documents(metadata.get("Documents"))
    abs_score, abs_reason = score_abstract(metadata.get("Abstract"))
    method_score, method_reason = score_data_collection_methodology(metadata.get("DataCollectionMethodology"))

    total = kind_score + format_score + docs_score + abs_score + method_score
    label = "LIKELY_QUALITATIVE" if total >= 10 else "POSSIBLE_QUALITATIVE" if total >= 6 else "UNCERTAIN" if total >= 1 else "UNLIKELY"

    return {
        "total_score": total,
        "label": label,
        "details": {
            "kind_of_data_score": kind_score,
            "data_format_score": format_score,
            "documents_score": docs_score,
            "abstract_score": abs_score,
            "methodology_score": method_score,
        },
        "reasons": kind_reason + format_reason + docs_reason + abs_reason + method_reason,
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


def insert_score(
    db: DatabaseManagement,
    *,
    db_project_id: int,
    source_project_id: str,
    score_result: ScoreResult,
) -> None:
    db.execute("""
        INSERT INTO qualitative_scores_ukdata (
            project_id,
            source_project_id,
            total_score,
            label,
            kind_of_data_score,
            data_format_score,
            documents_score,
            abstract_score,
            methodology_score,
            reasons,
            timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        db_project_id,
        source_project_id,
        score_result["total_score"],
        score_result["label"],
        score_result["details"]["kind_of_data_score"],
        score_result["details"]["data_format_score"],
        score_result["details"]["documents_score"],
        score_result["details"]["abstract_score"],
        score_result["details"]["methodology_score"],
        " | ".join(score_result["reasons"]),
        utc_now_iso(),
    ))


def write_project_snapshot(
    db: DatabaseManagement,
    *,
    config: PipelineConfig,
    source_project_id: str,
    keywords: List[str],
    metadata: Dict[str, Any],
    score_result: ScoreResult,
    file_rows: List[FileRow],
) -> int:
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
        "|".join(keywords),
        config.repository_id,
        config.repository_url,
        f"{config.repository_url}/studies/study/{source_project_id}",
        str(metadata.get("Version")),
        metadata.get("Title"),
        metadata.get("Abstract"),
        extract_text(metadata.get("LanguageOfStudyDescription")),
        metadata.get("DOI"),
        metadata.get("PublicationDate"),
        utc_now_iso(),
        config.repo_name,
        str(source_project_id),
        None,
        config.download_method,
    ))
    db_project_id = int(cursor.lastrowid)

    for keyword in keywords:
        db.execute("INSERT INTO keywords (project_id, keyword) VALUES (?, ?)", (db_project_id, keyword))

    license_value = normalize_license(metadata.get("AccessCondition"), metadata.get("TypeOfAccess"))
    if license_value:
        db.execute("INSERT INTO licenses (project_id, license) VALUES (?, ?)", (db_project_id, license_value))

    for person in collect_people(metadata):
        db.execute("INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)", (db_project_id, person, "UNKNOWN"))

    for file_name, file_type, status in file_rows:
        db.execute("INSERT INTO files (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)", (db_project_id, file_name, file_type, status))

    insert_score(db, db_project_id=db_project_id, source_project_id=str(source_project_id), score_result=score_result)
    return db_project_id


def create_summary(config: PipelineConfig, search_terms: List[str]) -> Dict[str, Any]:
    return {
        "repository": config.repo_name,
        "repository_id": config.repository_id,
        "run_started_at": utc_now_iso(),
        "run_finished_at": None,
        "search_terms": search_terms,
        "bucket_rule_note": "For UKDS, projects_with_numeric_data follows DataFormat == 'numeric'. File presence for non-numeric projects is based on open file-list availability or restricted access types.",
        "counts": {
            "received_results": 0,
            "unique_projects_seen": 0,
            "processed_projects": 0,
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


def infer_has_files_from_metadata(metadata: Dict[str, Any]) -> Optional[bool]:
    access = normalize_text(metadata.get("TypeOfAccess")) or "unknown"
    if access in {"safeguarded", "controlled"}:
        return True
    return bool(normalize_text(metadata.get("Documents")))


def stage_open_files(
    *,
    session: requests.Session,
    config: PipelineConfig,
    paths: RepoPaths,
    logger: logging.Logger,
    source_project_id: str,
) -> Tuple[Optional[str], List[FileRow], List[str], Optional[str]]:
    file_data = graphql(
        session=session,
        config=config,
        query=FILE_LIST_QUERY,
        variables={"BucketName": "", "FriendlyId": source_project_id},
        logger=logger,
    )
    if not file_data:
        return None, [], [], "Failed to fetch file list"

    files = ((file_data.get("data") or {}).get("getFileList")) or []
    if not files:
        return None, [(None, None, STATUS_FAILED_SERVER_UNRESPONSIVE)], ["Open study has no files in file list"], None

    project_tmp_dir = make_temp_dir(paths.tmp_root, "stage", source_project_id)
    file_rows: List[FileRow] = []
    seen_names: Set[str] = set()

    for file_meta in files:
        key = file_meta.get("Key")
        if not key:
            safe_rmtree(project_tmp_dir)
            return None, [], [], "File list contains an entry without a key"

        filename = key.split("/")[-1]
        if not filename:
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Unable to derive filename from file key {key}"

        if filename in seen_names:
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Duplicate filename detected in file list: {filename}"
        seen_names.add(filename)

        url_data = graphql(
            session=session,
            config=config,
            query=DOWNLOAD_QUERY,
            variables={"BucketName": "", "Key": key},
            logger=logger,
        )
        if not url_data:
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Failed to fetch download URL for file key {key}"

        url = (url_data.get("data") or {}).get("getFileUrl")
        if not url:
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Download URL missing for file key {key}"

        path = str(Path(project_tmp_dir) / filename)
        if download_file(url=url, path=path, timeout=config.timeout, session=session, logger=logger) != STATUS_SUCCEEDED:
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Failed to download file {filename}"

        file_rows.append((filename, get_ext(filename), STATUS_SUCCEEDED))

    return project_tmp_dir, file_rows, [], None


def build_refresh_payload(
    *,
    session: requests.Session,
    config: PipelineConfig,
    paths: RepoPaths,
    logger: logging.Logger,
    source_project_id: str,
    metadata: Dict[str, Any],
) -> Tuple[List[FileRow], Optional[str], List[str], Optional[str]]:
    access = normalize_text(metadata.get("TypeOfAccess")) or "unknown"
    if access == "open":
        staged_dir, file_rows, logs, error = stage_open_files(
            session=session,
            config=config,
            paths=paths,
            logger=logger,
            source_project_id=source_project_id,
        )
        return file_rows, staged_dir, logs, error
    if access in {"safeguarded", "controlled"}:
        return [(None, None, STATUS_FAILED_LOGIN_REQUIRED)], None, [], None
    return [(None, None, STATUS_FAILED_SERVER_UNRESPONSIVE)], None, [f"Unknown access type: {access}"], None


def determine_project_has_files(
    *,
    session: requests.Session,
    config: PipelineConfig,
    logger: logging.Logger,
    source_project_id: str,
    metadata: Dict[str, Any],
) -> bool:
    access = normalize_text(metadata.get("TypeOfAccess")) or "unknown"

    if access in {"safeguarded", "controlled"}:
        return True

    if access == "open":
        file_data = graphql(
            session=session,
            config=config,
            query=FILE_LIST_QUERY,
            variables={"BucketName": "", "FriendlyId": source_project_id},
            logger=logger,
        )
        if not file_data:
            logger.warning("Could not determine file presence from open file list: source_project_id=%s", source_project_id)
            return False
        return bool(((file_data.get("data") or {}).get("getFileList")) or [])

    return bool(normalize_text(metadata.get("Documents")))


def collect_project_keywords(
    *,
    session: requests.Session,
    config: PipelineConfig,
    search_terms: List[str],
    logger: logging.Logger,
    summary: Dict[str, Any],
) -> Tuple[Dict[str, Set[str]], List[str]]:
    project_keywords: Dict[str, Set[str]] = {}
    project_order: List[str] = []

    for keyword in search_terms:
        display_keyword = keyword if keyword else "ALL ITEMS"
        stored_keyword = keyword if keyword else "*"
        logger.info("Searching repository: query=%s", display_keyword)
        start = 0

        while True:
            data = graphql(
                session=session,
                config=config,
                query=SEARCH_QUERY,
                variables={
                    "QueryString": keyword if keyword is not None else "",
                    "Start": start,
                    "Rows": 50,
                    "Sort": 2,
                    "Phrase": False,
                    "DateFrom": 440,
                    "DateTo": 2026,
                    "FacetParams": "",
                },
                logger=logger,
            )
            if not data:
                logger.error("Search query failed: keyword=%s start=%s", stored_keyword, start)
                break

            results = ((data.get("data") or {}).get("getStudyList") or {}).get("Results") or []
            summary["counts"]["received_results"] += len(results)
            logger.debug("Search page received: query=%s start=%s items=%s", stored_keyword, start, len(results))

            if not results:
                break

            for result in results:
                study_id = result["FriendlyId"]
                if study_id not in project_keywords:
                    project_keywords[study_id] = set()
                    project_order.append(study_id)
                project_keywords[study_id].add(stored_keyword)

            start += 50

    summary["counts"]["unique_projects_seen"] = len(project_order)
    return project_keywords, project_order


def process_project(
    *,
    session: requests.Session,
    db: DatabaseManagement,
    config: PipelineConfig,
    paths: RepoPaths,
    logger: logging.Logger,
    summary: Dict[str, Any],
    allowed_labels: Set[str],
    study_id: str,
    keywords: List[str],
) -> None:
    detail = graphql(
        session=session,
        config=config,
        query=DETAIL_QUERY,
        variables={"FriendlyId": study_id},
        logger=logger,
    )
    if not detail:
        summary["counts"]["failed_projects"] += 1
        log_project_decision(logger, project_id=study_id, action="failed", reason="detail-fetch", has_files=None, level=logging.ERROR)
        return

    metadata = ((detail.get("data") or {}).get("getStudyItem"))
    if not metadata:
        summary["counts"]["failed_projects"] += 1
        log_project_decision(logger, project_id=study_id, action="failed", reason="empty-detail", has_files=None, level=logging.ERROR)
        return

    log_study_summary(logger, study_id, metadata)

    project_url = f"{config.repository_url}/studies/study/{study_id}"
    existing_db_project_id = get_existing_db_project_id(db, repository_id=config.repository_id, project_url=project_url)
    existing_folder_name = get_project_folder_by_db_id(db, existing_db_project_id)

    if normalize_text(metadata.get("DataFormat")) == "numeric":
        summary["counts"]["skipped_projects"] += 1
        summary["projects_with_numeric_data"] += 1
        has_files = infer_has_files_from_metadata(metadata)

        if not purge_project_snapshot_aligned(
            db=db,
            db_project_id=existing_db_project_id,
            source_project_id=study_id,
            repo_dir=paths.repo_dir,
            tmp_root=paths.tmp_root,
            current_folder_name=existing_folder_name,
            score_table=config.score_table,
            logger=logger,
        ):
            summary["counts"]["failed_projects"] += 1
            log_project_decision(logger, project_id=study_id, action="failed", reason="purge-failed", has_files=has_files, level=logging.ERROR)
            return

        log_project_decision(logger, project_id=study_id, action="skipped", reason="numeric-only", has_files=has_files)
        return

    summary["counts"]["processed_projects"] += 1
    score_result = score_study(metadata)
    log_score_summary(logger, study_id, score_result)

    has_files = determine_project_has_files(
        session=session,
        config=config,
        logger=logger,
        source_project_id=study_id,
        metadata=metadata,
    )
    is_qualitative = score_result["label"] in allowed_labels
    increment_classification_bucket(summary, is_qualitative=is_qualitative, has_files=has_files)

    if not is_qualitative:
        summary["counts"]["skipped_projects"] += 1

        if not purge_project_snapshot_aligned(
            db=db,
            db_project_id=existing_db_project_id,
            source_project_id=study_id,
            repo_dir=paths.repo_dir,
            tmp_root=paths.tmp_root,
            current_folder_name=existing_folder_name,
            score_table=config.score_table,
            logger=logger,
        ):
            summary["counts"]["failed_projects"] += 1
            log_project_decision(logger, project_id=study_id, action="failed", reason="purge-failed", has_files=has_files, level=logging.ERROR)
            return

        log_project_decision(
            logger,
            project_id=study_id,
            action="skipped",
            score=score_result["total_score"],
            label=score_result["label"],
            has_files=has_files,
        )
        return

    file_rows, staged_project_dir, log_messages, hard_error = build_refresh_payload(
        session=session,
        config=config,
        paths=paths,
        logger=logger,
        source_project_id=study_id,
        metadata=metadata,
    )
    if hard_error:
        summary["counts"]["skipped_projects"] += 1
        summary["counts"]["failed_projects"] += 1
        log_project_decision(logger, project_id=study_id, action="failed", reason="staging-error", has_files=has_files, level=logging.ERROR)
        logger.error("Staging detail: source_project_id=%s error=%s", study_id, hard_error)
        return

    refreshed = replace_project_snapshot_aligned(
        db=db,
        existing_db_project_id=existing_db_project_id,
        source_project_id=study_id,
        repo_dir=paths.repo_dir,
        tmp_root=paths.tmp_root,
        current_folder_name=str(study_id),
        previous_folder_name=existing_folder_name,
        staged_project_dir=staged_project_dir,
        score_table=config.score_table,
        write_db_rows_fn=lambda: write_project_snapshot(
            db=db,
            config=config,
            source_project_id=study_id,
            keywords=keywords,
            metadata=metadata,
            score_result=score_result,
            file_rows=file_rows,
        ),
        logger=logger,
    )
    if not refreshed:
        summary["counts"]["skipped_projects"] += 1
        summary["counts"]["failed_projects"] += 1
        log_project_decision(logger, project_id=study_id, action="failed", reason="refresh-failed", has_files=has_files, level=logging.ERROR)
        return

    for message in log_messages:
        logger.warning("Refresh note: source_project_id=%s message=%s", study_id, message)

    summary["counts"]["saved_projects"] += 1
    log_project_decision(
        logger,
        project_id=study_id,
        action="saved",
        score=score_result["total_score"],
        label=score_result["label"],
        has_files=has_files,
    )


def run() -> None:
    config = build_config()
    paths = build_repo_paths(base_dir=config.base_dir, repo_name=config.repo_name)
    logger = setup_logger(
        logger_name="repo_uk_data_service",
        log_file_path=str(Path(paths.log_dir) / config.log_file_name),
        console_level=logging.INFO,
        file_level=logging.DEBUG,
    )

    search_terms = normalize_search_terms(config.keywords, empty_value="")
    summary = create_summary(config, search_terms)
    allowed_labels = {"LIKELY_QUALITATIVE", "POSSIBLE_QUALITATIVE"}

    logger.info("UK Data Service qualitative pipeline started")
    logger.debug("Pipeline configuration: %s", config)

    session = requests.Session()
    db = DatabaseManagement(db_path=config.db_path, schema_path=config.schema_path)

    try:
        project_keywords, project_order = collect_project_keywords(
            session=session,
            config=config,
            search_terms=search_terms,
            logger=logger,
            summary=summary,
        )

        for study_id in project_order:
            keywords = sorted(project_keywords.get(study_id, {"*"}))
            try:
                process_project(
                    session=session,
                    db=db,
                    config=config,
                    paths=paths,
                    logger=logger,
                    summary=summary,
                    allowed_labels=allowed_labels,
                    study_id=study_id,
                    keywords=keywords,
                )
            except Exception:
                summary["counts"]["skipped_projects"] += 1
                summary["counts"]["failed_projects"] += 1
                log_project_decision(logger, project_id=study_id, action="failed", reason="unhandled-exception", has_files=None, level=logging.ERROR)
                logger.exception("Unhandled project processing error: source_project_id=%s", study_id)

    finally:
        summary["run_finished_at"] = utc_now_iso()
        run_tag = str(summary["run_started_at"]).replace(":", "").replace("-", "").replace(".", "").replace("T", "_")
        summary_path = str(Path(paths.summary_dir) / f"{config.repo_name}_summary_{run_tag}.json")
        write_json_summary(summary_path, summary)

        logger.info(
            "Pipeline finished: processed=%s saved=%s skipped=%s received_results=%s summary=%s",
            summary["counts"]["processed_projects"],
            summary["counts"]["saved_projects"],
            summary["counts"]["skipped_projects"],
            summary["counts"]["received_results"],
            summary_path,
        )

        session.close()
        db.close()


if __name__ == "__main__":
    run()