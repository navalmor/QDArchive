import os
import shutil
import sqlite3
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import requests

# -----------------------------------
# CONFIG
# -----------------------------------
API_URL = "https://ohlhy6cg7nhwtpuer664aeok2i.appsync-api.eu-west-2.amazonaws.com/graphql"

HEADERS = {
    "Content-Type": "application/json",
    "x-api-key": "da2-dbqlla2y3jf2vaqev4lcrpiq4a",
}

# Set to None or [] to search all items, or provide a list of keywords
KEYWORDS = []

BASE_DIR = "downloads"
REPO_NAME = "uk-data-service"
REPO_DIR = os.path.join(BASE_DIR, REPO_NAME)
TMP_ROOT = os.path.join(REPO_DIR, "_tmp")

DB_PATH = "23041405-seeding.db"

REPOSITORY_ID = 3
REPOSITORY_URL = "https://datacatalogue.ukdataservice.ac.uk"
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
# QUERIES
# -----------------------------------
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

# -----------------------------------
# HELPERS
# -----------------------------------
def utc_now_iso() -> str:
    return datetime.utcnow().isoformat()


def make_project_key(repository_id: int, project_id: Any) -> int:
    return int(f"{repository_id}{project_id}")


def get_search_terms() -> List[str]:
    if KEYWORDS is None or KEYWORDS == []:
        return [""]
    if isinstance(KEYWORDS, str):
        return [KEYWORDS]
    return list(KEYWORDS)


def graphql(query: str, variables: Dict[str, Any]):
    try:
        response = requests.post(
            API_URL,
            headers=HEADERS,
            json={"query": query, "variables": variables},
            timeout=60,
        )
        data = response.json()
        if "errors" in data:
            print("GraphQL error:", data["errors"])
            return None
        return data
    except Exception as e:
        print("Request failed:", e)
        return None


def extract_text(value: Any) -> str:
    """
    Convert strings, lists, dicts, or nested lists into a single text string.
    """
    if value is None:
        return ""

    if isinstance(value, dict):
        parts = []
        for key in ["Value", "Name", "Description"]:
            if value.get(key) is not None:
                parts.append(str(value.get(key)))
        if not parts:
            parts = [str(v) for v in value.values() if v is not None]
        return " ".join(parts)

    if isinstance(value, list):
        parts = []
        for item in value:
            if item is None:
                continue
            if isinstance(item, dict):
                parts.append(extract_text(item))
            else:
                parts.append(str(item))
        return " ".join(parts)

    return str(value)


def normalize_text(value: Any) -> str:
    return extract_text(value).lower().strip()


def any_contains(text: str, patterns: List[str]) -> bool:
    return any(p.lower() in text for p in patterns)


def get_ext(name: Optional[str]) -> str:
    return name.split(".")[-1].lower() if name and "." in name else "unknown"


def map_status(access: str, download_status: Optional[str] = None) -> str:
    if access == "open":
        return download_status or "SUCCEEDED"
    if access == "safeguarded":
        return "FAILED_LOGIN_REQUIRED"
    if access == "controlled":
        return "RESTRICTED"
    return "FAILED_SERVER_UNRESPONSIVE"


def safe_rmtree(path: str):
    if os.path.isdir(path):
        shutil.rmtree(path, ignore_errors=True)
    elif os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass


def make_temp_project_dir(project_id: Any, prefix: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%f")
    path = os.path.join(TMP_ROOT, f"{prefix}_{project_id}_{timestamp}")
    os.makedirs(path, exist_ok=True)
    return path


def download_file(url: str, path: str) -> str:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)

        with requests.get(url, timeout=60, stream=True) as response:
            if response.status_code != 200:
                return "FAILED_SERVER_UNRESPONSIVE"

            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)

        return "SUCCEEDED"
    except Exception:
        safe_rmtree(path)
        return "FAILED_SERVER_UNRESPONSIVE"


def log_failure(project_id: Any, reason: str):
    project_key = make_project_key(REPOSITORY_ID, project_id) if project_id is not None else None
    cur.execute(
        "INSERT INTO failures (project_key, reason, timestamp) VALUES (?, ?, ?)",
        (project_key, reason, utc_now_iso()),
    )


def print_study_summary(d: Dict[str, Any]):
    print("--------------------------------------------------")
    print(f"Title: {d.get('Title')}")
    print(f"KindOfData: {extract_text(d.get('KindOfData'))}")
    print(f"DataFormat: {extract_text(d.get('DataFormat'))}")
    print(f"Documents: {extract_text(d.get('Documents'))}")
    print(f"Abstract preview: {extract_text(d.get('Abstract'))[:180]}...")
    print(f"Methodology: {extract_text(d.get('DataCollectionMethodology'))}")
    print("--------------------------------------------------")


def delete_project_rows(project_key: int):
    tables = [
        "keywords",
        "licenses",
        "person_role",
        "files",
        "qualitative_scores_ukdata",
        "projects",
    ]
    for table in tables:
        cur.execute(f"DELETE FROM {table} WHERE project_key = ?", (project_key,))


def collect_people(d: Dict[str, Any]) -> List[str]:
    people: Set[str] = set()

    creator = d.get("Creator") or {}
    for name in creator.get("Organisations", []) or []:
        if name:
            people.add(str(name).strip())
    for name in creator.get("Individuals", []) or []:
        if name:
            people.add(str(name).strip())

    depositor = d.get("Depositor") or {}
    for name in depositor.get("Organisations", []) or []:
        if name:
            people.add(str(name).strip())
    for name in depositor.get("Individuals", []) or []:
        if name:
            people.add(str(name).strip())

    collectors = d.get("DataCollector")
    if collectors:
        if isinstance(collectors, list):
            for name in collectors:
                if name:
                    people.add(str(name).strip())
        else:
            people.add(str(collectors).strip())

    return sorted(p for p in people if p)


def insert_score(project_id: Any, score_result: Dict[str, Any]):
    project_key = make_project_key(REPOSITORY_ID, project_id)
    cur.execute("""
        INSERT INTO qualitative_scores_ukdata (
            project_key,
            project_id,
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
        project_key,
        project_id,
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


def print_score_summary(project_id: Any, score_result: Dict[str, Any]):
    details = score_result["details"]
    print(f"Score summary for {project_id}:")
    print(f"  Total: {score_result['total_score']}")
    print(f"  Label: {score_result['label']}")
    print(f"  KindOfData: {details['kind_of_data_score']}")
    print(f"  DataFormat: {details['data_format_score']}")
    print(f"  Documents: {details['documents_score']}")
    print(f"  Abstract: {details['abstract_score']}")
    print(f"  Methodology: {details['methodology_score']}")
    print(f"  Reasons: {' | '.join(score_result['reasons'])}")


def stage_open_files(project_id: Any) -> Tuple[Optional[str], List[Tuple[Optional[str], Optional[str], str]], List[str], Optional[str]]:
    file_data = graphql(FILE_LIST_QUERY, {
        "BucketName": "",
        "FriendlyId": project_id,
    })
    if not file_data:
        return None, [], [], "Failed to fetch file list"

    files = ((file_data.get("data") or {}).get("getFileList")) or []
    if not files:
        return None, [(None, None, "FAILED_SERVER_UNRESPONSIVE")], ["Open study has no files in file list"], None

    project_tmp_dir = make_temp_project_dir(project_id, "stage")
    file_rows: List[Tuple[Optional[str], Optional[str], str]] = []
    seen_names: Set[str] = set()

    for f in files:
        key = f.get("Key")
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

        url_data = graphql(DOWNLOAD_QUERY, {
            "BucketName": "",
            "Key": key,
        })
        if not url_data:
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Failed to fetch download URL for file key {key}"

        url = (url_data.get("data") or {}).get("getFileUrl")
        if not url:
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Download URL missing for file key {key}"

        path = os.path.join(project_tmp_dir, filename)
        dl_status = download_file(url, path)
        if dl_status != "SUCCEEDED":
            safe_rmtree(project_tmp_dir)
            return None, [], [], f"Failed to download file {filename}"

        file_rows.append((filename, get_ext(filename), "SUCCEEDED"))

    return project_tmp_dir, file_rows, [], None


def write_project_snapshot(
    project_id: Any,
    keywords: List[str],
    metadata: Dict[str, Any],
    score_result: Dict[str, Any],
    file_rows: List[Tuple[Optional[str], Optional[str], str]],
):
    project_key = make_project_key(REPOSITORY_ID, project_id)

    cur.execute("""
    INSERT INTO projects (
        project_key, project_id, query_string, repository_id, repository_url, project_url,
        version, title, description, language, doi, upload_date,
        download_date, download_repository_folder, download_project_folder, download_method
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        project_key,
        project_id,
        "|".join(keywords),
        REPOSITORY_ID,
        REPOSITORY_URL,
        f"{REPOSITORY_URL}/studies/study/{project_id}",
        str(metadata.get("Version")),
        metadata.get("Title"),
        metadata.get("Abstract"),
        extract_text(metadata.get("LanguageOfStudyDescription")),
        metadata.get("DOI"),
        metadata.get("PublicationDate"),
        utc_now_iso(),
        REPO_DIR,
        str(project_id),
        DOWNLOAD_METHOD,
    ))

    for keyword in keywords:
        cur.execute(
            "INSERT INTO keywords (project_key, keyword) VALUES (?, ?)",
            (project_key, keyword),
        )

    if metadata.get("AccessCondition"):
        cur.execute(
            "INSERT INTO licenses (project_key, license) VALUES (?, ?)",
            (project_key, metadata.get("AccessCondition")),
        )

    for person in collect_people(metadata):
        cur.execute(
            "INSERT INTO person_role (project_key, name, role) VALUES (?,?,?)",
            (project_key, person, "UNKNOWN"),
        )

    for file_name, file_type, status in file_rows:
        cur.execute("""
        INSERT INTO files (project_key, file_name, file_type, status)
        VALUES (?,?,?,?)
        """, (
            project_key,
            file_name,
            file_type,
            status,
        ))

    insert_score(project_id, score_result)


def replace_project_snapshot(
    project_id: Any,
    write_db_rows: Callable[[], None],
    staged_project_dir: Optional[str],
) -> bool:
    project_key = make_project_key(REPOSITORY_ID, project_id)
    final_project_dir = os.path.join(REPO_DIR, str(project_id))
    backup_project_dir: Optional[str] = None
    promoted_new_dir = False

    try:
        if os.path.exists(final_project_dir):
            backup_project_dir = make_temp_project_dir(project_id, "backup")
            safe_rmtree(backup_project_dir)
            os.replace(final_project_dir, backup_project_dir)

        conn.execute("BEGIN")
        delete_project_rows(project_key)
        write_db_rows()

        if staged_project_dir:
            os.replace(staged_project_dir, final_project_dir)
            promoted_new_dir = True

        conn.commit()

        if backup_project_dir and os.path.exists(backup_project_dir):
            safe_rmtree(backup_project_dir)

        return True

    except Exception as e:
        conn.rollback()

        if promoted_new_dir and os.path.exists(final_project_dir):
            safe_rmtree(final_project_dir)
        elif staged_project_dir and os.path.exists(staged_project_dir):
            safe_rmtree(staged_project_dir)

        if backup_project_dir and os.path.exists(backup_project_dir):
            if os.path.exists(final_project_dir):
                safe_rmtree(final_project_dir)
            os.replace(backup_project_dir, final_project_dir)

        print(f"Project refresh failed for {project_id}: {e}")
        return False


def purge_project_snapshot(project_id: Any) -> bool:
    return replace_project_snapshot(project_id, lambda: None, None)


# -----------------------------------
# SCORING FUNCTIONS
# -----------------------------------
def score_kind_of_data(kind_of_data: Any) -> Tuple[int, List[str]]:
    text = normalize_text(kind_of_data)

    if not text:
        return -1, ["KindOfData missing"]

    strong_positive = [
        "qualitative and mixed methods data",
    ]

    medium_positive = [
        "cohort and longitudinal studies",
        "cross-national survey data",
        "historical data",
        "geospatial data",
    ]

    weak_positive = [
        "other surveys",
        "teaching data",
        "time series data",
        "uk survey data",
    ]

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

    strong_positive_patterns = [
        "qualitative",
        "transcript",
        "transcripts",
        "interview",
        "interviews",
        "semi structured interview",
        "conducted interview",
    ]

    weak_positive_patterns = [
        "text",
        "audio",
        "video",
        "image",
        "still image",
        "other",
        "various others",
    ]

    if any_contains(text, strong_positive_patterns):
        return 2, ["DataFormat contains strong qualitative-related terms"]

    if any_contains(text, weak_positive_patterns):
        return 1, ["DataFormat contains a weak but relevant qualitative signal"]

    return -1, ["DataFormat does not show qualitative-related content"]


def score_documents(documents: Any) -> Tuple[int, List[str]]:
    if not documents:
        return -1, ["Documents missing"]

    if not isinstance(documents, list):
        documents = [documents]

    score = -1
    reasons: List[str] = []

    text = normalize_text(documents)

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

    reasons = list(dict.fromkeys(reasons))
    return score, reasons


def score_abstract(abstract: Any) -> Tuple[int, List[str]]:
    text = normalize_text(abstract)

    if not text:
        return -1, ["Abstract missing"]

    strong_positive = [
        "qualitative",
        "interview",
        "transcript",
        "focus group",
    ]

    medium_positive = [
        "narrative",
        "lived experience",
        "ethnography",
    ]

    if any_contains(text, strong_positive):
        return 3, ["Abstract contains strong qualitative keywords"]

    if any_contains(text, medium_positive):
        return 2, ["Abstract contains medium qualitative keywords"]

    return -1, ["Abstract does not contain qualitative keywords"]


def score_data_collection_methodology(methodology: Any) -> Tuple[int, List[str]]:
    text = normalize_text(methodology)

    if not text:
        return -1, ["DataCollectionMethodology missing"]

    strong_positive = [
        "interview",
        "focus group",
        "transcription",
    ]

    medium_positive = [
        "oral history",
        "audio recording",
        "diary",
        "video recording",
        "recording",
    ]

    if any_contains(text, strong_positive):
        return 3, ["DataCollectionMethodology contains strong qualitative methods"]

    if any_contains(text, medium_positive):
        return 2, ["DataCollectionMethodology contains qualitative-related methods"]

    return -1, ["DataCollectionMethodology does not indicate qualitative methods"]


def score_study(metadata: Dict[str, Any]) -> Dict[str, Any]:
    kind_score, kind_reason = score_kind_of_data(metadata.get("KindOfData"))
    format_score, format_reason = score_data_format(metadata.get("DataFormat"))
    docs_score, docs_reason = score_documents(metadata.get("Documents"))
    abs_score, abs_reason = score_abstract(metadata.get("Abstract"))
    method_score, method_reason = score_data_collection_methodology(metadata.get("DataCollectionMethodology"))

    total = kind_score + format_score + docs_score + abs_score + method_score

    if total >= 10:
        label = "LIKELY_QUALITATIVE"
    elif total >= 6:
        label = "POSSIBLE_QUALITATIVE"
    elif total >= 1:
        label = "UNCERTAIN"
    else:
        label = "UNLIKELY"

    reasons = kind_reason + format_reason + docs_reason + abs_reason + method_reason

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
        "reasons": reasons,
    }


def build_refresh_payload(
    project_id: Any,
    metadata: Dict[str, Any],
) -> Tuple[List[Tuple[Optional[str], Optional[str], str]], Optional[str], List[str], Optional[str]]:
    access = normalize_text(metadata.get("TypeOfAccess")) or "unknown"
    log_messages: List[str] = []

    if access == "open":
        staged_dir, file_rows, extra_logs, hard_error = stage_open_files(project_id)
        log_messages.extend(extra_logs)
        return file_rows, staged_dir, log_messages, hard_error

    if access == "safeguarded":
        return [(None, None, "FAILED_LOGIN_REQUIRED")], None, [], None

    if access == "controlled":
        return [(None, None, "RESTRICTED")], None, [], None

    log_messages.append(f"Unknown access type: {access}")
    return [(None, None, "FAILED_SERVER_UNRESPONSIVE")], None, log_messages, None


# -----------------------------------
# PIPELINE
# -----------------------------------
def run():
    print("\n🚀 FINAL PIPELINE\n")

    skipped = 0
    processed = 0
    received_results = 0
    search_terms = get_search_terms()
    allowed_labels = {"LIKELY_QUALITATIVE", "POSSIBLE_QUALITATIVE"}

    project_keywords: Dict[Any, Set[str]] = {}
    project_order: List[Any] = []

    for kw in search_terms:
        display_kw = kw if kw else "ALL ITEMS"
        stored_kw = kw if kw else "*"
        print(f"\n🔍 {display_kw}")

        start = 0

        while True:
            data = graphql(SEARCH_QUERY, {
                "QueryString": kw if kw is not None else "",
                "Start": start,
                "Rows": 50,
                "Sort": 2,
                "Phrase": False,
                "DateFrom": 440,
                "DateTo": 2026,
                "FacetParams": "",
            })

            if not data:
                log_failure(None, f"Search query failed for keyword '{stored_kw}' at start={start}")
                conn.commit()
                break

            results = ((data.get("data") or {}).get("getStudyList") or {}).get("Results") or []
            received_results += len(results)

            if not results:
                break

            for r in results:
                sid = r["FriendlyId"]

                if sid not in project_keywords:
                    project_keywords[sid] = set()
                    project_order.append(sid)

                project_keywords[sid].add(stored_kw)

            start += 50

    for sid in project_order:
        keywords = sorted(project_keywords.get(sid, {"*"}))

        print(f"\n📌 {sid}")

        detail = graphql(DETAIL_QUERY, {"FriendlyId": sid})
        if not detail:
            log_failure(sid, "Failed to fetch study detail")
            conn.commit()
            continue

        d = ((detail.get("data") or {}).get("getStudyItem"))
        if not d:
            log_failure(sid, "Study detail returned empty data")
            conn.commit()
            continue

        print_study_summary(d)

        if normalize_text(d.get("DataFormat")) == "numeric":
            skipped += 1
            print("DataFormat indicates only numeric data, removing existing local snapshot if present...")
            if not purge_project_snapshot(sid):
                log_failure(sid, "Failed to purge project snapshot after numeric-only classification")
            conn.commit()
            continue

        processed += 1
        score_result = score_study(d)
        print_score_summary(sid, score_result)

        if score_result["label"] not in allowed_labels:
            print(f"Label {score_result['label']} is not allowed, removing existing local snapshot if present...")
            if not purge_project_snapshot(sid):
                log_failure(sid, f"Failed to purge project snapshot for disallowed label {score_result['label']}")
            conn.commit()
            continue

        file_rows, staged_project_dir, log_messages, hard_error = build_refresh_payload(sid, d)
        if hard_error:
            log_failure(sid, hard_error)
            conn.commit()
            continue

        def write_rows():
            write_project_snapshot(
                project_id=sid,
                keywords=keywords,
                metadata=d,
                score_result=score_result,
                file_rows=file_rows,
            )

        refreshed = replace_project_snapshot(
            project_id=sid,
            write_db_rows=write_rows,
            staged_project_dir=staged_project_dir,
        )

        if not refreshed:
            log_failure(sid, "Atomic project refresh failed")
            conn.commit()
            continue

        for message in log_messages:
            log_failure(sid, message)
        conn.commit()

    print("\n🎉 DONE")
    print(f"Processed: {processed}, Skipped: {skipped}")
    print(f"Total projects: {processed + skipped}")
    print(f"Total received results: {received_results}")


# -----------------------------------
# RUN
# -----------------------------------
run()
conn.close()