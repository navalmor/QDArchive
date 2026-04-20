# QDArchive

QDArchive is a Python-based framework for collecting, screening, and locally archiving research-study metadata and downloadable assets from external data repositories. It brings together repository-specific extraction logic, a shared SQLite schema, and explicit qualitative-screening heuristics in one reproducible workflow.

At present, the project includes dedicated pipelines for **AUSSDA** and the **UK Data Service**. Each pipeline searches its source repository, retrieves study-level metadata, evaluates likely qualitative relevance, downloads accessible files where permitted, and stores the resulting snapshot in a shared local archive.

## Overview

Research repositories expose metadata in different formats, use different access models, and often require additional processing before the data can be reviewed consistently. QDArchive was built to address that problem by turning heterogeneous repository outputs into a single normalized local archive.

In practical terms, the project is intended to:

- collect metadata from multiple research-data repositories into one local archive
- normalize project, file, keyword, person-role, and license information into a shared schema
- identify likely qualitative studies using repository-specific heuristic scoring
- preserve file download outcomes explicitly at file level
- support repeatable archive refreshes through logs, summaries, and atomic snapshot replacement

## Supported Repositories

### AUSSDA

- Adapter: `aussda.py`
- Access mode: public API
- Download support: direct file download for unrestricted files with available file identifiers
- Restricted handling: restricted or access-request files are recorded with normalized failure statuses instead of being downloaded

### UK Data Service

- Adapter: `uk_data_service.py`
- Access mode: GraphQL API
- Download support: automated download for open-access studies with resolvable file URLs
- Restricted handling: controlled and safeguarded studies are kept in the archive, but their files are recorded as requiring authentication rather than being downloaded

## Core Features

- repository-specific ingestion pipelines for AUSSDA and UK Data Service
- a shared SQLite archive schema for normalized metadata persistence
- qualitative relevance screening using source-specific heuristic rules
- atomic project refresh and purge logic to keep database rows and download folders consistent
- structured logging for traceability and diagnostics
- per-run JSON summaries for reproducible batch reporting
- explicit file status tracking for successful, restricted, oversized, and failed download attempts
- repository-local download staging with rollback-safe backup handling

## Architecture

The repository is organized so that shared infrastructure remains separate from repository-specific metadata extraction and scoring logic.

### Shared infrastructure

- `pipeline_common.py` provides repository-agnostic helpers for timestamps, path creation, logger setup, streamed downloads, cleanup, summary writing, and snapshot orchestration.
- `database_manager.py` manages SQLite connection setup, schema initialization, transactions, and common database operations.
- `schema.sql` defines the shared persistence model used by all repository pipelines.

### Repository-specific adapters

- `aussda.py` implements search, metadata extraction, file handling, qualitative scoring, snapshot replacement, and summary generation for AUSSDA.
- `uk_data_service.py` implements GraphQL search and detail retrieval, access-aware download handling, qualitative scoring, snapshot replacement, and summary generation for the UK Data Service.

## Project Structure

```text
QDArchive/
├── aussda.py
├── uk_data_service.py
├── pipeline_common.py
├── database_manager.py
├── schema.sql
├── requirements.txt
├── 23041405-seeding.db
├── downloads/
│   ├── aussda/
│   │   ├── _tmp/
│   │   └── <project folders>/
│   └── uk-data-service/
│       ├── _tmp/
│       └── <study folders>/
├── logs/
│   ├── aussda.log
│   └── uk_data_service.log
└── summaries/
    ├── aussda_summary_<run_tag>.json
    └── uk-data-service_summary_<run_tag>.json
```

## File and Folder Roles

| Path | Purpose |
|---|---|
| `aussda.py` | AUSSDA ingestion pipeline |
| `uk_data_service.py` | UK Data Service ingestion pipeline |
| `pipeline_common.py` | Shared infrastructure for directories, logging, downloads, summaries, and snapshot replacement |
| `database_manager.py` | Shared SQLite manager and transaction helper |
| `schema.sql` | Common database schema |
| `requirements.txt` | Runtime dependency declaration |
| `23041405-seeding.db` | SQLite archive database produced by the pipelines |
| `downloads/` | Repository-specific download snapshots |
| `logs/` | Pipeline log files |
| `summaries/` | Machine-readable JSON run summaries |
| `downloads/<repo>/_tmp/` | Temporary staging and rollback backup directories |

## Database Design

QDArchive uses **SQLite** as its archive backend. The schema is centered on a canonical `projects` table, with related child tables linked through `project_id`.

### Main tables

- `projects` — core study/project metadata and local download provenance
- `files` — file-level records including filename, type, and normalized download status
- `keywords` — project keyword associations
- `person_role` — linked persons or organizations with a role label
- `licenses` — normalized license or access-license information
- `qualitative_scores_ukdata` — UK Data Service scoring breakdowns
- `qualitative_scores_aussda` — AUSSDA scoring breakdowns

### Data stored in `projects`

The `projects` table includes fields such as:

- repository provenance (`repository_id`, `repository_url`)
- project identification (`project_url`, `doi`)
- versioning (`version`, `download_version_folder`)
- descriptive metadata (`title`, `description`, `language`)
- ingestion timestamps (`upload_date`, `download_date`)
- local archive locations (`download_repository_folder`, `download_project_folder`)
- acquisition method (`download_method`)

## Pipeline Workflow

1. A repository script builds its local configuration.
2. Required directories are created for downloads, temporary staging, logs, and summaries.
3. The script initializes the shared SQLite database and applies the schema if needed.
4. The pipeline searches the external repository using repository-specific search calls.
5. Each returned study is fetched in detail and converted into a normalized metadata structure.
6. Repository-specific heuristic scoring determines whether the study is likely qualitative, possibly qualitative, uncertain, or unlikely.
7. Numeric-only or clearly non-target projects are skipped and, if already archived, purged from the local snapshot.
8. Qualifying projects move to file handling:
   - unrestricted or open files are staged for download
   - restricted or controlled files are recorded with normalized status values
   - hard failures stop the refresh for that project
9. Existing local project folders are backed up, database rows are replaced inside a transaction, and staged files are promoted to the final archive folder.
10. At the end of the run, the pipeline writes a JSON summary and a repository-specific log file.

## Qualitative Screening Logic

QDArchive does not use one universal scoring model across all sources. Instead, each repository adapter defines its own scoring rules based on the metadata fields that repository makes available.

### AUSSDA scoring dimensions

AUSSDA studies are scored using evidence drawn from:

- description text
- kind of data
- collection mode
- file categories
- file descriptions

The AUSSDA pipeline also excludes clearly non-target material when `kindOfData` is dominated by values such as numeric, geospatial, software, or interactive resource.

### UK Data Service scoring dimensions

UK Data Service studies are scored using evidence drawn from:

- kind of data
- data format
- documents
- abstract
- data collection methodology

The UK Data Service pipeline treats purely numeric `DataFormat` studies as non-target and removes them from the local archive when encountered.

## Download Behavior

QDArchive distinguishes between metadata archiving and file acquisition.

### AUSSDA

- Files are downloaded through direct API calls when they are unrestricted and have valid file identifiers.
- Restricted or access-request files are not downloaded and are recorded with a login-required status.
- Oversized or server-failing downloads are also mapped to explicit status values.

### UK Data Service

- Open studies can be downloaded through a GraphQL file-list query followed by a file-URL query for each file.
- Controlled and safeguarded studies are preserved as project metadata, but file download is not attempted.
- Unknown or unresolved access states are recorded as unsuccessful file outcomes.

### Normalized file statuses

The shared infrastructure uses the following status vocabulary:

- `SUCCEEDED`
- `FAILED_LOGIN_REQUIRED`
- `FAILED_TOO_LARGE`
- `FAILED_SERVER_UNRESPONSIVE`

## Logging and Summaries

Each pipeline produces both human-readable logs and machine-readable summaries.

### Logs

Repository log files are written to `logs/`:

- `logs/aussda.log`
- `logs/uk_data_service.log`

These logs capture progress messages, scoring decisions, warnings, failures, and refresh outcomes.

### Summaries

Each run writes a structured JSON summary to `summaries/`, including:

- repository metadata
- run start and finish timestamps
- search terms
- counts for received, analysed or processed, saved, skipped, and failed projects
- classification bucket summaries for numeric and non-numeric projects

## Installation

### Requirements

- Python 3.10+ recommended
- `requests>=2.31.0`

### Setup

```bash
git clone https://github.com/navalmor/QDArchive.git
cd QDArchive
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

The repository currently provides two standalone entry points.

### Run the AUSSDA pipeline

```bash
python aussda.py
```

### Run the UK Data Service pipeline

```bash
python uk_data_service.py
```

Both scripts create the required directories automatically and write to the shared SQLite database file `23041405-seeding.db` by default.

## Inputs and Outputs

### Inputs

Depending on the repository adapter, the system consumes:

- repository search terms
- repository item identifiers
- repository API responses
- access and license metadata
- downloadable file URLs where available
- local configuration values defined in each script

### Outputs

The main outputs are:

- a shared SQLite archive database
- repository-specific download folders
- repository-specific log files
- run-level JSON summaries

## Reproducibility Notes

The codebase is structured to support repeatable local snapshot refreshes:

- the schema is applied automatically at startup
- downloads are staged before promotion
- existing project folders are backed up before replacement
- database writes are wrapped in explicit transactions
- summary files are written atomically

This makes the repository suitable for iterative archive refresh workflows rather than one-off ad hoc scraping.

## Current Limitations

- The project currently includes adapters for only **two repositories**.
- There is **no shared top-level orchestrator**; pipelines are run one script at a time.
- Configuration is defined in source code rather than exposed through environment variables or CLI options.
- Restricted-file authentication workflows are **not** implemented.
- Qualitative identification is heuristic and rule-based rather than model-based.
- The shared infrastructure includes generic snapshot helpers, but the active repository scripts rely on aligned repository-local refresh helpers.
- The repository does not currently include a formal test suite, packaging configuration, or CI workflow in the shared files provided here.

## Extending the Project

To add a new repository adapter, the current architecture suggests the following pattern:

1. create a new repository-specific pipeline script
2. define repository configuration in a `PipelineConfig`
3. implement search and detail retrieval
4. map source metadata into the shared schema
5. define repository-specific qualitative scoring rules
6. reuse `pipeline_common.py` and `database_manager.py` for infrastructure and persistence
7. write repository-specific score rows into a dedicated scoring table if needed

## Recommended Citation / Academic Context

This project was prepared in a university academic context. The following information may be included for formal submission purposes:

- Author: Naval Mor
- Degree program: M.Sc. Data Science
- University: Friedrich-Alexander-Universität Erlangen-Nürnberg
- Academic supervisor: Prof. Dirk Riehle
- Project period: March 2026
- Submission date: 17 April 2026
- Version: 1.0

Suggested citation:

Mor, Naval. *QDArchive: Repository Ingestion and Qualitative Dataset Archiving Pipeline*. Friedrich-Alexander-Universität Erlangen-Nürnberg, 2026. Version 1.0.

## License

This repository is currently shared for academic and educational purposes. A formal software license has not yet been assigned.

## Author

**Author:** Naval Mor  
**Affiliation:** Student, Friedrich-Alexander-Universität Erlangen-Nürnberg

## Acknowledgements

QDArchive reflects a research-oriented software engineering approach to repository ingestion, combining source-specific metadata extraction with a normalized archive schema, explicit access handling, and reproducible local outputs.