CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_string TEXT,
    repository_id INTEGER,
    repository_url TEXT,
    project_url TEXT,
    version TEXT,
    title TEXT,
    description TEXT,
    language TEXT,
    doi TEXT,
    upload_date TEXT,
    download_date TEXT,
    download_repository_folder TEXT,
    download_project_folder TEXT,
    download_version_folder TEXT,
    download_method TEXT
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    file_name TEXT,
    file_type TEXT,
    status TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    keyword TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS person_role (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    name TEXT,
    role TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS licenses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    license TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS qualitative_scores_ukdata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    source_project_id TEXT,
    total_score INTEGER,
    label TEXT,
    kind_of_data_score INTEGER,
    data_format_score INTEGER,
    documents_score INTEGER,
    abstract_score INTEGER,
    methodology_score INTEGER,
    reasons TEXT,
    timestamp TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS qualitative_scores_aussda (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER,
    source_project_id TEXT,
    total_score INTEGER,
    label TEXT,
    description_score INTEGER,
    kind_of_data_score INTEGER,
    collection_mode_score INTEGER,
    file_categories_score INTEGER,
    file_description_score INTEGER,
    reasons TEXT,
    timestamp TEXT,
    FOREIGN KEY (project_id) REFERENCES projects(id)
);