-- Romshelf database schema (Milestone 1 - simplified)

-- DATs
CREATE TABLE IF NOT EXISTS dats (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    format TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_sha1 TEXT NOT NULL,
    file_size INTEGER,
    file_mtime INTEGER,
    category TEXT
);

-- Index for duplicate detection
CREATE UNIQUE INDEX IF NOT EXISTS idx_dats_sha1 ON dats(file_sha1);

CREATE TABLE IF NOT EXISTS dat_versions (
    id INTEGER PRIMARY KEY,
    dat_id INTEGER NOT NULL REFERENCES dats(id),
    version TEXT,
    date TEXT,
    loaded_at TEXT NOT NULL,
    entry_count INTEGER NOT NULL
);

-- Sets (groups of ROMs - games, applications, etc.)
CREATE TABLE IF NOT EXISTS sets (
    id INTEGER PRIMARY KEY,
    dat_version_id INTEGER NOT NULL REFERENCES dat_versions(id),
    name TEXT NOT NULL
);

-- Index for set lookups
CREATE INDEX IF NOT EXISTS idx_sets_dat_version ON sets(dat_version_id);

-- DAT entries (ROMs within sets)
CREATE TABLE IF NOT EXISTS dat_entries (
    id INTEGER PRIMARY KEY,
    dat_version_id INTEGER NOT NULL REFERENCES dat_versions(id),
    set_id INTEGER REFERENCES sets(id),
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    crc32 TEXT,
    md5 TEXT,
    sha1 TEXT
);

-- Files
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime INTEGER,
    crc32 TEXT,
    md5 TEXT,
    sha1 TEXT,
    scanned_at TEXT NOT NULL
);

-- Matches
CREATE TABLE IF NOT EXISTS matches (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL REFERENCES files(id),
    dat_entry_id INTEGER NOT NULL REFERENCES dat_entries(id),
    name_correct INTEGER NOT NULL,
    matched_at TEXT NOT NULL
);

-- Indexes for hash lookups
CREATE INDEX IF NOT EXISTS idx_dat_entries_crc32 ON dat_entries(crc32);
CREATE INDEX IF NOT EXISTS idx_dat_entries_sha1 ON dat_entries(sha1);
CREATE INDEX IF NOT EXISTS idx_files_crc32 ON files(crc32);
CREATE INDEX IF NOT EXISTS idx_files_sha1 ON files(sha1);

-- Index for rescan optimization (lookup by path)
CREATE INDEX IF NOT EXISTS idx_dats_file_path ON dats(file_path);

-- Directories (for lazy tree loading at scale)
CREATE TABLE IF NOT EXISTS directories (
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    parent_id INTEGER REFERENCES directories(id),
    file_count INTEGER NOT NULL DEFAULT 0,
    matched_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0
);

-- Index for fast child lookups
CREATE INDEX IF NOT EXISTS idx_directories_parent ON directories(parent_id);

-- Foreign key from files to directories
-- Note: directory_id is added via migration, not here

-- Checkpoints for resumable operations
CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY,
    job_type TEXT NOT NULL,
    source TEXT NOT NULL,
    last_token TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_checkpoints_job_source
    ON checkpoints(job_type, source);

-- Schema migrations for existing databases
-- Add mtime column to files if not exists
-- SQLite doesn't have IF NOT EXISTS for columns, but we handle this in code
