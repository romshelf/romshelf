-- Bitshelf database schema (Milestone 1 - simplified)

-- DATs
CREATE TABLE IF NOT EXISTS dats (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    format TEXT NOT NULL,
    file_path TEXT NOT NULL,
    file_sha1 TEXT NOT NULL
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

-- DAT entries (simplified - no parsed fields yet)
CREATE TABLE IF NOT EXISTS dat_entries (
    id INTEGER PRIMARY KEY,
    dat_version_id INTEGER NOT NULL REFERENCES dat_versions(id),
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
