//! Database module - SQLite connection, schema, queries

use anyhow::{Result, anyhow};
use chrono::Utc;
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use std::path::{Path, PathBuf};

/// Statistics about the collection
#[derive(Debug, Serialize)]
pub struct CollectionStats {
    pub dat_count: i64,
    pub entry_count: i64,
    pub scanned_files: i64,
    pub matched_files: i64,
    pub total_bytes_scanned: i64,
}

/// Summary of a loaded DAT file
#[derive(Debug, Serialize, Clone)]
pub struct DatSummary {
    pub id: i64,
    pub name: String,
    pub category: Option<String>,
    pub version: Option<String>,
    pub entry_count: i64,
    pub set_count: i64,
}

/// A node in the DAT tree hierarchy
#[derive(Debug, Serialize, Clone)]
pub struct DatTreeNode {
    pub name: String,
    pub children: Vec<DatTreeNode>,
    pub dats: Vec<DatSummary>,
}

/// Summary of a scanned file
#[derive(Debug, Serialize, Clone)]
pub struct FileSummary {
    pub id: i64,
    pub path: String,
    pub filename: String,
    pub size: i64,
    pub sha1: Option<String>,
    pub matched: bool,
    pub match_name: Option<String>,
}

/// A node in the file tree hierarchy (legacy - for small collections)
#[derive(Debug, Serialize, Clone)]
pub struct FileTreeNode {
    pub name: String,
    pub children: Vec<FileTreeNode>,
    pub files: Vec<FileSummary>,
    pub total_files: i64,
    pub matched_files: i64,
}

/// Directory summary for lazy tree loading
#[derive(Debug, Serialize, Clone)]
pub struct DirectorySummary {
    pub id: i64,
    pub path: String,
    pub name: String,
    pub parent_id: Option<i64>,
    pub file_count: i64,
    pub matched_count: i64,
    pub total_size: i64,
    pub child_count: i64,
}

/// Checkpoint information for resumable jobs
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub job_type: String,
    pub source: String,
    pub last_token: String,
    pub updated_at: String,
}

/// Get the default database path (~/.romshelf/romshelf.db)
pub fn default_db_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("Cannot find home directory"))?;
    let dir = home.join(".romshelf");
    Ok(dir.join("romshelf.db"))
}

/// Open the database at the default location, creating if needed
pub fn open_db() -> Result<Connection> {
    let path = default_db_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    init_db(&path)
}

/// Initialize the database, creating tables if they don't exist
pub fn init_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    create_schema(&conn)?;
    migrate_schema(&conn)?;
    Ok(conn)
}

/// Create the database schema
fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(include_str!("schema.sql"))?;
    Ok(())
}

/// Apply schema migrations for existing databases
fn migrate_schema(conn: &Connection) -> Result<()> {
    // Add mtime column to files if not exists
    if !column_exists(conn, "files", "mtime")? {
        conn.execute("ALTER TABLE files ADD COLUMN mtime INTEGER", [])?;
    }

    // Add file_size column to dats if not exists
    if !column_exists(conn, "dats", "file_size")? {
        conn.execute("ALTER TABLE dats ADD COLUMN file_size INTEGER", [])?;
    }

    // Add file_mtime column to dats if not exists
    if !column_exists(conn, "dats", "file_mtime")? {
        conn.execute("ALTER TABLE dats ADD COLUMN file_mtime INTEGER", [])?;
    }

    // Add directory_id column to files if not exists
    if !column_exists(conn, "files", "directory_id")? {
        conn.execute(
            "ALTER TABLE files ADD COLUMN directory_id INTEGER REFERENCES directories(id)",
            [],
        )?;
    }

    Ok(())
}

/// Check if a column exists in a table
fn column_exists(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let sql = format!("PRAGMA table_info({})", table);
    let mut stmt = conn.prepare(&sql)?;
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(1))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(columns.contains(&column.to_string()))
}

/// Get collection statistics
pub fn get_collection_stats(conn: &Connection) -> Result<CollectionStats> {
    let dat_count: i64 = conn.query_row("SELECT COUNT(*) FROM dats", [], |row| row.get(0))?;

    let entry_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM dat_entries", [], |row| row.get(0))?;

    let scanned_files: i64 = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;

    let matched_files: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT f.id) FROM files f
         INNER JOIN dat_entries e ON f.sha1 = e.sha1",
        [],
        |row| row.get(0),
    )?;

    let total_bytes_scanned: i64 =
        conn.query_row("SELECT COALESCE(SUM(size), 0) FROM files", [], |row| {
            row.get(0)
        })?;

    Ok(CollectionStats {
        dat_count,
        entry_count,
        scanned_files,
        matched_files,
        total_bytes_scanned,
    })
}

/// List all loaded DATs with summary info
pub fn list_dats(conn: &Connection) -> Result<Vec<DatSummary>> {
    let mut stmt = conn.prepare(
        "SELECT d.id, d.name, d.category,
                (SELECT dv.version FROM dat_versions dv WHERE dv.dat_id = d.id ORDER BY dv.loaded_at DESC LIMIT 1) as version,
                (SELECT COUNT(*) FROM dat_entries de
                 INNER JOIN dat_versions dv ON de.dat_version_id = dv.id
                 WHERE dv.dat_id = d.id) as entry_count,
                (SELECT COUNT(*) FROM sets s
                 INNER JOIN dat_versions dv ON s.dat_version_id = dv.id
                 WHERE dv.dat_id = d.id) as set_count
         FROM dats d
         ORDER BY d.category, d.name",
    )?;

    let dats = stmt
        .query_map([], |row| {
            Ok(DatSummary {
                id: row.get(0)?,
                name: row.get(1)?,
                category: row.get(2)?,
                version: row.get(3)?,
                entry_count: row.get(4)?,
                set_count: row.get(5)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(dats)
}

/// Build a tree structure from DATs based on their category paths
pub fn get_dat_tree(conn: &Connection) -> Result<DatTreeNode> {
    let dats = list_dats(conn)?;

    let mut root = DatTreeNode {
        name: "Root".to_string(),
        children: Vec::new(),
        dats: Vec::new(),
    };

    for dat in dats {
        let path = dat.category.clone().unwrap_or_default();
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

        insert_into_tree(&mut root, &parts, dat);
    }

    // Sort children alphabetically at each level
    sort_tree(&mut root);

    Ok(root)
}

fn insert_into_tree(node: &mut DatTreeNode, path: &[&str], dat: DatSummary) {
    if path.is_empty() {
        node.dats.push(dat);
        return;
    }

    let segment = path[0];
    let remaining = &path[1..];

    // Find or create child node
    let child = node.children.iter_mut().find(|c| c.name == segment);

    match child {
        Some(child) => insert_into_tree(child, remaining, dat),
        None => {
            let mut new_child = DatTreeNode {
                name: segment.to_string(),
                children: Vec::new(),
                dats: Vec::new(),
            };
            insert_into_tree(&mut new_child, remaining, dat);
            node.children.push(new_child);
        }
    }
}

fn sort_tree(node: &mut DatTreeNode) {
    node.children.sort_by(|a, b| a.name.cmp(&b.name));
    node.dats.sort_by(|a, b| a.name.cmp(&b.name));
    for child in &mut node.children {
        sort_tree(child);
    }
}

/// List scanned files with match status
pub fn list_files(conn: &Connection, limit: i64, offset: i64) -> Result<Vec<FileSummary>> {
    let mut stmt = conn.prepare(
        "SELECT f.id, f.path, f.filename, f.size, f.sha1,
                EXISTS(SELECT 1 FROM dat_entries e WHERE e.sha1 = f.sha1) as matched,
                (SELECT e.name FROM dat_entries e WHERE e.sha1 = f.sha1 LIMIT 1) as match_name
         FROM files f
         ORDER BY f.filename
         LIMIT ?1 OFFSET ?2",
    )?;

    let files = stmt
        .query_map([limit, offset], |row| {
            Ok(FileSummary {
                id: row.get(0)?,
                path: row.get(1)?,
                filename: row.get(2)?,
                size: row.get(3)?,
                sha1: row.get(4)?,
                matched: row.get(5)?,
                match_name: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(files)
}

/// Get all files as a tree structure based on filesystem paths
pub fn get_file_tree(conn: &Connection) -> Result<FileTreeNode> {
    let mut stmt = conn.prepare(
        "SELECT f.id, f.path, f.filename, f.size, f.sha1,
                EXISTS(SELECT 1 FROM dat_entries e WHERE e.sha1 = f.sha1) as matched,
                (SELECT e.name FROM dat_entries e WHERE e.sha1 = f.sha1 LIMIT 1) as match_name
         FROM files f
         ORDER BY f.path",
    )?;

    let files: Vec<FileSummary> = stmt
        .query_map([], |row| {
            Ok(FileSummary {
                id: row.get(0)?,
                path: row.get(1)?,
                filename: row.get(2)?,
                size: row.get(3)?,
                sha1: row.get(4)?,
                matched: row.get(5)?,
                match_name: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let mut root = FileTreeNode {
        name: "Root".to_string(),
        children: Vec::new(),
        files: Vec::new(),
        total_files: 0,
        matched_files: 0,
    };

    for file in files {
        // Use the directory part of the path
        let path_obj = std::path::Path::new(&file.path);
        let dir_path = path_obj
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        let parts: Vec<&str> = dir_path.split('/').filter(|s| !s.is_empty()).collect();

        insert_file_into_tree(&mut root, &parts, file);
    }

    // Sort and compute totals
    compute_file_tree_stats(&mut root);

    Ok(root)
}

fn insert_file_into_tree(node: &mut FileTreeNode, path: &[&str], file: FileSummary) {
    if path.is_empty() {
        node.files.push(file);
        return;
    }

    let segment = path[0];
    let remaining = &path[1..];

    // Find or create child node
    let child = node.children.iter_mut().find(|c| c.name == segment);

    match child {
        Some(child) => insert_file_into_tree(child, remaining, file),
        None => {
            let mut new_child = FileTreeNode {
                name: segment.to_string(),
                children: Vec::new(),
                files: Vec::new(),
                total_files: 0,
                matched_files: 0,
            };
            insert_file_into_tree(&mut new_child, remaining, file);
            node.children.push(new_child);
        }
    }
}

fn compute_file_tree_stats(node: &mut FileTreeNode) {
    // Sort children and files
    node.children.sort_by(|a, b| a.name.cmp(&b.name));
    node.files.sort_by(|a, b| a.filename.cmp(&b.filename));

    // Compute stats for children first
    for child in &mut node.children {
        compute_file_tree_stats(child);
    }

    // Sum up totals
    node.total_files =
        node.files.len() as i64 + node.children.iter().map(|c| c.total_files).sum::<i64>();
    node.matched_files = node.files.iter().filter(|f| f.matched).count() as i64
        + node.children.iter().map(|c| c.matched_files).sum::<i64>();
}

// ============================================================================
// Directory-based lazy loading (scales to millions of files)
// ============================================================================

/// Get or create a directory entry, returning its ID
pub fn get_or_create_directory(conn: &Connection, path: &str) -> Result<i64> {
    // Check if directory exists
    let existing: Option<i64> = conn
        .query_row(
            "SELECT id FROM directories WHERE path = ?1",
            [path],
            |row| row.get(0),
        )
        .ok();

    if let Some(id) = existing {
        return Ok(id);
    }

    // Parse path components
    let path_obj = Path::new(path);
    let name = path_obj
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| path.to_string());

    // Get or create parent
    let parent_id = if let Some(parent) = path_obj.parent() {
        let parent_str = parent.to_string_lossy().to_string();
        if parent_str.is_empty() || parent_str == "/" {
            None
        } else {
            Some(get_or_create_directory(conn, &parent_str)?)
        }
    } else {
        None
    };

    // Insert the directory
    conn.execute(
        "INSERT INTO directories (path, name, parent_id) VALUES (?1, ?2, ?3)",
        rusqlite::params![path, name, parent_id],
    )?;

    Ok(conn.last_insert_rowid())
}

/// Update directory stats after scanning
pub fn update_directory_stats(
    conn: &Connection,
    dir_id: i64,
    size: i64,
    matched: bool,
) -> Result<()> {
    conn.execute(
        "UPDATE directories SET
            file_count = file_count + 1,
            matched_count = matched_count + CASE WHEN ?2 THEN 1 ELSE 0 END,
            total_size = total_size + ?1
         WHERE id = ?3",
        rusqlite::params![size, matched, dir_id],
    )?;

    // Propagate to parent
    let parent_id: Option<i64> = conn
        .query_row(
            "SELECT parent_id FROM directories WHERE id = ?1",
            [dir_id],
            |row| row.get(0),
        )
        .ok()
        .flatten();

    if let Some(pid) = parent_id {
        update_directory_stats(conn, pid, size, matched)?;
    }

    Ok(())
}

/// Update or insert a checkpoint for resumable operations
pub fn upsert_checkpoint(
    conn: &Connection,
    job_type: &str,
    source: &str,
    last_token: &str,
) -> Result<()> {
    let now = Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO checkpoints (job_type, source, last_token, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(job_type, source)
         DO UPDATE SET last_token = excluded.last_token,
                       updated_at = excluded.updated_at",
        rusqlite::params![job_type, source, last_token, now],
    )?;
    Ok(())
}

/// Clear a checkpoint when a job completes successfully
pub fn delete_checkpoint(conn: &Connection, job_type: &str, source: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM checkpoints WHERE job_type = ?1 AND source = ?2",
        rusqlite::params![job_type, source],
    )?;
    Ok(())
}

/// Retrieve checkpoint state if one exists
pub fn get_checkpoint(
    conn: &Connection,
    job_type: &str,
    source: &str,
) -> Result<Option<Checkpoint>> {
    let mut stmt = conn.prepare(
        "SELECT job_type, source, last_token, updated_at
         FROM checkpoints
         WHERE job_type = ?1 AND source = ?2",
    )?;
    let checkpoint = stmt
        .query_row(rusqlite::params![job_type, source], |row| {
            Ok(Checkpoint {
                job_type: row.get(0)?,
                source: row.get(1)?,
                last_token: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })
        .optional()?;
    Ok(checkpoint)
}

/// Get root directories (top-level scan roots)
pub fn get_root_directories(conn: &Connection) -> Result<Vec<DirectorySummary>> {
    let mut stmt = conn.prepare(
        "SELECT d.id, d.path, d.name, d.parent_id, d.file_count, d.matched_count, d.total_size,
                (SELECT COUNT(*) FROM directories c WHERE c.parent_id = d.id) as child_count
         FROM directories d
         WHERE d.parent_id IS NULL
         ORDER BY d.name",
    )?;

    let dirs = stmt
        .query_map([], |row| {
            Ok(DirectorySummary {
                id: row.get(0)?,
                path: row.get(1)?,
                name: row.get(2)?,
                parent_id: row.get(3)?,
                file_count: row.get(4)?,
                matched_count: row.get(5)?,
                total_size: row.get(6)?,
                child_count: row.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(dirs)
}

/// Get child directories of a parent
pub fn get_child_directories(conn: &Connection, parent_id: i64) -> Result<Vec<DirectorySummary>> {
    let mut stmt = conn.prepare(
        "SELECT d.id, d.path, d.name, d.parent_id, d.file_count, d.matched_count, d.total_size,
                (SELECT COUNT(*) FROM directories c WHERE c.parent_id = d.id) as child_count
         FROM directories d
         WHERE d.parent_id = ?1
         ORDER BY d.name",
    )?;

    let dirs = stmt
        .query_map([parent_id], |row| {
            Ok(DirectorySummary {
                id: row.get(0)?,
                path: row.get(1)?,
                name: row.get(2)?,
                parent_id: row.get(3)?,
                file_count: row.get(4)?,
                matched_count: row.get(5)?,
                total_size: row.get(6)?,
                child_count: row.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(dirs)
}

/// Get files directly in a directory (not recursive)
pub fn get_files_in_directory(conn: &Connection, dir_id: i64) -> Result<Vec<FileSummary>> {
    let mut stmt = conn.prepare(
        "SELECT f.id, f.path, f.filename, f.size, f.sha1,
                EXISTS(SELECT 1 FROM dat_entries e WHERE e.sha1 = f.sha1) as matched,
                (SELECT e.name FROM dat_entries e WHERE e.sha1 = f.sha1 LIMIT 1) as match_name
         FROM files f
         WHERE f.directory_id = ?1
         ORDER BY f.filename",
    )?;

    let files = stmt
        .query_map([dir_id], |row| {
            Ok(FileSummary {
                id: row.get(0)?,
                path: row.get(1)?,
                filename: row.get(2)?,
                size: row.get(3)?,
                sha1: row.get(4)?,
                matched: row.get(5)?,
                match_name: row.get(6)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;

    Ok(files)
}

/// Reset all directory stats (call before re-scanning)
pub fn reset_directory_stats(conn: &Connection) -> Result<()> {
    conn.execute(
        "UPDATE directories SET file_count = 0, matched_count = 0, total_size = 0",
        [],
    )?;
    Ok(())
}

/// Recompute all directory stats from files (call after bulk file changes)
/// This is more efficient than updating stats per-file for bulk operations
pub fn recompute_directory_stats(conn: &Connection) -> Result<()> {
    // Step 1: Compute direct file stats for each directory
    conn.execute(
        "UPDATE directories SET
            file_count = (SELECT COUNT(*) FROM files f WHERE f.directory_id = directories.id),
            matched_count = (SELECT COUNT(*) FROM files f WHERE f.directory_id = directories.id
                            AND EXISTS(SELECT 1 FROM dat_entries e WHERE e.sha1 = f.sha1)),
            total_size = (SELECT COALESCE(SUM(size), 0) FROM files f WHERE f.directory_id = directories.id)",
        [],
    )?;

    // Step 2: Use recursive CTE to roll up stats from children to parents
    // This propagates direct stats upward through the tree hierarchy
    conn.execute_batch(
        "
        DROP TABLE IF EXISTS temp_dir_rollup;
        CREATE TEMP TABLE temp_dir_rollup AS
        WITH RECURSIVE rollup(id, file_count, matched_count, total_size) AS (
            -- Base: start with direct file stats from each directory
            SELECT id, file_count, matched_count, total_size FROM directories

            UNION ALL

            -- Recursive: propagate each directory's stats to its parent
            SELECT d.parent_id, r.file_count, r.matched_count, r.total_size
            FROM rollup r
            JOIN directories d ON d.id = r.id
            WHERE d.parent_id IS NOT NULL
        )
        -- Sum all contributions for each directory (direct + all descendants)
        SELECT id, SUM(file_count) as file_count, SUM(matched_count) as matched_count, SUM(total_size) as total_size
        FROM rollup
        GROUP BY id;

        UPDATE directories SET
            file_count = (SELECT file_count FROM temp_dir_rollup WHERE temp_dir_rollup.id = directories.id),
            matched_count = (SELECT matched_count FROM temp_dir_rollup WHERE temp_dir_rollup.id = directories.id),
            total_size = (SELECT total_size FROM temp_dir_rollup WHERE temp_dir_rollup.id = directories.id);

        DROP TABLE temp_dir_rollup;
        "
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_db_creates_tables() {
        let conn = Connection::open_in_memory().unwrap();
        create_schema(&conn).unwrap();

        // Verify tables exist
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        assert!(tables.contains(&"dats".to_string()));
        assert!(tables.contains(&"dat_versions".to_string()));
        assert!(tables.contains(&"sets".to_string()));
        assert!(tables.contains(&"dat_entries".to_string()));
        assert!(tables.contains(&"files".to_string()));
        assert!(tables.contains(&"matches".to_string()));
    }
}
