//! Database module - SQLite connection, schema, queries

use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;

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
