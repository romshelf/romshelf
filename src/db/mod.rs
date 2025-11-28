//! Database module - SQLite connection, schema, queries

use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;

/// Initialize the database, creating tables if they don't exist
pub fn init_db(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path)?;
    create_schema(&conn)?;
    Ok(conn)
}

/// Create the database schema
fn create_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(include_str!("schema.sql"))?;
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
