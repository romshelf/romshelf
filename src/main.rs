use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::time::Instant;

use bitshelf::dat;
use bitshelf::db;
use bitshelf::scan;
use bitshelf::verify;

#[derive(Parser)]
#[command(name = "bitshelf")]
#[command(about = "ROM collection manager - DAT-driven verification and organisation")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// DAT file operations
    Dat {
        #[command(subcommand)]
        command: DatCommands,
    },
    /// Scan ROM directories
    Scan {
        /// Directory to scan
        path: PathBuf,
    },
    /// Verify ROMs against loaded DATs
    Verify {
        /// Show detailed issues
        #[arg(long)]
        issues: bool,
    },
}

#[derive(Subcommand)]
enum DatCommands {
    /// Import a DAT file
    Import {
        /// Path to DAT file
        path: PathBuf,
    },
    /// List imported DATs
    List,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Get database path
    let db_path = get_db_path()?;
    let conn = db::init_db(&db_path)?;

    match cli.command {
        Commands::Dat { command } => match command {
            DatCommands::Import { path } => cmd_dat_import(&conn, &path),
            DatCommands::List => cmd_dat_list(&conn),
        },
        Commands::Scan { path } => cmd_scan(&conn, &path),
        Commands::Verify { issues } => cmd_verify(&conn, issues),
    }
}

fn get_db_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot find home directory"))?;
    let config_dir = home.join(".bitshelf");
    std::fs::create_dir_all(&config_dir)?;
    Ok(config_dir.join("bitshelf.db"))
}

fn cmd_dat_import(conn: &rusqlite::Connection, path: &PathBuf) -> Result<()> {
    eprintln!("Importing DAT from {}...", path.display());

    let parsed = dat::parse_dat(path)?;

    // Insert DAT record
    conn.execute(
        "INSERT INTO dats (name, format, file_path) VALUES (?1, ?2, ?3)",
        [&parsed.name, "logiqx", &path.to_string_lossy().to_string()],
    )?;
    let dat_id = conn.last_insert_rowid();

    // Insert version record
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO dat_versions (dat_id, version, loaded_at, entry_count) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            dat_id,
            parsed.version,
            now,
            parsed.entries.len() as i64
        ],
    )?;
    let version_id = conn.last_insert_rowid();

    // Insert entries
    let mut stmt = conn.prepare(
        "INSERT INTO dat_entries (dat_version_id, name, size, crc32, md5, sha1) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )?;

    for entry in &parsed.entries {
        stmt.execute(rusqlite::params![
            version_id,
            entry.rom_name,
            entry.size as i64,
            entry.crc32,
            entry.md5,
            entry.sha1
        ])?;
    }

    println!("Imported: {}", parsed.name);
    if let Some(version) = &parsed.version {
        println!("  Version: {}", version);
    }
    println!("  Entries: {}", parsed.entries.len());

    Ok(())
}

fn cmd_dat_list(conn: &rusqlite::Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT d.id, d.name, dv.version, dv.entry_count, dv.loaded_at
         FROM dats d
         JOIN dat_versions dv ON d.id = dv.dat_id
         ORDER BY dv.loaded_at DESC",
    )?;

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, i64>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;

    let mut count = 0;
    for row in rows {
        let (id, name, version, entry_count, loaded_at) = row?;
        println!("[{}] {}", id, name);
        if let Some(v) = version {
            println!("    Version: {}", v);
        }
        println!("    Entries: {}", entry_count);
        println!("    Loaded: {}", loaded_at);
        println!();
        count += 1;
    }

    if count == 0 {
        println!("No DATs imported yet. Use `bitshelf dat import <path>` to import one.");
    }

    Ok(())
}

fn cmd_scan(conn: &rusqlite::Connection, path: &PathBuf) -> Result<()> {
    eprintln!("Scanning {}...", path.display());
    let start = Instant::now();

    let files = scan::scan_directory(path)?;
    let elapsed = start.elapsed();

    // Store scanned files in database
    let now = chrono::Utc::now().to_rfc3339();
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO files (path, filename, size, crc32, md5, sha1, scanned_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;

    for file in &files {
        stmt.execute(rusqlite::params![
            file.path.to_string_lossy().to_string(),
            file.filename,
            file.size as i64,
            file.crc32,
            file.md5,
            file.sha1,
            now
        ])?;
    }

    let rate = if elapsed.as_secs_f32() > 0.0 {
        files.len() as f32 / elapsed.as_secs_f32()
    } else {
        files.len() as f32
    };

    println!("Scanning... {} files", files.len());
    println!("  Hashing at {:.0} files/sec", rate);
    println!("  Complete in {:.1}s", elapsed.as_secs_f32());

    Ok(())
}

fn cmd_verify(conn: &rusqlite::Connection, show_issues: bool) -> Result<()> {
    // Load files from database
    let mut file_stmt = conn.prepare("SELECT path, filename, size, crc32, md5, sha1 FROM files")?;
    let files: Vec<scan::ScannedFile> = file_stmt
        .query_map([], |row| {
            Ok(scan::ScannedFile {
                path: PathBuf::from(row.get::<_, String>(0)?),
                filename: row.get(1)?,
                size: row.get::<_, i64>(2)? as u64,
                crc32: row.get(3)?,
                md5: row.get(4)?,
                sha1: row.get(5)?,
            })
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Load DAT entries from database, grouped by DAT
    let mut entry_stmt = conn.prepare(
        "SELECT de.name, de.size, de.crc32, de.md5, de.sha1, d.name as dat_name
         FROM dat_entries de
         JOIN dat_versions dv ON de.dat_version_id = dv.id
         JOIN dats d ON dv.dat_id = d.id",
    )?;
    let all_entries: Vec<(dat::DatEntry, String)> = entry_stmt
        .query_map([], |row| {
            Ok((
                dat::DatEntry {
                    name: row.get::<_, String>(0)?.clone(),
                    rom_name: row.get(0)?,
                    size: row.get::<_, i64>(1)? as u64,
                    crc32: row.get(2)?,
                    md5: row.get(3)?,
                    sha1: row.get(4)?,
                },
                row.get::<_, String>(5)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if all_entries.is_empty() {
        println!("No DATs loaded. Use `bitshelf dat import <path>` first.");
        return Ok(());
    }

    if files.is_empty() {
        println!("No files scanned. Use `bitshelf scan <path>` first.");
        return Ok(());
    }

    // Group entries by DAT name
    let mut entries_by_dat: std::collections::HashMap<String, Vec<dat::DatEntry>> =
        std::collections::HashMap::new();
    for (entry, dat_name) in all_entries {
        entries_by_dat
            .entry(dat_name)
            .or_default()
            .push(entry);
    }

    // Track all misnamed and unmatched for detailed output
    let mut all_misnamed = Vec::new();
    let mut all_unmatched = files.clone();

    // Verify per DAT
    for (dat_name, entries) in &entries_by_dat {
        let result = verify::verify(&files, entries);

        let total = entries.len();
        let verified_count = result.verified.len();
        let misnamed_count = result.misnamed.len();
        let missing_count = result.missing.len();

        // Remove matched files from unmatched list
        for m in &result.verified {
            all_unmatched.retain(|f| f.path != m.file.path);
        }
        for m in &result.misnamed {
            all_unmatched.retain(|f| f.path != m.file.path);
            all_misnamed.push(m.clone());
        }

        let verified_pct = if total > 0 {
            (verified_count as f32 / total as f32) * 100.0
        } else {
            0.0
        };

        println!("{}", dat_name);
        println!("  Verified:   {:>6} ({:.1}%)", verified_count, verified_pct);
        println!("  Misnamed:   {:>6}", misnamed_count);
        println!("  Missing:    {:>6}", missing_count);
        println!();
    }

    // Summary of unmatched files (not in any DAT)
    if !all_unmatched.is_empty() {
        println!("Unmatched files (not in any DAT): {}", all_unmatched.len());
    }

    if show_issues {
        if !all_misnamed.is_empty() {
            println!("\nMISNAMED:");
            for m in &all_misnamed {
                println!("  {} -> {}", m.file.filename, m.entry.rom_name);
            }
        }

        if !all_unmatched.is_empty() {
            println!("\nUNMATCHED:");
            for f in &all_unmatched {
                println!("  {} (no DAT match)", f.filename);
            }
        }
    }

    Ok(())
}
