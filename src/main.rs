use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use bitshelf::dat;
use bitshelf::db;
use bitshelf::scan::{self, ScanProgress};
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

        /// Number of worker threads (default: all cores)
        #[arg(long, short = 't')]
        threads: Option<usize>,
    },
    /// Verify ROMs against loaded DATs
    Verify {
        /// Show detailed issues
        #[arg(long)]
        issues: bool,
    },
    /// Organise ROMs into a structured directory
    Organise {
        /// Target directory for organised ROMs
        #[arg(long)]
        target: PathBuf,

        /// Dry run - show what would be done without making changes
        #[arg(long)]
        dry_run: bool,

        /// Copy files instead of moving them
        #[arg(long)]
        copy: bool,
    },
}

#[derive(Subcommand)]
enum DatCommands {
    /// Import a DAT file
    Import {
        /// Path to DAT file
        path: PathBuf,
    },
    /// Import all DAT files from a directory (recursive)
    ImportDir {
        /// Directory containing DAT files
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
            DatCommands::ImportDir { path } => cmd_dat_import_dir(&conn, &path),
            DatCommands::List => cmd_dat_list(&conn),
        },
        Commands::Scan { path, threads } => cmd_scan(&conn, &path, threads),
        Commands::Verify { issues } => cmd_verify(&conn, issues),
        Commands::Organise { target, dry_run, copy } => cmd_organise(&conn, &target, dry_run, copy),
    }
}

fn get_db_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot find home directory"))?;
    let config_dir = home.join(".bitshelf");
    std::fs::create_dir_all(&config_dir)?;
    Ok(config_dir.join("bitshelf.db"))
}

/// Import result for tracking duplicates
enum ImportResult {
    Imported { name: String, version: Option<String>, entries: usize },
    Duplicate { name: String },
    Failed { path: PathBuf, error: String },
}

fn cmd_dat_import(conn: &rusqlite::Connection, path: &PathBuf) -> Result<()> {
    match import_single_dat(conn, path)? {
        ImportResult::Imported { name, version, entries } => {
            println!("Imported: {}", name);
            if let Some(v) = version {
                println!("  Version: {}", v);
            }
            println!("  Entries: {}", entries);
        }
        ImportResult::Duplicate { name } => {
            println!("Skipped (duplicate): {}", name);
        }
        ImportResult::Failed { path, error } => {
            eprintln!("Failed to import {}: {}", path.display(), error);
        }
    }
    Ok(())
}

fn cmd_dat_import_dir(conn: &rusqlite::Connection, path: &PathBuf) -> Result<()> {
    use walkdir::WalkDir;

    eprintln!("Scanning for DAT files in {}...", path.display());

    let dat_files: Vec<PathBuf> = WalkDir::new(path)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .map(|ext| ext.to_ascii_lowercase() == "dat")
                    .unwrap_or(false)
        })
        .map(|e| e.path().to_path_buf())
        .collect();

    eprintln!("Found {} DAT files", dat_files.len());

    let mut imported = 0;
    let mut duplicates = 0;
    let mut failed = 0;

    for (i, dat_path) in dat_files.iter().enumerate() {
        eprint!("\r  Processing: {}/{}", i + 1, dat_files.len());

        match import_single_dat(conn, dat_path) {
            Ok(ImportResult::Imported { .. }) => imported += 1,
            Ok(ImportResult::Duplicate { .. }) => duplicates += 1,
            Ok(ImportResult::Failed { path, error }) => {
                eprintln!("\n  Failed: {} - {}", path.display(), error);
                failed += 1;
            }
            Err(e) => {
                eprintln!("\n  Error: {} - {}", dat_path.display(), e);
                failed += 1;
            }
        }
    }

    eprintln!(); // New line after progress
    println!("\nImport complete:");
    println!("  Imported:   {:>6}", imported);
    println!("  Duplicates: {:>6}", duplicates);
    if failed > 0 {
        println!("  Failed:     {:>6}", failed);
    }

    Ok(())
}

fn import_single_dat(conn: &rusqlite::Connection, path: &PathBuf) -> Result<ImportResult> {
    // Hash the DAT file first for duplicate detection
    let file_sha1 = match dat::hash_dat_file(path) {
        Ok(hash) => hash,
        Err(e) => {
            return Ok(ImportResult::Failed {
                path: path.clone(),
                error: e.to_string(),
            });
        }
    };

    // Check if this exact DAT already exists
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM dats WHERE file_sha1 = ?1",
            [&file_sha1],
            |_| Ok(true),
        )
        .unwrap_or(false);

    if exists {
        // Get the name for reporting
        let name: String = conn
            .query_row(
                "SELECT name FROM dats WHERE file_sha1 = ?1",
                [&file_sha1],
                |row| row.get(0),
            )
            .unwrap_or_else(|_| "Unknown".to_string());
        return Ok(ImportResult::Duplicate { name });
    }

    // Parse the DAT
    let parsed = match dat::parse_dat(path) {
        Ok(p) => p,
        Err(e) => {
            return Ok(ImportResult::Failed {
                path: path.clone(),
                error: e.to_string(),
            });
        }
    };

    // Insert DAT record
    conn.execute(
        "INSERT INTO dats (name, format, file_path, file_sha1) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![
            &parsed.name,
            "logiqx",
            &path.to_string_lossy().to_string(),
            &file_sha1
        ],
    )?;
    let dat_id = conn.last_insert_rowid();

    // Insert version record
    let now = chrono::Utc::now().to_rfc3339();
    conn.execute(
        "INSERT INTO dat_versions (dat_id, version, loaded_at, entry_count) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![dat_id, parsed.version, now, parsed.entries.len() as i64],
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

    Ok(ImportResult::Imported {
        name: parsed.name,
        version: parsed.version,
        entries: parsed.entries.len(),
    })
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

fn cmd_scan(conn: &rusqlite::Connection, path: &PathBuf, threads: Option<usize>) -> Result<()> {
    let thread_count = threads.unwrap_or_else(num_cpus::get).max(1);

    eprintln!("Scanning {} with {} threads...", path.display(), thread_count);

    let progress = Arc::new(ScanProgress::new());
    let progress_display = Arc::clone(&progress);

    // Progress display thread
    let display_handle = thread::spawn(move || {
        loop {
            let discovered = progress_display.discovered.load(Ordering::Relaxed);
            let processed = progress_display.processed.load(Ordering::Relaxed);
            let rate = progress_display.files_per_sec();

            eprint!(
                "\r  Discovered: {:>6}  Processed: {:>6}  Speed: {:>6.0} files/sec  ",
                discovered, processed, rate
            );

            // Check if we're done (processed >= discovered and discovered > 0)
            if processed >= discovered && discovered > 0 {
                // Give a small grace period for final items
                thread::sleep(Duration::from_millis(100));
                let final_processed = progress_display.processed.load(Ordering::Relaxed);
                let final_discovered = progress_display.discovered.load(Ordering::Relaxed);
                if final_processed >= final_discovered {
                    break;
                }
            }

            thread::sleep(Duration::from_millis(100));
        }
        eprintln!(); // New line after progress
    });

    // Run the scan
    let result = scan::scan_directory_parallel(path, thread_count, progress)?;

    // Wait for progress display to finish
    let _ = display_handle.join();

    // Store scanned files in database
    let now = chrono::Utc::now().to_rfc3339();
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO files (path, filename, size, crc32, md5, sha1, scanned_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;

    for file in &result.files {
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

    // Print summary
    let rate = if result.duration.as_secs_f32() > 0.0 {
        result.files.len() as f32 / result.duration.as_secs_f32()
    } else {
        result.files.len() as f32
    };

    println!("\nScan complete in {:.1}s", result.duration.as_secs_f32());
    println!("  Files:      {:>6}", result.files.len());

    let total_archives = result.zip_archives + result.sevenz_archives;
    if total_archives > 0 {
        println!(
            "  Archives:   {:>6} ({} ZIP, {} 7z)",
            total_archives, result.zip_archives, result.sevenz_archives
        );
    }

    if !result.skipped.is_empty() {
        println!("  Skipped:    {:>6}", result.skipped.len());
    }

    println!("  Speed:      {:>6.0} files/sec", rate);

    // Show skipped files if any
    if !result.skipped.is_empty() {
        println!("\nSkipped files:");
        for skip in &result.skipped {
            println!("  {} ({})", skip.path.display(), skip.reason);
        }
    }

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

fn cmd_organise(
    conn: &rusqlite::Connection,
    target: &PathBuf,
    dry_run: bool,
    copy: bool,
) -> Result<()> {
    // Load all matched files with their DAT info
    let mut stmt = conn.prepare(
        "SELECT f.path, f.filename, de.name as rom_name, d.name as dat_name
         FROM files f
         JOIN dat_entries de ON f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
         JOIN dat_versions dv ON de.dat_version_id = dv.id
         JOIN dats d ON dv.dat_id = d.id",
    )?;

    let matches: Vec<(PathBuf, String, String, String)> = stmt
        .query_map([], |row| {
            Ok((
                PathBuf::from(row.get::<_, String>(0)?),
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if matches.is_empty() {
        println!("No matched files to organise. Run `bitshelf scan` and `bitshelf verify` first.");
        return Ok(());
    }

    println!(
        "{}",
        if dry_run {
            "Dry run - showing what would be done:"
        } else {
            "Organising files..."
        }
    );

    let mut organised = 0;
    let mut skipped = 0;
    let mut errors = 0;
    let mut seen_archives: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

    for (source_path, _filename, rom_name, dat_name) in &matches {
        // Handle archive paths (archive.zip#entry.rom)
        let (actual_source, target_filename) = if let Some(hash_pos) =
            source_path.to_string_lossy().find('#')
        {
            // File is inside an archive - organise the archive itself
            let archive_path_str = &source_path.to_string_lossy()[..hash_pos];
            let archive_path = PathBuf::from(archive_path_str);

            // Skip if we've already processed this archive
            if seen_archives.contains(&archive_path) {
                continue;
            }
            seen_archives.insert(archive_path.clone());

            // Use the archive filename, keeping the extension
            let archive_filename = archive_path
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "unknown.zip".to_string());

            (archive_path, archive_filename)
        } else {
            // Loose file - use the ROM name from the DAT
            (source_path.clone(), rom_name.clone())
        };

        // Create target path: target/dat_name/filename
        let target_dir = target.join(sanitise_path(dat_name));
        let target_path = target_dir.join(&target_filename);

        // Check if source exists
        if !actual_source.exists() {
            if dry_run {
                println!("  [MISSING] {}", actual_source.display());
            }
            skipped += 1;
            continue;
        }

        // Check if target already exists
        if target_path.exists() {
            if dry_run {
                println!("  [EXISTS] {}", target_path.display());
            }
            skipped += 1;
            continue;
        }

        if dry_run {
            println!(
                "  {} {} -> {}",
                if copy { "[COPY]" } else { "[MOVE]" },
                actual_source.display(),
                target_path.display()
            );
            organised += 1;
        } else {
            // Create target directory
            if let Err(e) = std::fs::create_dir_all(&target_dir) {
                eprintln!("  Error creating {}: {}", target_dir.display(), e);
                errors += 1;
                continue;
            }

            // Copy or move the file
            let result = if copy {
                std::fs::copy(&actual_source, &target_path).map(|_| ())
            } else {
                std::fs::rename(&actual_source, &target_path)
            };

            match result {
                Ok(()) => {
                    organised += 1;
                }
                Err(e) => {
                    eprintln!(
                        "  Error {} {}: {}",
                        if copy { "copying" } else { "moving" },
                        actual_source.display(),
                        e
                    );
                    errors += 1;
                }
            }
        }
    }

    println!();
    println!(
        "{}:",
        if dry_run {
            "Would organise"
        } else {
            "Organised"
        }
    );
    println!(
        "  {}: {:>6}",
        if copy { "Copied" } else { "Moved" },
        organised
    );
    if skipped > 0 {
        println!("  Skipped:  {:>6}", skipped);
    }
    if errors > 0 {
        println!("  Errors:   {:>6}", errors);
    }

    Ok(())
}

/// Sanitise a string for use as a directory/file name
fn sanitise_path(name: &str) -> String {
    name.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .collect()
}
