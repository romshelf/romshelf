use anyhow::Result;
use clap::{Parser, Subcommand};
use serde::Serialize;
use serde_json::json;
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use romshelf_core::dat;
use romshelf_core::db;
use romshelf_core::scan::{self, ScanProgress};
use romshelf_core::services::dat_importer::{DatImportOptions, DatImportOutcome, DatImporter};
use romshelf_core::services::progress::{DatImportEvent, ProgressSink, ScanEvent};
use romshelf_core::tosec;
use romshelf_core::verify;

/// A matched file ready for organisation
/// (source_path, filename, rom_name, dat_name, set_name, category)
type MatchedFile = (
    PathBuf,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

#[derive(Parser)]
#[command(name = "romshelf")]
#[command(about = "ROM collection manager - DAT-driven verification and organisation")]
struct Cli {
    /// Show verbose progress (current file, archives being opened, etc.)
    #[arg(long, short = 'v', global = true)]
    verbose: bool,

    /// Emit progress events as JSON instead of interactive text
    #[arg(long, global = true, default_value_t = false)]
    progress_json: bool,

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
        /// Directory to scan (optional with --prune)
        path: Option<PathBuf>,

        /// Number of worker threads (default: all cores)
        #[arg(long, short = 't')]
        threads: Option<usize>,

        /// Remove database entries for files that no longer exist on disk
        #[arg(long)]
        prune: bool,
    },
    /// Verify ROMs against loaded DATs
    Verify {
        /// Show detailed issues
        #[arg(long)]
        issues: bool,
    },
    /// Organise ROMs into a structured directory
    Organise {
        /// Target directory for organised ROMs (not used with --rename-only)
        #[arg(long, required_unless_present = "rename_only")]
        target: Option<PathBuf>,

        /// Dry run - show what would be done without making changes
        #[arg(long)]
        dry_run: bool,

        /// Copy files instead of moving them
        #[arg(long)]
        copy: bool,

        /// Output as loose files instead of ZIP archives
        #[arg(long)]
        loose: bool,

        /// Create one ZIP per DAT instead of per set
        #[arg(long)]
        zip_per_dat: bool,

        /// Only rename misnamed files in-place (don't reorganise)
        #[arg(long)]
        rename_only: bool,
    },
    /// Show collection statistics
    Stats,
    /// Show collection health report
    Health,
    /// Find duplicate files in the collection
    Duplicates {
        /// Show all duplicate file paths (not just summary)
        #[arg(long)]
        details: bool,
    },
}

#[derive(Subcommand)]
enum DatCommands {
    /// Import a DAT file
    Import {
        /// Path to DAT file
        path: PathBuf,

        /// Category for the DAT (e.g., "MAME/Arcade")
        #[arg(long)]
        category: Option<String>,
    },
    /// Import all DAT files from a directory (recursive)
    ImportDir {
        /// Directory containing DAT files
        path: PathBuf,

        /// Category prefix (e.g., "TOSEC" to create TOSEC/Manufacturer/System/...)
        #[arg(long)]
        prefix: Option<String>,
    },
    /// List imported DATs
    List {
        /// Filter by category (substring match)
        #[arg(long)]
        category: Option<String>,

        /// Search by name (substring match)
        #[arg(long)]
        search: Option<String>,
    },
    /// Show detailed information about a DAT
    Info {
        /// DAT ID or name (partial match)
        dat: String,
    },
    /// Remove a DAT and all its entries
    Remove {
        /// DAT ID or name (partial match)
        dat: String,

        /// Skip confirmation prompt
        #[arg(long, short = 'y')]
        yes: bool,

        /// Show what would be removed without actually removing
        #[arg(long)]
        dry_run: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Get database path
    let db_path = get_db_path()?;
    let mut conn = db::init_db(&db_path)?;

    let verbose = cli.verbose;
    let progress_sink = CliProgressSink::new(cli.progress_json);

    match cli.command {
        Commands::Dat { command } => match command {
            DatCommands::Import { path, category } => cmd_dat_import(
                &mut conn,
                path.as_path(),
                category.as_deref(),
                progress_sink.clone(),
            ),
            DatCommands::ImportDir { path, prefix } => cmd_dat_import_dir(
                &mut conn,
                &path,
                prefix.as_deref(),
                verbose,
                progress_sink.clone(),
            ),
            DatCommands::List { category, search } => {
                cmd_dat_list(&conn, category.as_deref(), search.as_deref())
            }
            DatCommands::Info { dat } => cmd_dat_info(&conn, &dat),
            DatCommands::Remove { dat, yes, dry_run } => cmd_dat_remove(&conn, &dat, yes, dry_run),
        },
        Commands::Scan {
            path,
            threads,
            prune,
        } => {
            if prune {
                cmd_prune(&conn, verbose)
            } else if let Some(path) = path {
                cmd_scan(
                    &conn,
                    &path,
                    threads,
                    verbose,
                    cli.progress_json,
                    progress_sink.clone(),
                )
            } else {
                eprintln!("Error: Path required unless using --prune");
                std::process::exit(1);
            }
        }
        Commands::Verify { issues } => cmd_verify(&conn, issues),
        Commands::Organise {
            target,
            dry_run,
            copy,
            loose,
            zip_per_dat,
            rename_only,
        } => {
            if rename_only {
                cmd_rename_in_place(&conn, dry_run)
            } else {
                cmd_organise(
                    &conn,
                    target.as_ref().unwrap(),
                    dry_run,
                    copy,
                    loose,
                    zip_per_dat,
                )
            }
        }
        Commands::Stats => cmd_stats(&conn),
        Commands::Health => cmd_health(&conn),
        Commands::Duplicates { details } => cmd_duplicates(&conn, details),
    }
}

fn get_db_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Cannot find home directory"))?;
    let config_dir = home.join(".romshelf");
    std::fs::create_dir_all(&config_dir)?;
    Ok(config_dir.join("romshelf.db"))
}

/// Import result for tracking duplicates
enum ImportResult {
    Imported {
        name: String,
        version: Option<String>,
        entries: usize,
        duration: Duration,
        entries_per_sec: f64,
    },
    Duplicate {
        name: String,
    },
    Unchanged {
        name: String,
    },
    Failed {
        path: PathBuf,
        error: String,
    },
}

fn cmd_dat_import(
    conn: &mut rusqlite::Connection,
    path: &Path,
    category: Option<&str>,
    progress_sink: CliProgressSink,
) -> Result<()> {
    match import_single_dat(conn, path, category, None, progress_sink.clone())? {
        ImportResult::Imported {
            name,
            version,
            entries,
            duration,
            entries_per_sec,
        } => {
            println!("Imported: {}", name);
            if let Some(v) = version {
                println!("  Version: {}", v);
            }
            println!("  Entries: {} ({:.1} per second)", entries, entries_per_sec);
            println!("  Duration: {:.2}s", duration.as_secs_f64());
        }
        ImportResult::Duplicate { name } => {
            println!("Skipped (duplicate): {}", name);
        }
        ImportResult::Unchanged { name } => {
            println!("Skipped (unchanged): {}", name);
        }
        ImportResult::Failed { path, error } => {
            eprintln!("Failed to import {}: {}", path.display(), error);
        }
    }
    Ok(())
}

fn cmd_dat_import_dir(
    conn: &mut rusqlite::Connection,
    path: &Path,
    prefix: Option<&str>,
    verbose: bool,
    progress_sink: CliProgressSink,
) -> Result<()> {
    use walkdir::WalkDir;

    eprintln!("Scanning for DAT files in {}...", path.display());

    // Canonicalize the base path for reliable relative path calculation
    let base_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());

    if verbose && !progress_sink.is_json() {
        eprintln!("  Scanning directories...");
    }

    let dat_files: Vec<PathBuf> = WalkDir::new(path)
        .follow_links(true)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_type().is_file()
                && e.path()
                    .extension()
                    .map(|ext| ext.eq_ignore_ascii_case("dat") || ext.eq_ignore_ascii_case("xml"))
                    .unwrap_or(false)
        })
        .map(|e| e.path().to_path_buf())
        .collect();

    if verbose && !progress_sink.is_json() {
        eprintln!("  Scanning directories...");
    }

    eprintln!("Found {} DAT files", dat_files.len());

    let mut imported = 0;
    let mut duplicates = 0;
    let mut failed = 0;

    for (i, dat_path) in dat_files.iter().enumerate() {
        if verbose {
            if !progress_sink.is_json() {
                // Show full DAT path in verbose mode
                let display_name = dat_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                eprint!(
                    "\r\x1b[2K  [{:>4}/{:>4}] {}{}",
                    i + 1,
                    dat_files.len(),
                    display_name,
                    " ".repeat(60usize.saturating_sub(display_name.len()))
                );
            }
        } else if !progress_sink.is_json() {
            eprint!("\r\x1b[2K  Processing: {}/{}", i + 1, dat_files.len());
        }

        // Compute category from relative path (parent directory of DAT file)
        // Use prefix if provided, otherwise use the base folder name
        let category_root = prefix
            .map(|p| p.to_string())
            .or_else(|| {
                base_path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
            })
            .unwrap_or_default();

        // Try TOSEC filename parsing first - this gives us proper manufacturer/platform paths
        let tosec_category = dat_path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(tosec::parse_tosec_category)
            .map(|cat| format!("{}/{}", category_root, cat));

        // Fall back to directory-based category if TOSEC parsing didn't work
        let dir_category = dat_path
            .canonicalize()
            .ok()
            .and_then(|p| p.parent().map(|p| p.to_path_buf()))
            .and_then(|parent| {
                parent
                    .strip_prefix(&base_path)
                    .ok()
                    .map(|p| p.to_path_buf())
            })
            .map(|rel_path| {
                let rel_str = rel_path.to_string_lossy();
                if rel_str.is_empty() {
                    category_root.clone()
                } else {
                    format!("{}/{}", category_root, rel_str)
                }
            })
            .filter(|s| !s.is_empty());

        // Prefer TOSEC filename parsing, then directory structure
        let category = tosec_category.or(dir_category);

        match import_single_dat(
            conn,
            dat_path,
            category.as_deref(),
            Some(base_path.as_path()),
            progress_sink.clone(),
        ) {
            Ok(ImportResult::Imported { .. }) => imported += 1,
            Ok(ImportResult::Duplicate { .. }) => duplicates += 1,
            Ok(ImportResult::Unchanged { .. }) => duplicates += 1,
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

    if !progress_sink.is_json() {
        eprintln!(); // New line after progress
    }
    println!("\nImport complete:");
    println!("  Imported:   {:>6}", imported);
    println!("  Duplicates: {:>6}", duplicates);
    if failed > 0 {
        println!("  Failed:     {:>6}", failed);
    }

    Ok(())
}

fn import_single_dat(
    conn: &mut rusqlite::Connection,
    path: &Path,
    category: Option<&str>,
    category_root: Option<&Path>,
    progress_sink: CliProgressSink,
) -> Result<ImportResult> {
    let mut importer = DatImporter::new(conn, progress_sink);
    let options = DatImportOptions {
        category: category.map(|c| c.to_string()),
        category_root: category_root.map(|p| p.to_path_buf()),
    };
    let result = match importer.import_path(path, options, |_event| {}) {
        Ok(res) => res,
        Err(e) => {
            return Ok(ImportResult::Failed {
                path: path.to_path_buf(),
                error: e.to_string(),
            });
        }
    };
    let mapped = match result.outcome {
        DatImportOutcome::Imported {
            name,
            entry_count,
            entries_per_sec,
            ..
        } => ImportResult::Imported {
            name,
            version: None,
            entries: entry_count as usize,
            duration: result.duration,
            entries_per_sec,
        },
        DatImportOutcome::Duplicate { name } => ImportResult::Duplicate { name },
        DatImportOutcome::Unchanged { name } => ImportResult::Unchanged { name },
    };
    Ok(mapped)
}

type DatListRow = (i64, String, Option<String>, Option<String>, i64, String);

fn cmd_dat_list(
    conn: &rusqlite::Connection,
    category_filter: Option<&str>,
    search_filter: Option<&str>,
) -> Result<()> {
    // Build query with optional filters
    let mut sql = String::from(
        "SELECT d.id, d.name, d.category, dv.version, dv.entry_count, dv.loaded_at
         FROM dats d
         JOIN dat_versions dv ON d.id = dv.dat_id
         WHERE 1=1",
    );

    if category_filter.is_some() {
        sql.push_str(" AND d.category LIKE '%' || ?1 || '%'");
    }

    if search_filter.is_some() {
        let param_num = if category_filter.is_some() {
            "?2"
        } else {
            "?1"
        };
        sql.push_str(&format!(" AND d.name LIKE '%' || {} || '%'", param_num));
    }

    sql.push_str(" ORDER BY d.category, d.name");

    let mut stmt = conn.prepare(&sql)?;

    let rows: Vec<DatListRow> = match (category_filter, search_filter) {
        (Some(cat), Some(search)) => stmt
            .query_map([cat, search], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect(),
        (Some(cat), None) => stmt
            .query_map([cat], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect(),
        (None, Some(search)) => stmt
            .query_map([search], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect(),
        (None, None) => stmt
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect(),
    };

    let count = rows.len();

    for (id, name, category, version, entry_count, loaded_at) in rows {
        println!("[{}] {}", id, name);
        if let Some(cat) = category {
            println!("    Category: {}", cat);
        }
        if let Some(v) = version {
            println!("    Version: {}", v);
        }
        println!("    Entries: {}", entry_count);
        println!("    Loaded: {}", loaded_at);
        println!();
    }

    if count == 0 {
        if category_filter.is_some() || search_filter.is_some() {
            println!("No DATs match the specified filters.");
        } else {
            println!("No DATs imported yet. Use `romshelf dat import <path>` to import one.");
        }
    } else {
        println!("Total: {} DATs", count);
    }

    Ok(())
}

/// Show detailed information about a DAT
fn cmd_dat_info(conn: &rusqlite::Connection, dat_ref: &str) -> Result<()> {
    // Try to find by ID first, then by name
    let dat_id: Option<i64> = dat_ref.parse().ok().and_then(|id: i64| {
        conn.query_row("SELECT id FROM dats WHERE id = ?1", [id], |row| row.get(0))
            .ok()
    });

    let dat_id = match dat_id {
        Some(id) => id,
        None => {
            // Search by name (case-insensitive substring match)
            let matches: Vec<(i64, String)> = conn
                .prepare("SELECT id, name FROM dats WHERE name LIKE '%' || ?1 || '%'")?
                .query_map([dat_ref], |row| Ok((row.get(0)?, row.get(1)?)))?
                .filter_map(|r| r.ok())
                .collect();

            match matches.len() {
                0 => {
                    println!("No DAT found matching '{}'", dat_ref);
                    return Ok(());
                }
                1 => matches[0].0,
                _ => {
                    println!(
                        "Multiple DATs match '{}'. Please be more specific:",
                        dat_ref
                    );
                    for (id, name) in &matches {
                        println!("  [{}] {}", id, name);
                    }
                    return Ok(());
                }
            }
        }
    };

    // Get DAT details
    let (name, format, file_path, category, file_size, file_mtime): (
        String,
        String,
        String,
        Option<String>,
        Option<i64>,
        Option<i64>,
    ) = conn.query_row(
        "SELECT name, format, file_path, category, file_size, file_mtime FROM dats WHERE id = ?1",
        [dat_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
            ))
        },
    )?;

    // Get version details
    let (version_id, version, loaded_at, entry_count): (i64, Option<String>, String, i64) = conn
        .query_row(
            "SELECT id, version, loaded_at, entry_count FROM dat_versions WHERE dat_id = ?1",
            [dat_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )?;

    // Get set count
    let set_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sets WHERE dat_version_id = ?1",
        [version_id],
        |row| row.get(0),
    )?;

    // Get match count (how many entries have matching files)
    let matched_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT de.id) FROM dat_entries de
         JOIN files f ON (f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size))
         WHERE de.dat_version_id = ?1",
        [version_id],
        |row| row.get(0),
    )?;

    println!("DAT Information");
    println!("===============");
    println!("  ID:         {}", dat_id);
    println!("  Name:       {}", name);
    if let Some(v) = version {
        println!("  Version:    {}", v);
    }
    if let Some(cat) = category {
        println!("  Category:   {}", cat);
    }
    println!("  Format:     {}", format);
    println!("  File:       {}", file_path);
    if let Some(size) = file_size {
        println!("  File size:  {}", format_bytes(size));
    }
    if let Some(mtime) = file_mtime {
        let dt = chrono::DateTime::from_timestamp(mtime, 0)
            .map(|d| d.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| mtime.to_string());
        println!("  File mtime: {}", dt);
    }
    println!("  Loaded:     {}", loaded_at);
    println!();
    println!("Contents");
    println!("--------");
    println!("  Sets:       {:>8}", set_count);
    println!("  Entries:    {:>8}", entry_count);
    println!();
    println!("Collection Status");
    println!("-----------------");
    let pct = if entry_count > 0 {
        (matched_count as f64 / entry_count as f64) * 100.0
    } else {
        0.0
    };
    println!(
        "  Matched:    {:>8} / {} ({:.1}%)",
        matched_count, entry_count, pct
    );
    println!("  Missing:    {:>8}", entry_count - matched_count);

    Ok(())
}

/// Remove a DAT and all its entries
fn cmd_dat_remove(
    conn: &rusqlite::Connection,
    dat_ref: &str,
    skip_confirm: bool,
    dry_run: bool,
) -> Result<()> {
    // Try to find by ID first, then by name
    let dat_id: Option<i64> = dat_ref.parse().ok().and_then(|id: i64| {
        conn.query_row("SELECT id FROM dats WHERE id = ?1", [id], |row| row.get(0))
            .ok()
    });

    let dat_id = match dat_id {
        Some(id) => id,
        None => {
            // Search by name (case-insensitive substring match)
            let matches: Vec<(i64, String)> = conn
                .prepare("SELECT id, name FROM dats WHERE name LIKE '%' || ?1 || '%'")?
                .query_map([dat_ref], |row| Ok((row.get(0)?, row.get(1)?)))?
                .filter_map(|r| r.ok())
                .collect();

            match matches.len() {
                0 => {
                    println!("No DAT found matching '{}'", dat_ref);
                    return Ok(());
                }
                1 => matches[0].0,
                _ => {
                    println!(
                        "Multiple DATs match '{}'. Please be more specific:",
                        dat_ref
                    );
                    for (id, name) in &matches {
                        println!("  [{}] {}", id, name);
                    }
                    return Ok(());
                }
            }
        }
    };

    // Get DAT details for confirmation
    let (name, entry_count): (String, i64) = conn.query_row(
        "SELECT d.name, dv.entry_count FROM dats d
         JOIN dat_versions dv ON d.id = dv.dat_id
         WHERE d.id = ?1",
        [dat_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    // Get version ID for counts/deletion
    let version_id: i64 = conn.query_row(
        "SELECT id FROM dat_versions WHERE dat_id = ?1",
        [dat_id],
        |row| row.get(0),
    )?;

    // Get counts for display
    let set_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sets WHERE dat_version_id = ?1",
        [version_id],
        |row| row.get(0),
    )?;

    let match_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM matches WHERE dat_entry_id IN (SELECT id FROM dat_entries WHERE dat_version_id = ?1)",
        [version_id],
        |row| row.get(0),
    )?;

    // Dry run - just show what would be removed
    if dry_run {
        println!("Would remove:");
        println!("  [{}] {}", dat_id, name);
        println!();
        println!("Would delete:");
        println!("  Entries:    {:>6}", entry_count);
        println!("  Sets:       {:>6}", set_count);
        if match_count > 0 {
            println!("  Matches:    {:>6}", match_count);
        }
        return Ok(());
    }

    // Confirm deletion unless -y flag was passed
    if !skip_confirm {
        println!("About to remove:");
        println!("  [{}] {} ({} entries)", dat_id, name, entry_count);
        println!();
        eprint!("Are you sure? [y/N] ");

        use std::io::{self, Write};
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Cancelled.");
            return Ok(());
        }
    }

    // Delete in order: matches -> dat_entries -> sets -> dat_versions -> dats
    // Note: matches reference dat_entries, so delete them first
    let matches_deleted: usize = conn.execute(
        "DELETE FROM matches WHERE dat_entry_id IN (SELECT id FROM dat_entries WHERE dat_version_id = ?1)",
        [version_id],
    )?;

    let entries_deleted: usize = conn.execute(
        "DELETE FROM dat_entries WHERE dat_version_id = ?1",
        [version_id],
    )?;

    let sets_deleted: usize =
        conn.execute("DELETE FROM sets WHERE dat_version_id = ?1", [version_id])?;

    conn.execute("DELETE FROM dat_versions WHERE id = ?1", [version_id])?;
    conn.execute("DELETE FROM dats WHERE id = ?1", [dat_id])?;

    println!("Removed: {}", name);
    println!("  Entries deleted: {}", entries_deleted);
    println!("  Sets deleted:    {}", sets_deleted);
    if matches_deleted > 0 {
        println!("  Matches deleted: {}", matches_deleted);
    }

    Ok(())
}

fn cmd_scan(
    conn: &rusqlite::Connection,
    path: &Path,
    threads: Option<usize>,
    verbose: bool,
    json_progress: bool,
    progress_sink: CliProgressSink,
) -> Result<()> {
    let thread_count = threads.unwrap_or_else(num_cpus::get).max(1);
    let cancel_flag = Arc::new(AtomicBool::new(false));
    if !json_progress {
        eprintln!("  Press Enter to stop the scan gracefully...");
        let cancel_clone = cancel_flag.clone();
        thread::spawn(move || {
            let stdin = io::stdin();
            let mut handle = stdin.lock();
            let mut line = String::new();
            let _ = handle.read_line(&mut line);
            cancel_clone.store(true, Ordering::SeqCst);
        });
    }

    // Load existing files from database for incremental scan
    let existing_files: std::collections::HashMap<String, (i64, Option<i64>)> = {
        let mut stmt = conn.prepare("SELECT path, size, mtime FROM files")?;
        stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                (row.get::<_, i64>(1)?, row.get::<_, Option<i64>>(2)?),
            ))
        })?
        .filter_map(|r| r.ok())
        .collect()
    };

    let existing_count = existing_files.len();
    if !json_progress {
        if existing_count > 0 {
            eprintln!(
                "Scanning {} with {} threads ({} files in database)...",
                path.display(),
                thread_count,
                existing_count
            );
        } else {
            eprintln!(
                "Scanning {} with {} threads...",
                path.display(),
                thread_count
            );
        }
        eprintln!("  Discovering directories and files...");
    }

    let progress = if json_progress {
        let sink: Arc<dyn ProgressSink<ScanEvent>> = Arc::new(progress_sink.clone());
        Arc::new(ScanProgress::with_sink(sink))
    } else {
        Arc::new(ScanProgress::new())
    };
    let progress_display = Arc::clone(&progress);

    // Progress display thread
    let display_handle = if json_progress {
        None
    } else {
        let cancel_for_display = cancel_flag.clone();
        Some(thread::spawn(move || {
            let mut last_line_count = 0usize;

            loop {
                let discovered = progress_display.discovered.load(Ordering::Relaxed);
                let processed = progress_display.processed.load(Ordering::Relaxed);
                let bytes_sec = progress_display.bytes_per_sec();
                let elapsed = progress_display.start_time.elapsed().as_secs_f64();
                let files_per_sec = if elapsed > 0.0 {
                    processed as f64 / elapsed
                } else {
                    0.0
                };
                let eta_text = if files_per_sec > 0.0 && discovered > processed {
                    Some(format_eta(
                        ((discovered - processed) as f64) / files_per_sec,
                    ))
                } else {
                    None
                };

                if verbose {
                    // Clear previous lines if we printed multiple
                    if last_line_count > 0 {
                        // Move cursor up and clear each line
                        for _ in 0..last_line_count {
                            eprint!("\x1b[A\x1b[2K"); // Move up, clear line
                        }
                    }
                    eprint!("\r\x1b[2K"); // Clear current line

                    // Get all active files (sorted by size, largest first)
                    let active_files = progress_display.get_active_files();
                    let file_count = active_files.len();

                    // Extract unique directories currently being processed
                    let active_dirs: std::collections::HashSet<String> = active_files
                        .iter()
                        .filter_map(|f| {
                            // Handle archive paths: /path/to/archive.zip#entry -> /path/to
                            let base_path = if let Some(hash_pos) = f.path.rfind('#') {
                                &f.path[..hash_pos]
                            } else {
                                &f.path
                            };
                            // Get parent directory
                            std::path::Path::new(base_path)
                                .parent()
                                .map(|p| p.to_string_lossy().to_string())
                        })
                        .collect();

                    // Show current directory (most common among active files, or first alphabetically)
                    let current_dir = active_dirs.iter().min().cloned().unwrap_or_default();
                    let display_dir = if current_dir.len() > 60 {
                        format!("...{}", &current_dir[current_dir.len() - 57..])
                    } else {
                        current_dir
                    };

                    // Header line with overall progress and current directory
                    let eta_suffix = eta_text
                        .as_ref()
                        .map(|eta| format!("  ETA {}", eta))
                        .unwrap_or_default();
                    eprintln!(
                        "  [{:>6}/{:>6}] {:>8}/s  {} workers  {}{}",
                        processed,
                        discovered,
                        format_bytes_short(bytes_sec as i64),
                        file_count,
                        display_dir,
                        eta_suffix
                    );

                    // Show up to 8 active files with progress bars
                    let max_display = 8.min(file_count);
                    for (i, file_prog) in active_files.iter().take(max_display).enumerate() {
                        // Extract just the filename (after # for archives, or last path component)
                        let display_name = if let Some(hash_pos) = file_prog.path.rfind('#') {
                            &file_prog.path[hash_pos + 1..]
                        } else {
                            file_prog.path.rsplit('/').next().unwrap_or(&file_prog.path)
                        };

                        // Truncate if too long (allow up to 80 chars for filename)
                        let display_name = if display_name.len() > 80 {
                            format!("...{}", &display_name[display_name.len() - 77..])
                        } else {
                            display_name.to_string()
                        };

                        // Calculate progress percentage
                        let pct = if file_prog.size > 0 {
                            ((file_prog.bytes_done as f64 / file_prog.size as f64) * 100.0)
                                .min(100.0)
                        } else {
                            0.0
                        };

                        // Tree-style prefix
                        let prefix = if i == max_display - 1 { "└" } else { "├" };

                        // Size display
                        let size_str = format_bytes_short(file_prog.size as i64);

                        eprintln!(
                            "    {} {:>6} {:>3.0}%  {}",
                            prefix, size_str, pct, display_name
                        );
                    }

                    // Track how many lines we printed (1 header + file lines)
                    last_line_count = 1 + max_display;

                    // If there are more files, show count
                    if file_count > max_display {
                        eprintln!("    ... and {} more", file_count - max_display);
                        last_line_count += 1;
                    }
                } else {
                    let eta_suffix = eta_text
                        .as_ref()
                        .map(|eta| format!("  ETA {}", eta))
                        .unwrap_or_default();
                    eprint!(
                        "\r\x1b[2K  Discovered: {:>6}  Processed: {:>6}  Speed: {:>8}/s{}",
                        discovered,
                        processed,
                        format_bytes_short(bytes_sec as i64),
                        eta_suffix
                    );
                }

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

                if cancel_for_display.load(Ordering::Relaxed) {
                    eprintln!("\nCancellation requested. Finishing current files...");
                    break;
                }

                thread::sleep(Duration::from_millis(100));
            }
            eprintln!(); // New line after progress
        }))
    };

    // Run the scan
    let result =
        scan::scan_directory_parallel(path, thread_count, progress, Some(cancel_flag.clone()))?;

    // Wait for progress display to finish
    if let Some(handle) = display_handle {
        let _ = handle.join();
    }

    // Track paths we've seen (for detecting missing files)
    let mut seen_paths: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Store scanned files in database
    let now = chrono::Utc::now().to_rfc3339();
    let mut stmt = conn.prepare(
        "INSERT OR REPLACE INTO files (path, filename, size, mtime, crc32, md5, sha1, scanned_at, directory_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
    )?;

    // Cache for directory IDs to avoid repeated lookups
    let mut dir_cache: std::collections::HashMap<String, i64> = std::collections::HashMap::new();

    let mut new_files = 0;
    let mut updated_files = 0;
    let mut unchanged_files = 0;

    for file in result.files.iter() {
        let path_str = file.path.to_string_lossy().to_string();
        seen_paths.insert(path_str.clone());

        // Check if file is unchanged (same size and mtime)
        if let Some(&(existing_size, existing_mtime)) = existing_files.get(&path_str) {
            if existing_size == file.size as i64 && existing_mtime == file.mtime {
                // File unchanged - skip updating (but track it as seen)
                unchanged_files += 1;
                continue;
            }
            updated_files += 1;
        } else {
            new_files += 1;
        }

        // Get or create directory entry
        let dir_path = file
            .path
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        let dir_id = if let Some(&id) = dir_cache.get(&dir_path) {
            id
        } else {
            let id = db::get_or_create_directory(conn, &dir_path)?;
            dir_cache.insert(dir_path.clone(), id);
            id
        };

        stmt.execute(rusqlite::params![
            path_str,
            file.filename,
            file.size as i64,
            file.mtime,
            file.crc32,
            file.md5,
            file.sha1,
            now,
            dir_id
        ])?;
    }

    // Handle missing files (files in DB that were under this scan path but weren't found)
    // Only remove files that are within the scanned directory - don't touch files from other paths
    let scan_path_str = path.to_string_lossy().to_string();
    let mut missing_files = 0;
    for existing_path in existing_files.keys() {
        // Only consider files that are under the scanned directory
        if existing_path.starts_with(&scan_path_str) && !seen_paths.contains(existing_path) {
            // File was in the scanned directory but no longer exists - remove from database
            conn.execute("DELETE FROM files WHERE path = ?1", [existing_path])?;
            missing_files += 1;
        }
    }

    // Print summary
    let duration_secs = result.duration.as_secs_f64();
    let bytes_per_sec = if duration_secs > 0.0 {
        result.total_bytes as f64 / duration_secs
    } else {
        result.total_bytes as f64
    };
    let files_per_sec = if duration_secs > 0.0 {
        result.files.len() as f64 / duration_secs
    } else {
        0.0
    };

    println!(
        "\nScan {} in {:.1}s",
        if cancel_flag.load(Ordering::Relaxed) {
            "cancelled"
        } else {
            "complete"
        },
        result.duration.as_secs_f32()
    );
    println!("  Files:      {:>6}", result.files.len());
    println!(
        "  Data:       {:>6}",
        format_bytes(result.total_bytes as i64)
    );

    if existing_count > 0 {
        println!("  New:        {:>6}", new_files);
        println!("  Updated:    {:>6}", updated_files);
        println!("  Unchanged:  {:>6}", unchanged_files);
        if missing_files > 0 {
            println!("  Removed:    {:>6}", missing_files);
        }
    }

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

    println!(
        "  Throughput: {:>6.1} files/s, {}/s",
        files_per_sec,
        format_bytes(bytes_per_sec as i64)
    );

    // Show skipped files if any
    if !result.skipped.is_empty() {
        println!("\nSkipped files:");
        for skipped in result.skipped.iter().take(20) {
            println!("  {} ({})", skipped.path.display(), skipped.reason);
        }
        if result.skipped.len() > 20 {
            println!("  ... and {} more", result.skipped.len() - 20);
        }
    }

    if cancel_flag.load(Ordering::Relaxed) {
        println!(
            "\nScan stopped early. Run the same command again to continue scanning remaining directories."
        );
    }

    // Recompute directory statistics (rollup from files to directories)
    if !dir_cache.is_empty() {
        eprint!("  Computing directory statistics...");
        db::recompute_directory_stats(conn)?;
        eprintln!(" done ({} directories)", dir_cache.len());
    }

    Ok(())
}

/// Remove database entries for files that no longer exist on disk
fn cmd_prune(conn: &rusqlite::Connection, verbose: bool) -> Result<()> {
    eprintln!("Checking for stale database entries...");

    // Load all file paths from database
    let paths: Vec<(i64, String)> = {
        let mut stmt = conn.prepare("SELECT id, path FROM files")?;
        stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .filter_map(|r| r.ok())
            .collect()
    };

    let total = paths.len();
    if total == 0 {
        println!("No files in database.");
        return Ok(());
    }

    eprintln!("Checking {} files...", total);

    let mut pruned = 0;
    let mut kept = 0;

    for (i, (id, path_str)) in paths.iter().enumerate() {
        // Show progress periodically
        if (i + 1) % 1000 == 0 || i + 1 == total {
            eprint!("\r  Checked: {:>6}/{}", i + 1, total);
        }

        // Handle archive paths (archive.zip#entry.rom)
        let path_to_check = if let Some(hash_pos) = path_str.find('#') {
            PathBuf::from(&path_str[..hash_pos])
        } else {
            PathBuf::from(path_str)
        };

        if path_to_check.exists() {
            kept += 1;
        } else {
            // File no longer exists - remove from database
            if verbose {
                eprintln!("\r  Pruning: {}", path_str);
            }
            conn.execute("DELETE FROM files WHERE id = ?1", [id])?;
            pruned += 1;
        }
    }

    eprintln!(); // Clear progress line

    println!("\nPrune complete:");
    println!("  Checked:    {:>6}", total);
    println!("  Kept:       {:>6}", kept);
    println!("  Pruned:     {:>6}", pruned);

    Ok(())
}

fn cmd_verify(conn: &rusqlite::Connection, show_issues: bool) -> Result<()> {
    // Load files from database
    let mut file_stmt =
        conn.prepare("SELECT path, filename, size, mtime, crc32, md5, sha1 FROM files")?;
    let files: Vec<scan::ScannedFile> = file_stmt
        .query_map([], |row| {
            Ok(scan::ScannedFile {
                path: PathBuf::from(row.get::<_, String>(0)?),
                filename: row.get(1)?,
                size: row.get::<_, i64>(2)? as u64,
                mtime: row.get(3)?,
                crc32: row.get(4)?,
                md5: row.get(5)?,
                sha1: row.get(6)?,
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
                    name: row.get(0)?,
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
        println!("No DATs loaded. Use `romshelf dat import <path>` first.");
        return Ok(());
    }

    if files.is_empty() {
        println!("No files scanned. Use `romshelf scan <path>` first.");
        return Ok(());
    }

    // Group entries by DAT name
    let mut entries_by_dat: std::collections::HashMap<String, Vec<dat::DatEntry>> =
        std::collections::HashMap::new();
    for (entry, dat_name) in all_entries {
        entries_by_dat.entry(dat_name).or_default().push(entry);
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
                println!("  {} -> {}", m.file.filename, m.entry.name);
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
    target: &Path,
    dry_run: bool,
    copy: bool,
    loose: bool,
    zip_per_dat: bool,
) -> Result<()> {
    // Load all matched files with their DAT and set info
    // Include category for directory structure
    let mut stmt = conn.prepare(
        "SELECT f.path, f.filename, de.name as rom_name, d.name as dat_name, s.name as set_name, d.category
         FROM files f
         JOIN dat_entries de ON f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
         JOIN dat_versions dv ON de.dat_version_id = dv.id
         JOIN dats d ON dv.dat_id = d.id
         LEFT JOIN sets s ON de.set_id = s.id",
    )?;

    let matches: Vec<MatchedFile> = stmt
        .query_map([], |row| {
            Ok((
                PathBuf::from(row.get::<_, String>(0)?),
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?
        .filter_map(|r| r.ok())
        .collect();

    if matches.is_empty() {
        println!("No matched files to organise. Run `romshelf scan` and `romshelf verify` first.");
        return Ok(());
    }

    let mode_desc = if loose {
        "as loose files"
    } else if zip_per_dat {
        "into ZIP per DAT"
    } else {
        "into TorrentZIP per set"
    };

    println!(
        "{}",
        if dry_run {
            format!("Dry run - showing what would be organised {}:", mode_desc)
        } else {
            format!("Organising files {}...", mode_desc)
        }
    );

    if loose {
        organise_loose(&matches, target, dry_run, copy)
    } else if zip_per_dat {
        organise_zip_per_dat(&matches, target, dry_run, copy)
    } else {
        organise_zip_per_set(&matches, target, dry_run, copy)
    }
}

/// Organise files as loose files
fn organise_loose(matches: &[MatchedFile], target: &Path, dry_run: bool, copy: bool) -> Result<()> {
    let mut organised = 0;
    let mut skipped = 0;
    let mut errors = 0;
    let mut seen_archives: std::collections::HashSet<PathBuf> = std::collections::HashSet::new();

    for (source_path, _filename, rom_name, _dat_name, set_name, category) in matches {
        // Handle archive paths (archive.zip#entry.rom)
        let (actual_source, target_filename) =
            if let Some(hash_pos) = source_path.to_string_lossy().find('#') {
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

        // Create target path: target/category/[set_name/]filename
        // Category is the directory path like "CPC/Games/[DSK]"
        let base_dir = if let Some(cat) = category {
            target.join(cat)
        } else {
            target.to_path_buf()
        };
        let target_dir = if let Some(set) = set_name {
            base_dir.join(sanitise_path(set))
        } else {
            base_dir
        };
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
            if let Err(e) = std::fs::create_dir_all(&target_dir) {
                eprintln!("  Error creating {}: {}", target_dir.display(), e);
                errors += 1;
                continue;
            }

            let result = if copy {
                std::fs::copy(&actual_source, &target_path).map(|_| ())
            } else {
                std::fs::rename(&actual_source, &target_path)
            };

            match result {
                Ok(()) => organised += 1,
                Err(e) => {
                    eprintln!("  Error: {}", e);
                    errors += 1;
                }
            }
        }
    }

    print_organise_summary(organised, skipped, errors, dry_run, copy);
    Ok(())
}

/// Organise files into ZIP archives, one per set
fn organise_zip_per_set(
    matches: &[MatchedFile],
    target: &Path,
    dry_run: bool,
    copy: bool,
) -> Result<()> {
    // Group files by (category, set_name)
    // category is the path like "CPC/Games/[DSK]"
    let mut sets: std::collections::HashMap<(String, String), Vec<(PathBuf, String)>> =
        std::collections::HashMap::new();

    for (source_path, _filename, rom_name, _dat_name, set_name, category) in matches {
        let cat = category.clone().unwrap_or_default();
        let set = set_name.clone().unwrap_or_else(|| "unknown".to_string());
        let set_key = (cat, set);
        sets.entry(set_key)
            .or_default()
            .push((source_path.clone(), rom_name.clone()));
    }

    let mut archives_created = 0;
    let mut files_packed = 0;
    let mut errors = 0;

    for ((category, set_name), files) in &sets {
        // Use category path for directory structure
        let target_dir = target.join(category);
        let archive_name = format!("{}.zip", sanitise_path(set_name));
        let archive_path = target_dir.join(&archive_name);

        if dry_run {
            println!("  {} ({} files)", archive_path.display(), files.len());
            archives_created += 1;
            files_packed += files.len();
        } else {
            if archive_path.exists() {
                continue;
            }

            if let Err(e) = std::fs::create_dir_all(&target_dir) {
                eprintln!("  Error creating {}: {}", target_dir.display(), e);
                errors += 1;
                continue;
            }

            match create_archive_from_matches(&archive_path, files, copy) {
                Ok(count) => {
                    println!("  {} ({} files)", archive_path.display(), count);
                    archives_created += 1;
                    files_packed += count;
                }
                Err(e) => {
                    eprintln!("  [ERROR] {}: {}", archive_path.display(), e);
                    errors += 1;
                }
            }
        }
    }

    println!();
    println!("{}:", if dry_run { "Would create" } else { "Created" });
    println!("  Archives: {:>6}", archives_created);
    println!("  Files:    {:>6}", files_packed);
    if errors > 0 {
        println!("  Errors:   {:>6}", errors);
    }

    Ok(())
}

/// Organise files into ZIP archives, one per DAT
fn organise_zip_per_dat(
    matches: &[MatchedFile],
    target: &Path,
    dry_run: bool,
    copy: bool,
) -> Result<()> {
    // Group files by (category, dat_name)
    // category is the path like "CPC/Games/[DSK]"
    type DatFileEntry = (PathBuf, String, Option<String>);
    let mut dats: std::collections::HashMap<(String, String), Vec<DatFileEntry>> =
        std::collections::HashMap::new();

    for (source_path, _filename, rom_name, dat_name, set_name, category) in matches {
        let cat = category.clone().unwrap_or_default();
        let dat_key = (cat, dat_name.clone());
        dats.entry(dat_key).or_default().push((
            source_path.clone(),
            rom_name.clone(),
            set_name.clone(),
        ));
    }

    let mut archives_created = 0;
    let mut files_packed = 0;
    let mut errors = 0;

    for ((category, dat_name), files) in &dats {
        // Use category path for directory structure
        let target_dir = target.join(category);
        let archive_name = format!("{}.zip", sanitise_path(dat_name));
        let archive_path = target_dir.join(&archive_name);

        if dry_run {
            println!("  {} ({} files)", archive_path.display(), files.len());
            archives_created += 1;
            files_packed += files.len();
        } else {
            if archive_path.exists() {
                continue;
            }

            if let Err(e) = std::fs::create_dir_all(&target_dir) {
                eprintln!("  Error creating {}: {}", target_dir.display(), e);
                errors += 1;
                continue;
            }

            // For per-DAT archives, include set name in the path inside the archive
            let files_with_paths: Vec<(PathBuf, String)> = files
                .iter()
                .map(|(path, rom_name, set_name)| {
                    let inner_path = if let Some(set) = set_name {
                        format!("{}/{}", sanitise_path(set), rom_name)
                    } else {
                        rom_name.clone()
                    };
                    (path.clone(), inner_path)
                })
                .collect();

            match create_archive_from_matches(&archive_path, &files_with_paths, copy) {
                Ok(count) => {
                    println!("  {} ({} files)", archive_path.display(), count);
                    archives_created += 1;
                    files_packed += count;
                }
                Err(e) => {
                    eprintln!("  [ERROR] {}: {}", archive_path.display(), e);
                    errors += 1;
                }
            }
        }
    }

    println!();
    println!("{}:", if dry_run { "Would create" } else { "Created" });
    println!("  Archives: {:>6}", archives_created);
    println!("  Files:    {:>6}", files_packed);
    if errors > 0 {
        println!("  Errors:   {:>6}", errors);
    }

    Ok(())
}

/// Create a ZIP archive from matched files (TorrentZIP compliant)
fn create_archive_from_matches(
    archive_path: &PathBuf,
    files: &[(PathBuf, String)],
    _copy: bool,
) -> Result<usize> {
    use std::io::Write;

    let file = std::fs::File::create(archive_path)?;
    let mut zip = zip::ZipWriter::new(file);

    // TorrentZIP settings: deflate level 9, no extra fields
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .compression_level(Some(9));

    // TorrentZIP requires alphabetically sorted entries
    let mut sorted_files: Vec<_> = files.to_vec();
    sorted_files.sort_by(|a, b| a.1.to_lowercase().cmp(&b.1.to_lowercase()));

    let mut count = 0;
    for (source_path, inner_name) in &sorted_files {
        // Handle archive paths - need to extract the file
        let content = if let Some(hash_pos) = source_path.to_string_lossy().find('#') {
            let archive_path_str = &source_path.to_string_lossy()[..hash_pos];
            let entry_name = &source_path.to_string_lossy()[hash_pos + 1..];
            extract_file_from_archive(&PathBuf::from(archive_path_str), entry_name)?
        } else {
            std::fs::read(source_path)?
        };

        zip.start_file(inner_name, options)?;
        zip.write_all(&content)?;
        count += 1;
    }

    zip.finish()?;
    Ok(count)
}

/// Extract a single file from an archive
fn extract_file_from_archive(archive_path: &PathBuf, entry_name: &str) -> Result<Vec<u8>> {
    let ext = archive_path
        .extension()
        .map(|s| s.to_ascii_lowercase().to_string_lossy().to_string())
        .unwrap_or_default();

    if ext == "zip" {
        let file = std::fs::File::open(archive_path)?;
        let mut archive = zip::ZipArchive::new(std::io::BufReader::new(file))?;
        let mut entry = archive.by_name(entry_name)?;
        let mut content = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut content)?;
        Ok(content)
    } else if ext == "7z" {
        // Extract to temp and read
        let temp_dir = tempfile::tempdir()?;
        sevenz_rust::decompress_file(archive_path, temp_dir.path())?;
        let extracted_path = temp_dir.path().join(entry_name);
        Ok(std::fs::read(extracted_path)?)
    } else {
        Err(anyhow::anyhow!("Unknown archive format"))
    }
}

fn print_organise_summary(
    organised: usize,
    skipped: usize,
    errors: usize,
    dry_run: bool,
    copy: bool,
) {
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

fn cmd_stats(conn: &rusqlite::Connection) -> Result<()> {
    // Get DAT counts
    let dat_count: i64 = conn.query_row("SELECT COUNT(*) FROM dats", [], |row| row.get(0))?;
    let entry_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM dat_entries", [], |row| row.get(0))?;
    let file_count: i64 = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;

    println!("Collection Summary");
    println!("==================");
    println!("  DATs loaded:      {:>8}", dat_count);
    println!("  DAT entries:      {:>8}", entry_count);
    println!("  Files scanned:    {:>8}", file_count);
    println!();

    if dat_count == 0 {
        println!("No DATs loaded. Use `romshelf dat import` or `romshelf dat import-dir` first.");
        return Ok(());
    }

    // Note: We'll show the category tree even without files scanned,
    // as it's useful to see the DAT structure

    // Get per-DAT stats with category
    let mut stmt = conn.prepare(
        "SELECT
            d.name,
            d.category,
            COUNT(DISTINCT de.id) as total_entries,
            COUNT(DISTINCT CASE WHEN f.id IS NOT NULL THEN de.id END) as matched_entries
         FROM dats d
         JOIN dat_versions dv ON d.id = dv.dat_id
         JOIN dat_entries de ON dv.id = de.dat_version_id
         LEFT JOIN files f ON (f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size))
         GROUP BY d.id, d.name, d.category
         ORDER BY d.category, d.name",
    )?;

    let rows: Vec<(String, Option<String>, i64, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })?
        .filter_map(|r| r.ok())
        .collect();

    // Calculate totals
    let total_entries: i64 = rows.iter().map(|(_, _, t, _)| t).sum();
    let total_matched: i64 = rows.iter().map(|(_, _, _, m)| m).sum();

    // Count unmatched files (files not in any DAT)
    let unmatched_files: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files f
         WHERE NOT EXISTS (
             SELECT 1 FROM dat_entries de
             WHERE f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
         )",
        [],
        |row| row.get(0),
    )?;

    let overall_pct = if total_entries > 0 {
        (total_matched as f64 / total_entries as f64) * 100.0
    } else {
        0.0
    };

    println!("Overall Progress");
    println!("----------------");
    println!(
        "  Verified:         {:>8} / {} ({:.1}%)",
        total_matched, total_entries, overall_pct
    );
    println!("  Missing:          {:>8}", total_entries - total_matched);
    println!("  Unmatched files:  {:>8}", unmatched_files);
    println!();

    // Check if we have any categories
    let has_categories = rows.iter().any(|(_, cat, _, _)| cat.is_some());

    if has_categories {
        // Build tree structure from categories
        println!("Category Tree");
        println!("-------------");
        print_category_tree(&rows);
        println!();
    }

    // Show per-DAT breakdown
    println!("Per-DAT Breakdown");
    println!("-----------------");

    // Sort by completeness percentage descending
    let mut sorted_rows = rows.clone();
    sorted_rows.sort_by(|a, b| {
        let pct_a = if a.2 > 0 {
            a.3 as f64 / a.2 as f64
        } else {
            0.0
        };
        let pct_b = if b.2 > 0 {
            b.3 as f64 / b.2 as f64
        } else {
            0.0
        };
        pct_b.partial_cmp(&pct_a).unwrap()
    });

    for (name, _category, total, matched) in &sorted_rows {
        if *matched == 0 && sorted_rows.len() > 20 {
            continue; // Skip empty DATs if we have many
        }

        let pct = if *total > 0 {
            (*matched as f64 / *total as f64) * 100.0
        } else {
            0.0
        };

        let bar = progress_bar(pct, 20);
        println!(
            "  {:50} {:>5}/{:<5} {:>5.1}% {}",
            truncate_string(name, 50),
            matched,
            total,
            pct,
            bar
        );
    }

    // Show count of empty DATs if we skipped them
    let empty_count = sorted_rows.iter().filter(|(_, _, _, m)| *m == 0).count();
    if empty_count > 0 && sorted_rows.len() > 20 {
        println!("  ... and {} DATs with no matches", empty_count);
    }

    Ok(())
}

fn cmd_health(conn: &rusqlite::Connection) -> Result<()> {
    println!("Collection Health Report");
    println!("========================\n");

    // Basic counts
    let dat_count: i64 = conn.query_row("SELECT COUNT(*) FROM dats", [], |row| row.get(0))?;
    let entry_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM dat_entries", [], |row| row.get(0))?;
    let file_count: i64 = conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?;

    if dat_count == 0 {
        println!("No DATs loaded. Run `romshelf dat import` first.");
        return Ok(());
    }

    if file_count == 0 {
        println!("No files scanned. Run `romshelf scan` first.");
        println!("  DATs loaded: {}", dat_count);
        println!("  DAT entries: {}", entry_count);
        return Ok(());
    }

    // Verified files (match by hash AND correct name)
    let verified_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT f.id) FROM files f
         JOIN dat_entries de ON f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
         WHERE LOWER(f.filename) = LOWER(de.name)",
        [],
        |row| row.get(0),
    )?;

    // Misnamed files (match by hash but wrong name)
    let misnamed_count: i64 = conn.query_row(
        "SELECT COUNT(DISTINCT f.id) FROM files f
         JOIN dat_entries de ON f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
         WHERE LOWER(f.filename) != LOWER(de.name)
         AND f.path NOT LIKE '%#%'",
        [],
        |row| row.get(0),
    )?;

    // Unmatched files (no DAT match)
    let unmatched_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files f
         WHERE NOT EXISTS (
             SELECT 1 FROM dat_entries de
             WHERE f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
         )",
        [],
        |row| row.get(0),
    )?;

    // Missing entries (DAT entries with no matching file)
    let missing_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dat_entries de
         WHERE NOT EXISTS (
             SELECT 1 FROM files f
             WHERE f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
         )",
        [],
        |row| row.get(0),
    )?;

    // Duplicate files
    let duplicate_groups: i64 = conn.query_row(
        "SELECT COUNT(*) FROM (
             SELECT sha1 FROM files GROUP BY sha1 HAVING COUNT(*) > 1
         )",
        [],
        |row| row.get(0),
    )?;

    let duplicate_files: i64 = conn.query_row(
        "SELECT COALESCE(SUM(cnt), 0) FROM (
             SELECT COUNT(*) as cnt FROM files GROUP BY sha1 HAVING COUNT(*) > 1
         )",
        [],
        |row| row.get(0),
    )?;

    // Wasted space from duplicates
    let wasted_bytes: i64 = conn.query_row(
        "SELECT COALESCE(SUM(wasted), 0) FROM (
             SELECT (COUNT(*) - 1) * (SUM(size) / COUNT(*)) as wasted
             FROM files GROUP BY sha1 HAVING COUNT(*) > 1
         )",
        [],
        |row| row.get(0),
    )?;

    // DATs with zero matches
    let empty_dats: i64 = conn.query_row(
        "SELECT COUNT(*) FROM dats d
         WHERE NOT EXISTS (
             SELECT 1 FROM dat_versions dv
             JOIN dat_entries de ON dv.id = de.dat_version_id
             JOIN files f ON f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size)
             WHERE dv.dat_id = d.id
         )",
        [],
        |row| row.get(0),
    )?;

    // Calculate percentages
    let verified_pct = (verified_count as f64 / entry_count as f64) * 100.0;
    let missing_pct = (missing_count as f64 / entry_count as f64) * 100.0;

    // Print summary
    println!("Overview");
    println!("--------");
    println!("  DATs loaded:      {:>8}", dat_count);
    println!("  DAT entries:      {:>8}", entry_count);
    println!("  Files scanned:    {:>8}", file_count);
    println!();

    println!("Verification Status");
    println!("-------------------");
    println!(
        "  Verified:         {:>8} ({:.1}%)",
        verified_count, verified_pct
    );
    println!(
        "  Missing:          {:>8} ({:.1}%)",
        missing_count, missing_pct
    );
    println!();

    // Issues section
    let has_issues =
        misnamed_count > 0 || unmatched_count > 0 || duplicate_groups > 0 || empty_dats > 0;

    if has_issues {
        println!("Issues Found");
        println!("------------");

        if misnamed_count > 0 {
            println!(
                "  Misnamed files:   {:>8}  <- Run `romshelf rename --dry-run`",
                misnamed_count
            );
        }

        if unmatched_count > 0 {
            println!(
                "  Unmatched files:  {:>8}  <- Not in any DAT",
                unmatched_count
            );
        }

        if duplicate_groups > 0 {
            println!(
                "  Duplicate groups: {:>8}  ({} files, {} wasted)",
                duplicate_groups,
                duplicate_files,
                format_bytes(wasted_bytes)
            );
            println!("                             <- Run `romshelf duplicates --details`");
        }

        if empty_dats > 0 {
            println!(
                "  Empty DATs:       {:>8}  <- No matching files",
                empty_dats
            );
        }

        println!();
    } else {
        println!("No issues found.\n");
    }

    // Suggested actions
    println!("Suggested Actions");
    println!("-----------------");

    if misnamed_count > 0 {
        println!("  1. Fix misnamed files:  romshelf rename --dry-run");
    }

    if missing_count > 0 && verified_pct < 100.0 {
        println!(
            "  {}. Find missing ROMs:    romshelf verify --issues",
            if misnamed_count > 0 { "2" } else { "1" }
        );
    }

    if duplicate_groups > 0 {
        println!(
            "  {}. Review duplicates:    romshelf duplicates --details",
            if misnamed_count > 0 && missing_count > 0 {
                "3"
            } else if misnamed_count > 0 || missing_count > 0 {
                "2"
            } else {
                "1"
            }
        );
    }

    if !has_issues && verified_pct == 100.0 {
        println!("  Collection is complete and healthy!");
    }

    Ok(())
}

/// Print category tree with rollup statistics
fn print_category_tree(rows: &[(String, Option<String>, i64, i64)]) {
    use std::collections::BTreeMap;

    // Build a tree: path -> (total, matched)
    let mut tree: BTreeMap<String, (i64, i64)> = BTreeMap::new();

    for (_name, category, total, matched) in rows {
        let cat = category.as_deref().unwrap_or("");

        // Add to the full path
        let entry = tree.entry(cat.to_string()).or_insert((0, 0));
        entry.0 += total;
        entry.1 += matched;

        // Also add to all parent paths for rollup
        let parts: Vec<&str> = cat.split('/').filter(|s| !s.is_empty()).collect();
        for i in 0..parts.len() {
            let parent = parts[..i].join("/");
            let entry = tree.entry(parent).or_insert((0, 0));
            entry.0 += total;
            entry.1 += matched;
        }
    }

    // Sort and print
    let mut paths: Vec<_> = tree.into_iter().collect();
    paths.sort_by(|a, b| a.0.cmp(&b.0));

    // Find max depth for calculating column width
    let max_depth = paths
        .iter()
        .map(|(p, _)| {
            if p.is_empty() {
                0
            } else {
                p.matches('/').count() + 1
            }
        })
        .max()
        .unwrap_or(0);

    // Column width: enough for deepest indent + reasonable name length
    let name_col_width = 40 + (max_depth * 2);

    for (path, (total, matched)) in &paths {
        let depth = if path.is_empty() {
            0
        } else {
            path.matches('/').count() + 1
        };
        let indent = "  ".repeat(depth);
        let display_name = if path.is_empty() {
            "(root)".to_string()
        } else {
            path.rsplit('/').next().unwrap_or(path).to_string()
        };

        let pct = if *total > 0 {
            (*matched as f64 / *total as f64) * 100.0
        } else {
            0.0
        };

        // Combine indent and name, then pad to fixed width
        let name_with_indent = format!("{}{}", indent, display_name);
        println!(
            "{:width$} {:>6}/{:<6} {:>5.1}%",
            name_with_indent,
            matched,
            total,
            pct,
            width = name_col_width
        );
    }
}

#[derive(Clone)]
struct CliProgressSink {
    json: bool,
    stderr: Arc<Mutex<()>>,
}

impl CliProgressSink {
    fn new(json: bool) -> Self {
        Self {
            json,
            stderr: Arc::new(Mutex::new(())),
        }
    }

    fn is_json(&self) -> bool {
        self.json
    }

    fn emit_json<T: Serialize>(&self, stream: &str, event: &T) {
        if !self.json {
            return;
        }

        match serde_json::to_string(&json!({ "stream": stream, "event": event })) {
            Ok(line) => {
                let _guard = self.stderr.lock().unwrap();
                eprintln!("{}", line);
            }
            Err(err) => {
                let _guard = self.stderr.lock().unwrap();
                eprintln!("{{\"stream\":\"logger\",\"error\":\"{}\"}}", err);
            }
        }
    }
}

impl ProgressSink<DatImportEvent> for CliProgressSink {
    fn emit(&self, event: DatImportEvent) {
        self.emit_json("dat_import", &event);
    }
}

impl ProgressSink<ScanEvent> for CliProgressSink {
    fn emit(&self, event: ScanEvent) {
        self.emit_json("scan", &event);
    }
}

fn progress_bar(pct: f64, width: usize) -> String {
    let filled = ((pct / 100.0) * width as f64).round() as usize;
    let empty = width.saturating_sub(filled);
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

fn format_eta(secs: f64) -> String {
    if !secs.is_finite() {
        return String::new();
    }
    let mut total = secs.max(0.0).round() as i64;
    let hours = total / 3600;
    total %= 3600;
    let minutes = total / 60;
    let seconds = total % 60;
    if hours > 0 {
        format!("~{}h {}m", hours, minutes)
    } else if minutes > 0 {
        format!("~{}m {}s", minutes, seconds)
    } else {
        format!("~{}s", seconds)
    }
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        format!("{:width$}", s, width = max_len)
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

fn cmd_duplicates(conn: &rusqlite::Connection, show_details: bool) -> Result<()> {
    // Find all SHA1 hashes that appear more than once
    let mut stmt = conn.prepare(
        "SELECT sha1, COUNT(*) as count, SUM(size) as total_size
         FROM files
         GROUP BY sha1
         HAVING COUNT(*) > 1
         ORDER BY total_size DESC",
    )?;

    let duplicates: Vec<(String, i64, i64)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .filter_map(|r| r.ok())
        .collect();

    if duplicates.is_empty() {
        println!("No duplicate files found.");
        return Ok(());
    }

    // Calculate totals
    let total_groups = duplicates.len();
    let total_duplicate_files: i64 = duplicates.iter().map(|(_, count, _)| count).sum();
    let total_wasted_bytes: i64 = duplicates
        .iter()
        .map(|(_, count, size)| {
            // Wasted = (count - 1) * (size / count) = size - size/count
            size - (size / count)
        })
        .sum();

    println!("Duplicate Files Report");
    println!("======================");
    println!();
    println!("Summary:");
    println!("  Duplicate groups:   {:>8}", total_groups);
    println!("  Total duplicates:   {:>8}", total_duplicate_files);
    println!(
        "  Wasted space:       {:>8}",
        format_bytes(total_wasted_bytes)
    );
    println!();

    if show_details {
        // Show each duplicate group with file paths
        let mut path_stmt =
            conn.prepare("SELECT path, size FROM files WHERE sha1 = ?1 ORDER BY path")?;

        for (sha1, count, _) in &duplicates {
            let paths: Vec<(String, i64)> = path_stmt
                .query_map([sha1], |row| Ok((row.get(0)?, row.get(1)?)))?
                .filter_map(|r| r.ok())
                .collect();

            if let Some((_, size)) = paths.first() {
                println!(
                    "[{}] {} copies, {} each:",
                    &sha1[..8],
                    count,
                    format_bytes(*size)
                );
                for (path, _) in &paths {
                    println!("  {}", path);
                }
                println!();
            }
        }
    } else {
        // Show top duplicates by wasted space
        println!("Top duplicates by wasted space (use --details for full list):");
        println!();

        let mut path_stmt =
            conn.prepare("SELECT path FROM files WHERE sha1 = ?1 ORDER BY path LIMIT 1")?;

        for (sha1, count, total_size) in duplicates.iter().take(10) {
            let size_per_file = total_size / count;
            let wasted = total_size - size_per_file;

            // Get first path as example
            let example_path: String = path_stmt
                .query_row([sha1], |row| row.get(0))
                .unwrap_or_else(|_| "?".to_string());

            // Extract just the filename for display
            let filename = std::path::Path::new(&example_path)
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or(example_path.clone());

            println!(
                "  {:40} {:>3} copies  {:>10} wasted",
                truncate_string(&filename, 40),
                count,
                format_bytes(wasted)
            );
        }

        if total_groups > 10 {
            println!("  ... and {} more duplicate groups", total_groups - 10);
        }
    }

    Ok(())
}

/// Format bytes as human-readable string
fn format_bytes(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format bytes as short string (for progress display)
fn format_bytes_short(bytes: i64) -> String {
    const KB: i64 = 1024;
    const MB: i64 = KB * 1024;
    const GB: i64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1}G", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}M", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.0}K", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

/// Rename misnamed files in-place (without restructuring)
fn cmd_rename_in_place(conn: &rusqlite::Connection, dry_run: bool) -> Result<()> {
    // Find misnamed files: files that match a DAT entry by hash but have wrong filename
    // Only consider loose files (not inside archives - those have # in path)
    let mut stmt = conn.prepare(
        "SELECT DISTINCT f.path, f.filename, de.name as correct_name
         FROM files f
         JOIN dat_entries de ON (f.sha1 = de.sha1 OR (f.crc32 = de.crc32 AND f.size = de.size))
         WHERE f.path NOT LIKE '%#%'
           AND LOWER(f.filename) != LOWER(de.name)
         ORDER BY f.path",
    )?;

    let misnamed: Vec<(String, String, String)> = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
        .filter_map(|r| r.ok())
        .collect();

    if misnamed.is_empty() {
        println!("No misnamed files found.");
        return Ok(());
    }

    println!(
        "{}",
        if dry_run {
            "Dry run - showing what would be renamed:"
        } else {
            "Renaming misnamed files..."
        }
    );
    println!();

    let mut renamed = 0;
    let mut skipped = 0;
    let mut errors = 0;

    for (path_str, current_name, correct_name) in &misnamed {
        let path = PathBuf::from(path_str);

        // Build the new path (same directory, new filename)
        let new_path = path
            .parent()
            .map(|p| p.join(correct_name))
            .unwrap_or_else(|| PathBuf::from(correct_name));

        // Check if source exists
        if !path.exists() {
            if dry_run {
                println!("  [MISSING] {}", path.display());
            }
            skipped += 1;
            continue;
        }

        // Check if target already exists
        if new_path.exists() {
            if dry_run {
                println!(
                    "  [EXISTS] {} -> {} (target exists)",
                    current_name, correct_name
                );
            }
            skipped += 1;
            continue;
        }

        if dry_run {
            println!("  {} -> {}", current_name, correct_name);
            renamed += 1;
        } else {
            match std::fs::rename(&path, &new_path) {
                Ok(()) => {
                    println!("  {} -> {}", current_name, correct_name);
                    renamed += 1;

                    // Update the database with the new path
                    let new_path_str = new_path.to_string_lossy().to_string();
                    conn.execute(
                        "UPDATE files SET path = ?1, filename = ?2 WHERE path = ?3",
                        rusqlite::params![new_path_str, correct_name, path_str],
                    )?;
                }
                Err(e) => {
                    eprintln!("  [ERROR] {} -> {}: {}", current_name, correct_name, e);
                    errors += 1;
                }
            }
        }
    }

    println!();
    println!("{}:", if dry_run { "Would rename" } else { "Renamed" });
    println!("  Renamed:    {:>6}", renamed);
    if skipped > 0 {
        println!("  Skipped:    {:>6}", skipped);
    }
    if errors > 0 {
        println!("  Errors:     {:>6}", errors);
    }

    Ok(())
}
