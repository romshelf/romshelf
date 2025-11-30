//! File scanning module - directory walking, hashing, archive support, parallelism

use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Sender};
use crc32fast::Hasher as Crc32Hasher;
use md5::{Digest, Md5};
use rayon::prelude::*;
use sha1::Sha1;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use walkdir::WalkDir;
use zip::ZipArchive;

/// A scanned file with computed hashes
#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub path: PathBuf,
    pub filename: String,
    pub size: u64,
    pub mtime: Option<i64>,
    pub crc32: String,
    pub md5: String,
    pub sha1: String,
}

/// A file that was skipped during scanning
#[derive(Debug, Clone)]
pub struct SkippedFile {
    pub path: PathBuf,
    pub reason: String,
}

/// Result of a scan operation
#[derive(Debug)]
pub struct ScanResult {
    pub files: Vec<ScannedFile>,
    pub skipped: Vec<SkippedFile>,
    pub zip_archives: u64,
    pub sevenz_archives: u64,
    pub duration: Duration,
}

/// Progress tracking for scans
pub struct ScanProgress {
    pub discovered: AtomicU64,
    pub processed: AtomicU64,
    pub archives_opened: AtomicU64,
    pub start_time: Instant,
    /// Current file being processed (for verbose output)
    current_file: std::sync::Mutex<Option<String>>,
}

impl ScanProgress {
    pub fn new() -> Self {
        Self {
            discovered: AtomicU64::new(0),
            processed: AtomicU64::new(0),
            archives_opened: AtomicU64::new(0),
            start_time: Instant::now(),
            current_file: std::sync::Mutex::new(None),
        }
    }

    pub fn files_per_sec(&self) -> f32 {
        let elapsed = self.start_time.elapsed().as_secs_f32();
        if elapsed > 0.0 {
            self.processed.load(Ordering::Relaxed) as f32 / elapsed
        } else {
            0.0
        }
    }

    /// Set the current file being processed
    pub fn set_current(&self, path: &Path) {
        if let Ok(mut current) = self.current_file.lock() {
            *current = Some(path.to_string_lossy().to_string());
        }
    }

    /// Get the current file being processed
    pub fn get_current(&self) -> Option<String> {
        self.current_file.lock().ok().and_then(|c| c.clone())
    }
}

impl Default for ScanProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Work item for the scanning queue
enum WorkItem {
    File(PathBuf),
    ZipArchive(PathBuf),
    SevenZArchive(PathBuf),
}

/// Scan a directory with parallel processing
pub fn scan_directory_parallel(
    path: &Path,
    threads: usize,
    progress: Arc<ScanProgress>,
) -> Result<ScanResult> {
    let start_time = Instant::now();
    let (sender, receiver) = bounded::<WorkItem>(1000);

    // Shared state for results
    let skipped = Arc::new(std::sync::Mutex::new(Vec::new()));
    let zip_count = Arc::new(AtomicU64::new(0));
    let sevenz_count = Arc::new(AtomicU64::new(0));

    // Clone for discovery thread
    let progress_discovery = Arc::clone(&progress);
    let path_owned = path.to_path_buf();

    // Discovery thread - walks directories and pushes work items
    let discovery_handle = std::thread::spawn(move || {
        discover_files(&path_owned, sender, &progress_discovery)
    });

    // Process work items in parallel
    let skipped_clone = Arc::clone(&skipped);
    let zip_count_clone = Arc::clone(&zip_count);
    let sevenz_count_clone = Arc::clone(&sevenz_count);
    let progress_clone = Arc::clone(&progress);

    // Configure thread pool
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .unwrap();

    let files: Vec<ScannedFile> = pool.install(|| {
        receiver
            .into_iter()
            .par_bridge()
            .flat_map(|item| {
                let result = process_work_item(
                    item,
                    &skipped_clone,
                    &zip_count_clone,
                    &sevenz_count_clone,
                    &progress_clone,
                );
                progress_clone.processed.fetch_add(1, Ordering::Relaxed);
                result
            })
            .collect()
    });

    // Wait for discovery to finish
    discovery_handle.join().unwrap()?;

    let duration = start_time.elapsed();

    // Extract skipped files - use lock() if Arc still has other refs
    let skipped_files = match Arc::try_unwrap(skipped) {
        Ok(mutex) => mutex.into_inner().unwrap(),
        Err(arc) => arc.lock().unwrap().clone(),
    };

    Ok(ScanResult {
        files,
        skipped: skipped_files,
        zip_archives: zip_count.load(Ordering::Relaxed),
        sevenz_archives: sevenz_count.load(Ordering::Relaxed),
        duration,
    })
}

/// Discover files and push work items to queue
fn discover_files(
    path: &Path,
    sender: Sender<WorkItem>,
    progress: &ScanProgress,
) -> Result<()> {
    for entry in WalkDir::new(path).follow_links(true) {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                eprintln!("Warning: {}", e);
                continue;
            }
        };

        if entry.file_type().is_file() {
            let file_path = entry.path().to_path_buf();
            let item = if is_zip_file(&file_path) {
                WorkItem::ZipArchive(file_path)
            } else if is_7z_file(&file_path) {
                WorkItem::SevenZArchive(file_path)
            } else {
                WorkItem::File(file_path)
            };

            progress.discovered.fetch_add(1, Ordering::Relaxed);

            if sender.send(item).is_err() {
                break; // Receiver dropped, stop discovery
            }
        }
    }
    Ok(())
}

/// Process a single work item
fn process_work_item(
    item: WorkItem,
    skipped: &Arc<std::sync::Mutex<Vec<SkippedFile>>>,
    zip_count: &Arc<AtomicU64>,
    sevenz_count: &Arc<AtomicU64>,
    progress: &Arc<ScanProgress>,
) -> Vec<ScannedFile> {
    match item {
        WorkItem::File(ref path) => {
            progress.set_current(path);
            match hash_file(path) {
                Ok(f) => vec![f],
                Err(e) => {
                    skipped.lock().unwrap().push(SkippedFile {
                        path: path.clone(),
                        reason: e.to_string(),
                    });
                    vec![]
                }
            }
        }
        WorkItem::ZipArchive(ref path) => {
            progress.set_current(path);
            zip_count.fetch_add(1, Ordering::Relaxed);
            match scan_zip_archive(path) {
                Ok(files) => files,
                Err(e) => {
                    skipped.lock().unwrap().push(SkippedFile {
                        path: path.clone(),
                        reason: format!("ZIP error: {}", e),
                    });
                    vec![]
                }
            }
        }
        WorkItem::SevenZArchive(ref path) => {
            progress.set_current(path);
            sevenz_count.fetch_add(1, Ordering::Relaxed);
            match scan_7z_archive(path) {
                Ok(files) => files,
                Err(e) => {
                    skipped.lock().unwrap().push(SkippedFile {
                        path: path.clone(),
                        reason: format!("7z error: {}", e),
                    });
                    vec![]
                }
            }
        }
    }
}

/// Legacy single-threaded scan (for compatibility)
pub fn scan_directory(path: &Path) -> Result<Vec<ScannedFile>> {
    let progress = Arc::new(ScanProgress::new());
    let result = scan_directory_parallel(path, 1, progress)?;
    Ok(result.files)
}

/// Check if a file is a ZIP archive based on extension
fn is_zip_file(path: &Path) -> bool {
    path.extension()
        .map(|ext| ext.eq_ignore_ascii_case("zip"))
        .unwrap_or(false)
}

/// Check if a file is a 7z archive based on extension
fn is_7z_file(path: &Path) -> bool {
    path.extension()
        .map(|ext| ext.eq_ignore_ascii_case("7z"))
        .unwrap_or(false)
}

/// Scan contents of a ZIP archive
fn scan_zip_archive(archive_path: &Path) -> Result<Vec<ScannedFile>> {
    let file = File::open(archive_path)
        .with_context(|| format!("Failed to open archive: {}", archive_path.display()))?;

    // Get archive mtime for entries (they don't have their own reliable mtime)
    let archive_mtime = file
        .metadata()
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64);

    let mut archive = ZipArchive::new(BufReader::new(file))
        .with_context(|| format!("Failed to read ZIP archive: {}", archive_path.display()))?;

    let mut files = Vec::new();

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;

        // Skip directories
        if entry.is_dir() {
            continue;
        }

        let entry_name = entry.name().to_string();
        let entry_size = entry.size();

        // Hash the entry contents
        let hashes = hash_reader(&mut entry)?;

        // Path format: archive_path#entry_name (so we know where it came from)
        let virtual_path = PathBuf::from(format!(
            "{}#{}",
            archive_path.display(),
            entry_name
        ));

        // For matching purposes, use just the entry filename
        let filename = Path::new(&entry_name)
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or(entry_name.clone());

        files.push(ScannedFile {
            path: virtual_path,
            filename,
            size: entry_size,
            mtime: archive_mtime,
            crc32: hashes.0,
            md5: hashes.1,
            sha1: hashes.2,
        });
    }

    Ok(files)
}

/// Scan contents of a 7z archive
fn scan_7z_archive(archive_path: &Path) -> Result<Vec<ScannedFile>> {
    // Get archive mtime for entries
    let archive_mtime = std::fs::metadata(archive_path)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64);

    // 7z requires extraction - use temp directory
    let temp_dir = tempfile::tempdir()
        .with_context(|| "Failed to create temp directory for 7z extraction")?;

    sevenz_rust::decompress_file(archive_path, temp_dir.path())
        .with_context(|| format!("Failed to extract 7z archive: {}", archive_path.display()))?;

    let mut files = Vec::new();

    for entry in WalkDir::new(temp_dir.path()) {
        let entry = entry?;
        if entry.file_type().is_file() {
            let mut scanned = hash_file(entry.path())?;

            // Convert temp path to virtual archive path
            let relative = entry
                .path()
                .strip_prefix(temp_dir.path())
                .unwrap_or(entry.path());

            scanned.path = PathBuf::from(format!(
                "{}#{}",
                archive_path.display(),
                relative.display()
            ));

            // Update filename to just the file part
            scanned.filename = entry
                .path()
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();

            // Use archive mtime for entries (extracted files have temp mtime)
            scanned.mtime = archive_mtime;

            files.push(scanned);
        }
    }

    // temp_dir is automatically cleaned up when dropped
    Ok(files)
}

/// Hash a single file, computing CRC32, MD5, and SHA1 in a single read
pub fn hash_file(path: &Path) -> Result<ScannedFile> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open file: {}", path.display()))?;
    let metadata = file.metadata()?;
    let mut reader = BufReader::new(file);

    let (crc32, md5, sha1) = hash_reader(&mut reader)?;

    // Get mtime as Unix timestamp
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64);

    Ok(ScannedFile {
        path: path.to_path_buf(),
        filename: path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default(),
        size: metadata.len(),
        mtime,
        crc32,
        md5,
        sha1,
    })
}

/// Hash content from a reader, returning (crc32, md5, sha1)
fn hash_reader<R: Read>(reader: &mut R) -> Result<(String, String, String)> {
    let mut crc = Crc32Hasher::new();
    let mut md5 = Md5::new();
    let mut sha1 = Sha1::new();

    let mut buffer = [0u8; 65536]; // Larger buffer for better throughput
    loop {
        let bytes_read = reader.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        crc.update(&buffer[..bytes_read]);
        md5.update(&buffer[..bytes_read]);
        sha1.update(&buffer[..bytes_read]);
    }

    Ok((
        format!("{:08x}", crc.finalize()),
        format!("{:x}", md5.finalize()),
        format!("{:x}", sha1.finalize()),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_hash_known_content() {
        // Create a temp file with known content
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();

        let scanned = hash_file(file.path()).unwrap();

        assert_eq!(scanned.size, 12);
        // These are the known hashes for "test content"
        assert_eq!(scanned.crc32, "57f4675d");
        assert_eq!(scanned.md5, "9473fdd0d880a43c21b7778d34872157");
        assert_eq!(
            scanned.sha1,
            "1eebdf4fdc9fc7bf283031b93f9aef3338de9052"
        );
    }

    #[test]
    fn test_hash_empty_file() {
        let file = NamedTempFile::new().unwrap();
        let scanned = hash_file(file.path()).unwrap();

        assert_eq!(scanned.size, 0);
        // Known hashes for empty content
        assert_eq!(scanned.crc32, "00000000");
        assert_eq!(scanned.md5, "d41d8cd98f00b204e9800998ecf8427e");
        assert_eq!(scanned.sha1, "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn test_is_zip_file() {
        assert!(is_zip_file(Path::new("game.zip")));
        assert!(is_zip_file(Path::new("game.ZIP")));
        assert!(!is_zip_file(Path::new("game.adf")));
        assert!(!is_zip_file(Path::new("game")));
    }

    #[test]
    fn test_is_7z_file() {
        assert!(is_7z_file(Path::new("game.7z")));
        assert!(is_7z_file(Path::new("game.7Z")));
        assert!(!is_7z_file(Path::new("game.zip")));
        assert!(!is_7z_file(Path::new("game")));
    }
}
