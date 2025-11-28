//! File scanning module - directory walking, hashing, archive support

use anyhow::{Context, Result};
use crc32fast::Hasher as Crc32Hasher;
use md5::{Digest, Md5};
use sha1::Sha1;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;
use zip::ZipArchive;

/// A scanned file with computed hashes
#[derive(Debug, Clone)]
pub struct ScannedFile {
    pub path: PathBuf,
    pub filename: String,
    pub size: u64,
    pub crc32: String,
    pub md5: String,
    pub sha1: String,
}

/// Scan a directory and hash all files (including contents of ZIP archives)
pub fn scan_directory(path: &Path) -> Result<Vec<ScannedFile>> {
    let mut files = Vec::new();
    let mut archives_scanned = 0;

    for entry in WalkDir::new(path).follow_links(true) {
        let entry = entry.with_context(|| format!("Failed to read directory entry in {}", path.display()))?;

        if entry.file_type().is_file() {
            let file_path = entry.path();

            // Check if it's a ZIP archive
            if is_zip_file(file_path) {
                match scan_zip_archive(file_path) {
                    Ok(mut archive_files) => {
                        archives_scanned += 1;
                        files.append(&mut archive_files);
                    }
                    Err(e) => {
                        eprintln!("Warning: Failed to scan archive {}: {}", file_path.display(), e);
                    }
                }
            } else {
                // Regular file
                match hash_file(file_path) {
                    Ok(scanned) => files.push(scanned),
                    Err(e) => {
                        eprintln!("Warning: Failed to hash {}: {}", file_path.display(), e);
                    }
                }
            }
        }
    }

    if archives_scanned > 0 {
        eprintln!("  Archives scanned: {}", archives_scanned);
    }

    Ok(files)
}

/// Check if a file is a ZIP archive based on extension
fn is_zip_file(path: &Path) -> bool {
    path.extension()
        .map(|ext| ext.to_ascii_lowercase() == "zip")
        .unwrap_or(false)
}

/// Scan contents of a ZIP archive
fn scan_zip_archive(archive_path: &Path) -> Result<Vec<ScannedFile>> {
    let file = File::open(archive_path)
        .with_context(|| format!("Failed to open archive: {}", archive_path.display()))?;
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
            crc32: hashes.0,
            md5: hashes.1,
            sha1: hashes.2,
        });
    }

    Ok(files)
}

/// Hash a single file, computing CRC32, MD5, and SHA1 in a single read
pub fn hash_file(path: &Path) -> Result<ScannedFile> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open file: {}", path.display()))?;
    let metadata = file.metadata()?;
    let mut reader = BufReader::new(file);

    let (crc32, md5, sha1) = hash_reader(&mut reader)?;

    Ok(ScannedFile {
        path: path.to_path_buf(),
        filename: path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default(),
        size: metadata.len(),
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

    let mut buffer = [0u8; 8192];
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
}
