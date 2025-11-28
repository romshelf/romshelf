//! Verification module - matching files to DAT entries

use crate::dat::DatEntry;
use crate::scan::ScannedFile;

/// Result of verification
#[derive(Debug)]
pub struct VerifyResult {
    pub verified: Vec<Match>,
    pub misnamed: Vec<Match>,
    pub missing: Vec<DatEntry>,
    pub unmatched: Vec<ScannedFile>,
}

/// A match between a file and a DAT entry
#[derive(Debug, Clone)]
pub struct Match {
    pub file: ScannedFile,
    pub entry: DatEntry,
}

/// Verify scanned files against DAT entries
pub fn verify(files: &[ScannedFile], entries: &[DatEntry]) -> VerifyResult {
    let mut verified = Vec::new();
    let mut misnamed = Vec::new();
    let mut unmatched = Vec::new();
    let mut matched_entry_indices: Vec<bool> = vec![false; entries.len()];

    // For each file, try to find a matching DAT entry
    for file in files {
        if let Some((idx, entry)) = find_match(file, entries) {
            matched_entry_indices[idx] = true;

            let name_correct = is_name_correct(&file.filename, &entry.rom_name);
            let m = Match {
                file: file.clone(),
                entry: entry.clone(),
            };

            if name_correct {
                verified.push(m);
            } else {
                misnamed.push(m);
            }
        } else {
            unmatched.push(file.clone());
        }
    }

    // Find missing entries (entries without matching files)
    let missing: Vec<DatEntry> = entries
        .iter()
        .enumerate()
        .filter(|(idx, _)| !matched_entry_indices[*idx])
        .map(|(_, e)| e.clone())
        .collect();

    VerifyResult {
        verified,
        misnamed,
        missing,
        unmatched,
    }
}

/// Find a matching DAT entry for a file by hash
fn find_match<'a>(file: &ScannedFile, entries: &'a [DatEntry]) -> Option<(usize, &'a DatEntry)> {
    for (idx, entry) in entries.iter().enumerate() {
        // First try SHA1 (most reliable)
        if let Some(ref sha1) = entry.sha1 {
            if sha1 == &file.sha1 {
                return Some((idx, entry));
            }
        }

        // Fall back to CRC32 + size
        if let Some(ref crc32) = entry.crc32 {
            if crc32 == &file.crc32 && entry.size == file.size {
                return Some((idx, entry));
            }
        }

        // Fall back to MD5
        if let Some(ref md5) = entry.md5 {
            if md5 == &file.md5 {
                return Some((idx, entry));
            }
        }
    }

    None
}

/// Check if the filename matches the expected ROM name
fn is_name_correct(filename: &str, rom_name: &str) -> bool {
    // Simple case-insensitive comparison
    // Could be more sophisticated (ignore extension, etc.)
    filename.to_lowercase() == rom_name.to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_file(filename: &str, crc32: &str, sha1: &str) -> ScannedFile {
        ScannedFile {
            path: filename.into(),
            filename: filename.to_string(),
            size: 1024,
            crc32: crc32.to_string(),
            md5: "md5hash".to_string(),
            sha1: sha1.to_string(),
        }
    }

    fn make_entry(name: &str, rom_name: &str, crc32: &str, sha1: &str) -> DatEntry {
        DatEntry {
            name: name.to_string(),
            rom_name: rom_name.to_string(),
            size: 1024,
            crc32: Some(crc32.to_string()),
            md5: None,
            sha1: Some(sha1.to_string()),
        }
    }

    #[test]
    fn test_verified_match() {
        let files = vec![make_file("game.rom", "abcd1234", "sha1hash")];
        let entries = vec![make_entry("Game", "game.rom", "abcd1234", "sha1hash")];

        let result = verify(&files, &entries);

        assert_eq!(result.verified.len(), 1);
        assert_eq!(result.misnamed.len(), 0);
        assert_eq!(result.missing.len(), 0);
        assert_eq!(result.unmatched.len(), 0);
    }

    #[test]
    fn test_misnamed_match() {
        let files = vec![make_file("wrong_name.rom", "abcd1234", "sha1hash")];
        let entries = vec![make_entry("Game", "correct_name.rom", "abcd1234", "sha1hash")];

        let result = verify(&files, &entries);

        assert_eq!(result.verified.len(), 0);
        assert_eq!(result.misnamed.len(), 1);
        assert_eq!(result.missing.len(), 0);
        assert_eq!(result.unmatched.len(), 0);
    }

    #[test]
    fn test_missing_entry() {
        let files: Vec<ScannedFile> = vec![];
        let entries = vec![make_entry("Game", "game.rom", "abcd1234", "sha1hash")];

        let result = verify(&files, &entries);

        assert_eq!(result.verified.len(), 0);
        assert_eq!(result.misnamed.len(), 0);
        assert_eq!(result.missing.len(), 1);
        assert_eq!(result.unmatched.len(), 0);
    }

    #[test]
    fn test_unmatched_file() {
        let files = vec![make_file("unknown.rom", "ffffffff", "unknown")];
        let entries = vec![make_entry("Game", "game.rom", "abcd1234", "sha1hash")];

        let result = verify(&files, &entries);

        assert_eq!(result.verified.len(), 0);
        assert_eq!(result.misnamed.len(), 0);
        assert_eq!(result.missing.len(), 1);
        assert_eq!(result.unmatched.len(), 1);
    }
}
