use serde::Serialize;
use std::path::PathBuf;

/// Events emitted while importing DAT files
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum DatImportEvent {
    Started {
        path: PathBuf,
    },
    DatDetected {
        name: String,
        format: String,
    },
    SetStarted {
        name: String,
        index: u64,
    },
    RomProgress {
        total_entries: u64,
    },
    Completed {
        name: String,
        entry_count: u64,
        duration_ms: u128,
        entries_per_sec: f64,
    },
    Skipped {
        reason: String,
    },
}

/// Events emitted during scanning
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ScanEvent {
    Discovery {
        directory: PathBuf,
    },
    FileStarted {
        path: PathBuf,
        size: u64,
    },
    FileProgress {
        path: PathBuf,
        bytes_done: u64,
        bytes_total: u64,
    },
    FileCompleted {
        path: PathBuf,
        size: u64,
    },
    Summary {
        discovered_files: u64,
        processed_files: u64,
        total_bytes: u64,
        duration_ms: u128,
        files_per_sec: f64,
        bytes_per_sec: f64,
    },
}

pub trait ProgressSink<E>: Send + Sync + 'static {
    fn emit(&self, event: E);
}

impl<E> ProgressSink<E> for ()
where
    E: Send,
{
    fn emit(&self, _event: E) {}
}

impl<E, F> ProgressSink<E> for F
where
    E: Send,
    F: Fn(E) + Send + Sync + 'static,
{
    fn emit(&self, event: E) {
        (self)(event);
    }
}
