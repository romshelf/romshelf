use crate::dat::{self, DatEntry, DatHeader, DatSetInfo, DatVisitor};
use crate::services::progress::{DatImportEvent, ProgressSink};
use crate::tosec;
use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use rusqlite::{Connection, Transaction, params};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// Options controlling how a DAT import behaves
#[derive(Default, Clone)]
pub struct DatImportOptions {
    /// Optional category path (e.g. "Commodore/Amiga/Games/[ADF]")
    pub category: Option<String>,
    /// Optional hint that helps derive tree paths for TOSEC packs
    pub category_root: Option<PathBuf>,
}

/// Outcome of an import
#[derive(Debug, Clone)]
pub enum DatImportOutcome {
    Imported {
        dat_id: i64,
        entry_count: u64,
        name: String,
        entries_per_sec: f64,
    },
    Duplicate {
        name: String,
    },
    Unchanged {
        name: String,
    },
}

/// Summary returned after an import attempt
#[derive(Debug, Clone)]
pub struct DatImportResult {
    pub outcome: DatImportOutcome,
    pub duration: Duration,
}

pub struct DatImporter<'conn, S: ProgressSink<DatImportEvent> = ()> {
    conn: &'conn mut Connection,
    sink: S,
}

impl<'conn, S: ProgressSink<DatImportEvent>> DatImporter<'conn, S> {
    pub fn new(conn: &'conn mut Connection, sink: S) -> Self {
        Self { conn, sink }
    }

    pub fn import_path<F>(
        &mut self,
        path: &Path,
        options: DatImportOptions,
        mut on_event: F,
    ) -> Result<DatImportResult>
    where
        F: FnMut(DatImportEvent),
    {
        let started = DatImportEvent::Started {
            path: path.to_path_buf(),
        };
        on_event(started.clone());
        self.sink.emit(started);

        let metadata = std::fs::metadata(path)
            .with_context(|| format!("Unable to read metadata for DAT file: {}", path.display()))?;
        let file_size = metadata.len() as i64;
        let file_mtime = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64);

        let path_str = path.to_string_lossy().to_string();

        if let Some((name, existing_size, existing_mtime)) =
            self.lookup_existing_by_path(&path_str)?
            && Some(existing_size) == Some(file_size)
            && existing_mtime == file_mtime
        {
            let event = DatImportEvent::Skipped {
                reason: format!("Unchanged DAT: {}", name),
            };
            on_event(event.clone());
            self.sink.emit(event);
            return Ok(DatImportResult {
                outcome: DatImportOutcome::Unchanged { name },
                duration: Duration::from_secs(0),
            });
        }

        let file_sha1 = dat::hash_dat_file(path)?;
        if let Some(name) = self.lookup_existing_by_hash(&file_sha1)? {
            let event = DatImportEvent::Skipped {
                reason: format!("Duplicate DAT: {}", name),
            };
            on_event(event.clone());
            self.sink.emit(event);
            return Ok(DatImportResult {
                outcome: DatImportOutcome::Duplicate { name },
                duration: Duration::from_secs(0),
            });
        }

        let effective_category = options
            .category
            .clone()
            .or_else(|| derive_category(path, options.category_root.as_deref()));

        let start_time = std::time::Instant::now();
        let tx = self.conn.transaction()?;
        let mut context = ImportContext::new(
            tx,
            path,
            file_sha1,
            file_size,
            file_mtime,
            effective_category,
            &mut on_event,
            &self.sink,
        );

        dat::parse_dat_streaming(path, &mut context)?;
        let result = context.finish()?;
        let duration = start_time.elapsed();
        let entries_per_sec = if result.entry_count > 0 && duration.as_secs_f64() > 0.0 {
            result.entry_count as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        let completed = DatImportEvent::Completed {
            name: result.name.clone(),
            entry_count: result.entry_count,
            duration_ms: duration.as_millis(),
            entries_per_sec,
        };
        on_event(completed.clone());
        self.sink.emit(completed);
        Ok(DatImportResult {
            outcome: DatImportOutcome::Imported {
                dat_id: result.dat_id,
                entry_count: result.entry_count,
                name: result.name,
                entries_per_sec,
            },
            duration,
        })
    }

    fn lookup_existing_by_path(&self, path: &str) -> Result<Option<(String, i64, Option<i64>)>> {
        self.conn
            .query_row(
                "SELECT name, file_size, file_mtime FROM dats WHERE file_path = ?1",
                [path],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
    }

    fn lookup_existing_by_hash(&self, sha1: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT name FROM dats WHERE file_sha1 = ?1",
                [sha1],
                |row| row.get(0),
            )
            .optional()
    }
}

trait OptionalRow<T> {
    fn optional(self) -> Result<Option<T>>;
}

impl<T> OptionalRow<T> for Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

struct ImportContext<'conn, 'cb, S: ProgressSink<DatImportEvent>, F: FnMut(DatImportEvent) + 'cb> {
    tx: Transaction<'conn>,
    file_path: PathBuf,
    file_sha1: String,
    file_size: i64,
    file_mtime: Option<i64>,
    category: Option<String>,
    on_event: &'cb mut F,
    sink: &'cb S,
    dat_id: Option<i64>,
    dat_version_id: Option<i64>,
    current_set_id: Option<i64>,
    total_sets: u64,
    total_entries: u64,
    dat_name: Option<String>,
}

struct ImportSummary {
    dat_id: i64,
    entry_count: u64,
    name: String,
}

impl<'conn, 'cb, S: ProgressSink<DatImportEvent>, F: FnMut(DatImportEvent)>
    ImportContext<'conn, 'cb, S, F>
{
    #[allow(clippy::too_many_arguments)]
    fn new(
        tx: Transaction<'conn>,
        file_path: &Path,
        file_sha1: String,
        file_size: i64,
        file_mtime: Option<i64>,
        category: Option<String>,
        on_event: &'cb mut F,
        sink: &'cb S,
    ) -> Self {
        Self {
            tx,
            file_path: file_path.to_path_buf(),
            file_sha1,
            file_size,
            file_mtime,
            category,
            on_event,
            sink,
            dat_id: None,
            dat_version_id: None,
            current_set_id: None,
            total_sets: 0,
            total_entries: 0,
            dat_name: None,
        }
    }

    fn finish(self) -> Result<ImportSummary> {
        let dat_version_id = self
            .dat_version_id
            .ok_or_else(|| anyhow!("DAT version was not created"))?;
        self.tx.execute(
            "UPDATE dat_versions SET entry_count = ?1 WHERE id = ?2",
            params![self.total_entries as i64, dat_version_id],
        )?;

        let dat_id = self.dat_id.ok_or_else(|| anyhow!("DAT not created"))?;
        let name = self.dat_name.unwrap_or_else(|| "Unknown".to_string());
        self.tx.commit()?;
        Ok(ImportSummary {
            dat_id,
            entry_count: self.total_entries,
            name,
        })
    }

    fn insert_dat(&mut self, header: &DatHeader) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.tx.execute(
            "INSERT INTO dats (name, format, file_path, file_sha1, file_size, file_mtime, category)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                header.name,
                header.format.to_string(),
                self.file_path.to_string_lossy(),
                self.file_sha1,
                self.file_size,
                self.file_mtime,
                self.category,
            ],
        )?;
        let dat_id = self.tx.last_insert_rowid();
        let version_id = {
            self.tx.execute(
                "INSERT INTO dat_versions (dat_id, version, loaded_at, entry_count)
                 VALUES (?1, ?2, ?3, 0)",
                params![dat_id, header.version, now],
            )?;
            self.tx.last_insert_rowid()
        };
        self.dat_id = Some(dat_id);
        self.dat_version_id = Some(version_id);
        self.dat_name = Some(header.name.clone());
        Ok(())
    }

    fn insert_set(&mut self, name: &str) -> Result<()> {
        let dat_version_id = self
            .dat_version_id
            .ok_or_else(|| anyhow!("DAT version not initialised before set"))?;
        self.tx.execute(
            "INSERT INTO sets (dat_version_id, name) VALUES (?1, ?2)",
            params![dat_version_id, name],
        )?;
        self.current_set_id = Some(self.tx.last_insert_rowid());
        self.total_sets += 1;
        Ok(())
    }

    fn insert_rom(&mut self, entry: &DatEntry) -> Result<()> {
        let dat_version_id = self
            .dat_version_id
            .ok_or_else(|| anyhow!("DAT version not initialised before ROM"))?;
        self.tx.execute(
            "INSERT INTO dat_entries (dat_version_id, set_id, name, size, crc32, md5, sha1)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                dat_version_id,
                self.current_set_id,
                entry.name,
                entry.size as i64,
                entry.crc32,
                entry.md5,
                entry.sha1,
            ],
        )?;
        self.total_entries += 1;
        Ok(())
    }
}

impl<'conn, 'cb, S: ProgressSink<DatImportEvent>, F: FnMut(DatImportEvent)> DatVisitor
    for ImportContext<'conn, 'cb, S, F>
{
    fn dat_start(&mut self, header: &DatHeader) -> Result<()> {
        let event = DatImportEvent::DatDetected {
            name: header.name.clone(),
            format: header.format.to_string(),
        };
        (self.on_event)(event.clone());
        self.sink.emit(event);
        self.insert_dat(header)
    }

    fn dat_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_start(&mut self, set: &DatSetInfo) -> Result<()> {
        self.insert_set(&set.name)?;
        let event = DatImportEvent::SetStarted {
            name: set.name.clone(),
            index: self.total_sets,
        };
        (self.on_event)(event.clone());
        self.sink.emit(event);
        Ok(())
    }

    fn set_end(&mut self, _set: &DatSetInfo) -> Result<()> {
        self.current_set_id = None;
        Ok(())
    }

    fn rom(&mut self, entry: &DatEntry) -> Result<()> {
        self.insert_rom(entry)?;
        #[allow(clippy::manual_is_multiple_of)]
        if self.total_entries % 1000 == 0 {
            let event = DatImportEvent::RomProgress {
                total_entries: self.total_entries,
            };
            (self.on_event)(event.clone());
            self.sink.emit(event);
        }
        Ok(())
    }
}

fn derive_category(path: &Path, prefix: Option<&Path>) -> Option<String> {
    let filename = path.file_name()?.to_str()?;
    if let Some(category) = tosec::parse_tosec_category(filename) {
        if let Some(root) = prefix
            && let Ok(rel) = path.strip_prefix(root)
        {
            let rel_str = rel
                .parent()
                .map(|p| clean_category_segments(&p.to_string_lossy()))
                .filter(|s| !s.is_empty());
            if let Some(rel_str) = rel_str {
                return Some(format!("{}/{}", rel_str, category));
            }
        }
        return Some(category);
    }

    prefix.and_then(|root| {
        path.strip_prefix(root)
            .ok()
            .and_then(|rel| rel.parent())
            .map(|p| clean_category_segments(&p.to_string_lossy()))
            .filter(|s| !s.is_empty())
    })
}

fn clean_category_segments(value: &str) -> String {
    value
        .trim_matches(|c| c == '/' || c == '\\')
        .trim()
        .to_string()
}
