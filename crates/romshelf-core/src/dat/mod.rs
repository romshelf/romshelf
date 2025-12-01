//! DAT parsing module - streaming parser with visitor support (TOSEC, No-Intro, MAME, etc.)

use anyhow::{Context, Result, anyhow};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use sha1::{Digest, Sha1};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

/// A parsed DAT file (legacy API)
#[derive(Debug, Default)]
pub struct ParsedDat {
    pub name: String,
    pub version: Option<String>,
    pub sets: Vec<DatSet>,
}

impl ParsedDat {
    pub fn entry_count(&self) -> usize {
        self.sets.iter().map(|s| s.roms.len()).sum()
    }
}

/// A set (game, application, etc.) containing one or more ROMs
#[derive(Debug, Clone)]
pub struct DatSet {
    pub name: String,
    pub roms: Vec<DatEntry>,
}

/// A single ROM entry within a set
#[derive(Debug, Clone)]
pub struct DatEntry {
    pub name: String,
    pub size: u64,
    pub crc32: Option<String>,
    pub md5: Option<String>,
    pub sha1: Option<String>,
}

/// Metadata emitted at the start of a DAT
#[derive(Debug, Clone)]
pub struct DatHeader {
    pub name: String,
    pub description: Option<String>,
    pub version: Option<String>,
    pub format: DatFormat,
}

/// Information about the current set being parsed
#[derive(Debug, Clone)]
pub struct DatSetInfo {
    pub name: String,
}

/// Supported DAT formats (best-effort detection)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatFormat {
    Tosec,
    NoIntro,
    Redump,
    Mame,
    ClrMamePro,
    Unknown,
}

impl Display for DatFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DatFormat::Tosec => write!(f, "TOSEC"),
            DatFormat::NoIntro => write!(f, "No-Intro"),
            DatFormat::Redump => write!(f, "Redump"),
            DatFormat::Mame => write!(f, "MAME"),
            DatFormat::ClrMamePro => write!(f, "ClrMamePro"),
            DatFormat::Unknown => write!(f, "Unknown"),
        }
    }
}

impl DatFormat {
    pub fn from_path(path: &Path) -> Self {
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_lowercase();

        if filename.contains("tosec") {
            DatFormat::Tosec
        } else if filename.contains("no-intro") {
            DatFormat::NoIntro
        } else if filename.contains("redump") {
            DatFormat::Redump
        } else if filename.contains("mame") || filename.contains("softwarelist") {
            DatFormat::Mame
        } else if filename.contains("clrmame") {
            DatFormat::ClrMamePro
        } else {
            DatFormat::Unknown
        }
    }
}

/// Visitor invoked during streaming parsing
pub trait DatVisitor {
    fn dat_start(&mut self, _header: &DatHeader) -> Result<()> {
        Ok(())
    }

    fn dat_end(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_start(&mut self, _set: &DatSetInfo) -> Result<()> {
        Ok(())
    }

    fn set_end(&mut self, _set: &DatSetInfo) -> Result<()> {
        Ok(())
    }

    fn rom(&mut self, _entry: &DatEntry) -> Result<()> {
        Ok(())
    }
}

/// Parse a DAT file (legacy, materialises entire structure)
pub fn parse_dat(path: &Path) -> Result<ParsedDat> {
    let mut collector = CollectingVisitor::default();
    parse_dat_streaming(path, &mut collector)?;
    Ok(collector.into_dat())
}

/// Stream a DAT file into a visitor
pub fn parse_dat_streaming(path: &Path, visitor: &mut impl DatVisitor) -> Result<()> {
    let file =
        File::open(path).with_context(|| format!("Failed to open DAT file: {}", path.display()))?;
    let reader = Reader::from_reader(BufReader::new(file));
    parse_logiqx(reader, path, visitor)
}

/// Compute SHA1 hash of a DAT file for duplicate detection
pub fn hash_dat_file(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("Failed to open DAT file: {}", path.display()))?;

    let mut hasher = Sha1::new();
    let mut buffer = [0u8; 65536];

    loop {
        let bytes_read = file.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn parse_logiqx<R: BufRead>(
    mut reader: Reader<R>,
    path: &Path,
    visitor: &mut impl DatVisitor,
) -> Result<()> {
    let mut buf = Vec::new();
    let mut current_set: Option<DatSetInfo> = None;
    let mut in_header = false;
    let mut current_text_target: Option<&str> = None;
    let mut header_description: Option<String> = None;
    let mut dat_name = String::new();
    let mut dat_version: Option<String> = None;
    let mut dat_started = false;
    let format = DatFormat::from_path(path);

    fn emit_header(
        dat_started: &mut bool,
        dat_name: &mut String,
        dat_version: &Option<String>,
        header_description: &mut Option<String>,
        format: DatFormat,
        visitor: &mut impl DatVisitor,
        path: &Path,
    ) -> Result<()> {
        if *dat_started {
            return Ok(());
        }

        if dat_name.is_empty() {
            dat_name.push_str(
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("Unnamed DAT"),
            );
        }

        if let Some(desc) = header_description.clone()
            && desc.len() > dat_name.len()
        {
            *dat_name = desc;
        }

        let header = DatHeader {
            name: dat_name.clone(),
            description: header_description.clone(),
            version: dat_version.clone(),
            format,
        };
        visitor.dat_start(&header)?;
        *dat_started = true;
        Ok(())
    }

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                match tag_name.as_str() {
                    "header" => in_header = true,
                    "name" if in_header => current_text_target = Some("name"),
                    "description" if in_header => current_text_target = Some("description"),
                    "version" if in_header => current_text_target = Some("version"),
                    "game" | "machine" | "software" => {
                        emit_header(
                            &mut dat_started,
                            &mut dat_name,
                            &dat_version,
                            &mut header_description,
                            format,
                            visitor,
                            path,
                        )?;

                        let mut set_name = String::new();
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"name" {
                                set_name = String::from_utf8_lossy(&attr.value).to_string();
                            }
                        }
                        let set = DatSetInfo { name: set_name };
                        visitor.set_start(&set)?;
                        current_set = Some(set);
                    }
                    "rom" => {
                        emit_header(
                            &mut dat_started,
                            &mut dat_name,
                            &dat_version,
                            &mut header_description,
                            format,
                            visitor,
                            path,
                        )?;
                        let entry = parse_rom_attributes(&e);
                        visitor.rom(&entry)?;
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                match tag_name.as_str() {
                    "header" => {
                        in_header = false;
                        emit_header(
                            &mut dat_started,
                            &mut dat_name,
                            &dat_version,
                            &mut header_description,
                            format,
                            visitor,
                            path,
                        )?;
                    }
                    "game" | "machine" | "software" => {
                        if let Some(set) = current_set.take() {
                            visitor.set_end(&set)?;
                        }
                    }
                    _ => {}
                }

                current_text_target = None;
            }
            Ok(Event::Text(e)) => {
                if let Some(target) = current_text_target {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match target {
                        "name" => dat_name = text,
                        "description" => header_description = Some(text),
                        "version" => dat_version = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag_name == "rom" {
                    emit_header(
                        &mut dat_started,
                        &mut dat_name,
                        &dat_version,
                        &mut header_description,
                        format,
                        visitor,
                        path,
                    )?;
                    let entry = parse_rom_attributes(&e);
                    visitor.rom(&entry)?;
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(anyhow!(
                    "Error parsing XML at position {}: {:?}",
                    reader.error_position(),
                    e
                ));
            }
            _ => {}
        }
        buf.clear();
    }

    if !dat_started {
        emit_header(
            &mut dat_started,
            &mut dat_name,
            &dat_version,
            &mut header_description,
            format,
            visitor,
            path,
        )?;
    }

    visitor.dat_end()?;
    Ok(())
}

fn parse_rom_attributes(e: &quick_xml::events::BytesStart) -> DatEntry {
    let mut entry = DatEntry {
        name: String::new(),
        size: 0,
        crc32: None,
        md5: None,
        sha1: None,
    };

    for attr in e.attributes().flatten() {
        let key = attr.key.as_ref();
        let value = String::from_utf8_lossy(&attr.value).to_string();

        match key {
            b"name" => entry.name = value,
            b"size" => entry.size = value.parse().unwrap_or(0),
            b"crc" => entry.crc32 = Some(value),
            b"md5" => entry.md5 = Some(value),
            b"sha1" => entry.sha1 = Some(value),
            _ => {}
        }
    }

    entry
}

#[derive(Default)]
struct CollectingVisitor {
    dat: ParsedDat,
    current_set: Option<DatSet>,
}

impl CollectingVisitor {
    fn into_dat(mut self) -> ParsedDat {
        if let Some(set) = self.current_set.take() {
            self.dat.sets.push(set);
        }
        self.dat
    }
}

impl DatVisitor for CollectingVisitor {
    fn dat_start(&mut self, header: &DatHeader) -> Result<()> {
        self.dat.name = header.name.clone();
        self.dat.version = header.version.clone();
        Ok(())
    }

    fn set_start(&mut self, set: &DatSetInfo) -> Result<()> {
        if let Some(prev) = self.current_set.take() {
            self.dat.sets.push(prev);
        }
        self.current_set = Some(DatSet {
            name: set.name.clone(),
            roms: Vec::new(),
        });
        Ok(())
    }

    fn set_end(&mut self, _set: &DatSetInfo) -> Result<()> {
        if let Some(prev) = self.current_set.take() {
            self.dat.sets.push(prev);
        }
        Ok(())
    }

    fn rom(&mut self, entry: &DatEntry) -> Result<()> {
        if let Some(ref mut set) = self.current_set {
            set.roms.push(entry.clone());
        } else {
            self.current_set = Some(DatSet {
                name: "Default".to_string(),
                roms: vec![entry.clone()],
            });
        }
        Ok(())
    }
}
