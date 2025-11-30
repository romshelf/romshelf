//! DAT parsing module - TOSEC, No-Intro, MAME format support

use anyhow::{Context, Result};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use std::fs;
use std::path::Path;

/// A parsed DAT file
#[derive(Debug)]
pub struct ParsedDat {
    pub name: String,
    pub version: Option<String>,
    pub sets: Vec<DatSet>,
}

impl ParsedDat {
    /// Total number of ROM entries across all sets
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

/// Parse a DAT file (Logiqx XML format)
pub fn parse_dat(path: &Path) -> Result<ParsedDat> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read DAT file: {}", path.display()))?;

    // Strip UTF-8 BOM if present
    let content = content.strip_prefix('\u{feff}').unwrap_or(&content);

    parse_logiqx_xml(content)
}

/// Compute SHA1 hash of a DAT file for duplicate detection
pub fn hash_dat_file(path: &Path) -> Result<String> {
    use sha1::{Digest, Sha1};
    use std::io::Read;

    let mut file = fs::File::open(path)
        .with_context(|| format!("Failed to open DAT file: {}", path.display()))?;

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

/// Parse Logiqx XML format (used by TOSEC, No-Intro, Redump)
fn parse_logiqx_xml(xml: &str) -> Result<ParsedDat> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut dat = ParsedDat {
        name: String::new(),
        version: None,
        sets: Vec::new(),
    };

    let mut buf = Vec::new();
    let mut current_path: Vec<String> = Vec::new();
    let mut current_set: Option<DatSet> = None;
    let mut in_header = false;
    let mut current_text_target: Option<&str> = None;
    let mut header_description: Option<String> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                current_path.push(tag_name.clone());

                match tag_name.as_str() {
                    "header" => in_header = true,
                    "name" if in_header => current_text_target = Some("name"),
                    "description" if in_header => current_text_target = Some("description"),
                    "version" if in_header => current_text_target = Some("version"),
                    "game" | "machine" | "software" => {
                        // Start a new set - get name attribute
                        let mut set_name = String::new();
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"name" {
                                set_name = String::from_utf8_lossy(&attr.value).to_string();
                            }
                        }
                        current_set = Some(DatSet {
                            name: set_name,
                            roms: Vec::new(),
                        });
                    }
                    "rom" => {
                        // Parse ROM element attributes and add to current set
                        let entry = parse_rom_attributes(&e);
                        if let Some(ref mut set) = current_set {
                            set.roms.push(entry);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                match tag_name.as_str() {
                    "header" => in_header = false,
                    "game" | "machine" | "software" => {
                        // End of set - push it if it has ROMs
                        if let Some(set) = current_set.take() {
                            if !set.roms.is_empty() {
                                dat.sets.push(set);
                            }
                        }
                    }
                    _ => {}
                }

                current_text_target = None;
                current_path.pop();
            }
            Ok(Event::Text(e)) => {
                if let Some(target) = current_text_target {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match target {
                        "name" => dat.name = text,
                        "description" => header_description = Some(text),
                        "version" => dat.version = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                // Handle self-closing <rom /> elements
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag_name == "rom" {
                    let entry = parse_rom_attributes(&e);
                    if let Some(ref mut set) = current_set {
                        set.roms.push(entry);
                    }
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Error parsing XML at position {}: {:?}",
                    reader.error_position(),
                    e
                ))
            }
            _ => {}
        }
        buf.clear();
    }

    // Prefer description over name for MAME Software Lists (which have cryptic names like "a2600")
    // but only if description is more informative (longer than the name)
    if let Some(desc) = header_description {
        if desc.len() > dat.name.len() {
            dat.name = desc;
        }
    }

    Ok(dat)
}

/// Parse ROM attributes from an XML element
fn parse_rom_attributes(e: &quick_xml::events::BytesStart) -> DatEntry {
    let mut entry = DatEntry {
        name: String::new(),
        size: 0,
        crc32: None,
        md5: None,
        sha1: None,
    };

    for attr in e.attributes().flatten() {
        let key = String::from_utf8_lossy(attr.key.as_ref());
        let value = String::from_utf8_lossy(&attr.value).to_string();

        match key.as_ref() {
            "name" => entry.name = value,
            "size" => entry.size = value.parse().unwrap_or(0),
            "crc" => entry.crc32 = Some(value.to_lowercase()),
            "md5" => entry.md5 = Some(value.to_lowercase()),
            "sha1" => entry.sha1 = Some(value.to_lowercase()),
            _ => {}
        }
    }

    entry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_dat() {
        let xml = r#"<?xml version="1.0"?>
<datafile>
  <header>
    <name>Test DAT</name>
    <version>2025-01-30</version>
  </header>
  <game name="Test Game">
    <rom name="test.rom" size="1024" crc="abcd1234" md5="1234567890abcdef" sha1="abc123"/>
  </game>
</datafile>"#;

        let dat = parse_logiqx_xml(xml).unwrap();
        assert_eq!(dat.name, "Test DAT");
        assert_eq!(dat.version, Some("2025-01-30".to_string()));
        assert_eq!(dat.sets.len(), 1);
        assert_eq!(dat.sets[0].name, "Test Game");
        assert_eq!(dat.sets[0].roms.len(), 1);
        assert_eq!(dat.sets[0].roms[0].name, "test.rom");
        assert_eq!(dat.sets[0].roms[0].size, 1024);
        assert_eq!(dat.sets[0].roms[0].crc32, Some("abcd1234".to_string()));
    }

    #[test]
    fn test_parse_multiple_sets() {
        let xml = r#"<?xml version="1.0"?>
<datafile>
  <header>
    <name>Multi Test</name>
  </header>
  <game name="Game 1">
    <rom name="game1.rom" size="100" crc="11111111"/>
  </game>
  <game name="Game 2">
    <rom name="game2.rom" size="200" crc="22222222"/>
  </game>
</datafile>"#;

        let dat = parse_logiqx_xml(xml).unwrap();
        assert_eq!(dat.sets.len(), 2);
        assert_eq!(dat.sets[0].name, "Game 1");
        assert_eq!(dat.sets[1].name, "Game 2");
        assert_eq!(dat.entry_count(), 2);
    }

    #[test]
    fn test_parse_multi_rom_set() {
        let xml = r#"<?xml version="1.0"?>
<datafile>
  <header>
    <name>Multi-ROM Test</name>
  </header>
  <game name="Multi Disk Game">
    <rom name="disk1.adf" size="901120" crc="11111111"/>
    <rom name="disk2.adf" size="901120" crc="22222222"/>
    <rom name="disk3.adf" size="901120" crc="33333333"/>
  </game>
</datafile>"#;

        let dat = parse_logiqx_xml(xml).unwrap();
        assert_eq!(dat.sets.len(), 1);
        assert_eq!(dat.sets[0].name, "Multi Disk Game");
        assert_eq!(dat.sets[0].roms.len(), 3);
        assert_eq!(dat.sets[0].roms[0].name, "disk1.adf");
        assert_eq!(dat.sets[0].roms[1].name, "disk2.adf");
        assert_eq!(dat.sets[0].roms[2].name, "disk3.adf");
        assert_eq!(dat.entry_count(), 3);
    }
}
