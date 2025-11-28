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
    pub entries: Vec<DatEntry>,
}

/// A single entry from a DAT file
#[derive(Debug, Clone)]
pub struct DatEntry {
    pub name: String,
    pub rom_name: String,
    pub size: u64,
    pub crc32: Option<String>,
    pub md5: Option<String>,
    pub sha1: Option<String>,
}

/// Parse a DAT file (Logiqx XML format)
pub fn parse_dat(path: &Path) -> Result<ParsedDat> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read DAT file: {}", path.display()))?;

    parse_logiqx_xml(&content)
}

/// Parse Logiqx XML format (used by TOSEC, No-Intro, Redump)
fn parse_logiqx_xml(xml: &str) -> Result<ParsedDat> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut dat = ParsedDat {
        name: String::new(),
        version: None,
        entries: Vec::new(),
    };

    let mut buf = Vec::new();
    let mut current_path: Vec<String> = Vec::new();
    let mut current_game_name: Option<String> = None;
    let mut in_header = false;
    let mut current_text_target: Option<&str> = None;

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                current_path.push(tag_name.clone());

                match tag_name.as_str() {
                    "header" => in_header = true,
                    "name" if in_header => current_text_target = Some("name"),
                    "version" if in_header => current_text_target = Some("version"),
                    "game" | "machine" => {
                        // Get name attribute
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"name" {
                                current_game_name =
                                    Some(String::from_utf8_lossy(&attr.value).to_string());
                            }
                        }
                    }
                    "rom" => {
                        // Parse ROM element attributes
                        let mut entry = DatEntry {
                            name: current_game_name.clone().unwrap_or_default(),
                            rom_name: String::new(),
                            size: 0,
                            crc32: None,
                            md5: None,
                            sha1: None,
                        };

                        for attr in e.attributes().flatten() {
                            let key = String::from_utf8_lossy(attr.key.as_ref());
                            let value = String::from_utf8_lossy(&attr.value).to_string();

                            match key.as_ref() {
                                "name" => entry.rom_name = value,
                                "size" => entry.size = value.parse().unwrap_or(0),
                                "crc" => entry.crc32 = Some(value.to_lowercase()),
                                "md5" => entry.md5 = Some(value.to_lowercase()),
                                "sha1" => entry.sha1 = Some(value.to_lowercase()),
                                _ => {}
                            }
                        }

                        dat.entries.push(entry);
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                match tag_name.as_str() {
                    "header" => in_header = false,
                    "game" | "machine" => current_game_name = None,
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
                        "version" => dat.version = Some(text),
                        _ => {}
                    }
                }
            }
            Ok(Event::Empty(e)) => {
                // Handle self-closing <rom /> elements
                let tag_name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if tag_name == "rom" {
                    let mut entry = DatEntry {
                        name: current_game_name.clone().unwrap_or_default(),
                        rom_name: String::new(),
                        size: 0,
                        crc32: None,
                        md5: None,
                        sha1: None,
                    };

                    for attr in e.attributes().flatten() {
                        let key = String::from_utf8_lossy(attr.key.as_ref());
                        let value = String::from_utf8_lossy(&attr.value).to_string();

                        match key.as_ref() {
                            "name" => entry.rom_name = value,
                            "size" => entry.size = value.parse().unwrap_or(0),
                            "crc" => entry.crc32 = Some(value.to_lowercase()),
                            "md5" => entry.md5 = Some(value.to_lowercase()),
                            "sha1" => entry.sha1 = Some(value.to_lowercase()),
                            _ => {}
                        }
                    }

                    dat.entries.push(entry);
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

    Ok(dat)
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
        assert_eq!(dat.entries.len(), 1);
        assert_eq!(dat.entries[0].name, "Test Game");
        assert_eq!(dat.entries[0].rom_name, "test.rom");
        assert_eq!(dat.entries[0].size, 1024);
        assert_eq!(dat.entries[0].crc32, Some("abcd1234".to_string()));
    }

    #[test]
    fn test_parse_multiple_entries() {
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
        assert_eq!(dat.entries.len(), 2);
        assert_eq!(dat.entries[0].name, "Game 1");
        assert_eq!(dat.entries[1].name, "Game 2");
    }
}
