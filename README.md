# Romshelf

> Renamed from **Bitshelf** after discovering the GitHub organisation name was already taken.

A ROM collection manager with DAT-driven verification and organisation.

## Features

- Import DAT files (Logiqx XML format - TOSEC, No-Intro, Redump, MAME)
- Multi-threaded file scanning with hash calculation (CRC32, MD5, SHA1)
- Support for loose files, ZIP archives, and 7z archives
- Verify your collection against loaded DATs
- Organise matched files into a structured directory
- Category tree view showing collection completeness

## Installation

```bash
cargo install --path .
```

## Usage

### Import DATs

Import a single DAT file:
```bash
romshelf dat import /path/to/dat-file.dat
```

Import all DATs from a directory (recursive):
```bash
romshelf dat import-dir /path/to/dats/
```

List imported DATs:
```bash
romshelf dat list
```

### Scan ROMs

Scan a directory for ROM files:
```bash
romshelf scan /path/to/roms/
```

Use multiple threads:
```bash
romshelf scan /path/to/roms/ --threads 8
```

### Verify Collection

Check your scanned files against loaded DATs:
```bash
romshelf verify
```

Show detailed issues (misnamed files, unmatched files):
```bash
romshelf verify --issues
```

### Organise Collection

Move matched files into a structured directory:
```bash
romshelf organise --target /path/to/organised/
```

Preview what would happen without making changes:
```bash
romshelf organise --target /path/to/organised/ --dry-run
```

Copy instead of moving:
```bash
romshelf organise --target /path/to/organised/ --copy
```

### View Statistics

Show collection overview with category tree:
```bash
romshelf stats
```

## Category Organisation

The stats command displays a category tree showing your collection completeness at each level of the hierarchy. Categories are determined in two ways:

### Directory-based categories

If you organise your DAT files into subdirectories before importing, the directory structure becomes the category hierarchy:

```
/path/to/dats/
  TOSEC/
    Commodore Amiga - Games - [ADF] (TOSEC-v2025).dat
    GCE Vectrex - Games (TOSEC-v2023).dat
  No-Intro/
    Nintendo - Game Boy.dat
  Redump/
    Sony - PlayStation.dat
```

Running `romshelf dat import-dir /path/to/dats/` produces categories:
- `TOSEC/Commodore/Amiga/Games/[ADF]`
- `TOSEC/GCE/Vectrex/Games`
- `No-Intro`
- `Redump`

### TOSEC filename parsing

For flat TOSEC DAT packs (all files in one directory), Romshelf automatically parses the TOSEC naming convention to extract categories:

- `Commodore Amiga - Games - [ADF] (TOSEC-v2025).dat` → `Commodore/Amiga/Games/[ADF]`
- `GCE Vectrex - Demos - Music (TOSEC-v2023).dat` → `GCE/Vectrex/Demos/Music`

This mapping is derived from the official TOSEC move scripts and covers 472 manufacturer/model combinations.

### Recommended setup

For the best organisation, create top-level folders for each DAT source:

```
/dats/
  TOSEC/
    (flat TOSEC DAT files here)
  No-Intro/
    (No-Intro DAT files here)
  Redump/
    (Redump DAT files here)
```

Then import with:
```bash
romshelf dat import-dir /dats/
```

The category tree will show:
```
Category Tree
-------------
(root)                                    0/100000   0.0%
  No-Intro                                0/50000    0.0%
    Nintendo - Game Boy                   0/1500     0.0%
    ...
  Redump                                  0/30000    0.0%
    Sony - PlayStation                    0/5000     0.0%
    ...
  TOSEC                                   0/20000    0.0%
    Commodore                             0/15000    0.0%
      Amiga                               0/12000    0.0%
        Games                             0/10000    0.0%
          [ADF]                           0/8000     0.0%
    ...
```

## Database

Romshelf stores its database at `~/.romshelf/romshelf.db` (SQLite).

## Supported DAT Formats

- Logiqx XML (used by TOSEC, No-Intro, Redump)
- MAME XML (game/machine/software elements)

## Supported Archive Formats

- ZIP (.zip)
- 7-Zip (.7z)
