//! ROMShelf GUI - Tauri commands exposing core functionality
//!
//! This module provides thin wrappers around romshelf-core functions.
//! All business logic and database queries live in the core library.

use romshelf_core::db::{
    self, CollectionStats, DatSummary, DatTreeNode, DirectorySummary, FileSummary, FileTreeNode,
};

/// Get collection statistics
#[tauri::command]
fn get_stats() -> Result<CollectionStats, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::get_collection_stats(&conn).map_err(|e| e.to_string())
}

/// List all loaded DATs with summary info
#[tauri::command]
fn list_dats() -> Result<Vec<DatSummary>, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::list_dats(&conn).map_err(|e| e.to_string())
}

/// Get DATs as a tree structure based on category hierarchy
#[tauri::command]
fn get_dat_tree() -> Result<DatTreeNode, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::get_dat_tree(&conn).map_err(|e| e.to_string())
}

/// List scanned files with match status
#[tauri::command]
fn list_files(limit: i64, offset: i64) -> Result<Vec<FileSummary>, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::list_files(&conn, limit, offset).map_err(|e| e.to_string())
}

/// Get files as a tree structure based on filesystem paths
#[tauri::command]
fn get_file_tree() -> Result<FileTreeNode, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::get_file_tree(&conn).map_err(|e| e.to_string())
}

// ============================================================================
// Lazy loading directory API (scales to millions of files)
// ============================================================================

/// Get root directories for lazy tree loading
#[tauri::command]
fn get_root_directories() -> Result<Vec<DirectorySummary>, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::get_root_directories(&conn).map_err(|e| e.to_string())
}

/// Get child directories of a parent directory
#[tauri::command]
fn get_child_directories(parent_id: i64) -> Result<Vec<DirectorySummary>, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::get_child_directories(&conn, parent_id).map_err(|e| e.to_string())
}

/// Get files directly in a directory (not recursive)
#[tauri::command]
fn get_files_in_directory(dir_id: i64) -> Result<Vec<FileSummary>, String> {
    let conn = db::open_db().map_err(|e| e.to_string())?;
    db::get_files_in_directory(&conn, dir_id).map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_opener::init())
        .invoke_handler(tauri::generate_handler![
            get_stats,
            list_dats,
            get_dat_tree,
            list_files,
            get_file_tree,
            get_root_directories,
            get_child_directories,
            get_files_in_directory
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
