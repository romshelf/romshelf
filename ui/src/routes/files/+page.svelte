<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { onMount } from "svelte";

  interface FileSummary {
    id: number;
    path: string;
    filename: string;
    size: number;
    sha1: string | null;
    matched: boolean;
    match_name: string | null;
  }

  interface DirectorySummary {
    id: number;
    path: string;
    name: string;
    parent_id: number | null;
    file_count: number;
    matched_count: number;
    total_size: number;
    child_count: number;
  }

  // Root directories (top level)
  let rootDirs = $state<DirectorySummary[]>([]);

  // Caches for loaded data (keyed by directory ID)
  let childCache = $state<Map<number, DirectorySummary[]>>(new Map());
  let fileCache = $state<Map<number, FileSummary[]>>(new Map());

  // Loading states
  let loadingChildren = $state<Set<number>>(new Set());
  let loadingFiles = $state<Set<number>>(new Set());

  // Expanded directories
  let expandedDirs = $state<Set<number>>(new Set());

  let error = $state<string | null>(null);
  let loading = $state(true);

  // Compute total stats from root directories
  let totalFiles = $derived(rootDirs.reduce((sum, d) => sum + d.file_count, 0));
  let totalMatched = $derived(rootDirs.reduce((sum, d) => sum + d.matched_count, 0));

  onMount(async () => {
    try {
      rootDirs = await invoke<DirectorySummary[]>("get_root_directories");
      // Auto-expand root directories if there's only one
      if (rootDirs.length === 1) {
        await toggleExpand(rootDirs[0]);
      }
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  });

  async function toggleExpand(dir: DirectorySummary) {
    const dirId = dir.id;

    if (expandedDirs.has(dirId)) {
      // Collapse
      expandedDirs.delete(dirId);
      expandedDirs = new Set(expandedDirs);
    } else {
      // Expand - load children if not cached
      expandedDirs.add(dirId);
      expandedDirs = new Set(expandedDirs);

      // Load child directories if needed
      if (dir.child_count > 0 && !childCache.has(dirId)) {
        loadingChildren.add(dirId);
        loadingChildren = new Set(loadingChildren);
        try {
          const children = await invoke<DirectorySummary[]>("get_child_directories", { parentId: dirId });
          childCache.set(dirId, children);
          childCache = new Map(childCache);
        } catch (e) {
          console.error("Failed to load children:", e);
        } finally {
          loadingChildren.delete(dirId);
          loadingChildren = new Set(loadingChildren);
        }
      }

      // Load files in this directory if needed
      if (!fileCache.has(dirId)) {
        loadingFiles.add(dirId);
        loadingFiles = new Set(loadingFiles);
        try {
          const files = await invoke<FileSummary[]>("get_files_in_directory", { dirId: dirId });
          fileCache.set(dirId, files);
          fileCache = new Map(fileCache);
        } catch (e) {
          console.error("Failed to load files:", e);
        } finally {
          loadingFiles.delete(dirId);
          loadingFiles = new Set(loadingFiles);
        }
      }
    }
  }

  function formatBytes(bytes: number): string {
    if (bytes >= 1_000_000_000) {
      return `${(bytes / 1_000_000_000).toFixed(1)} GB`;
    } else if (bytes >= 1_000_000) {
      return `${(bytes / 1_000_000).toFixed(1)} MB`;
    } else if (bytes >= 1_000) {
      return `${(bytes / 1_000).toFixed(1)} KB`;
    }
    return `${bytes} B`;
  }

  function formatNumber(n: number): string {
    return n.toLocaleString();
  }
</script>

{#snippet directoryNode(dir: DirectorySummary, depth: number)}
  {@const expanded = expandedDirs.has(dir.id)}
  {@const children = childCache.get(dir.id) ?? []}
  {@const files = fileCache.get(dir.id) ?? []}
  {@const isLoadingChildren = loadingChildren.has(dir.id)}
  {@const isLoadingFiles = loadingFiles.has(dir.id)}

  <div class="tree-item" style="--depth: {depth}">
    <button class="tree-toggle" onclick={() => toggleExpand(dir)}>
      <span class="chevron">{expanded ? "▼" : "▶"}</span>
      <span class="folder-icon">📁</span>
      <span class="node-name">{dir.name}</span>
      <span class="node-stats">
        <span class="node-matched">{formatNumber(dir.matched_count)}</span>
        <span class="node-separator">/</span>
        <span class="node-total">{formatNumber(dir.file_count)}</span>
      </span>
    </button>
  </div>

  {#if expanded}
    {#if isLoadingChildren || isLoadingFiles}
      <div class="tree-item loading-item" style="--depth: {depth + 1}">
        <span class="loading-spinner"></span>
        Loading...
      </div>
    {/if}

    {#each children as child (child.id)}
      {@render directoryNode(child, depth + 1)}
    {/each}

    {#each files as file (file.id)}
      <div class="tree-item file-item" class:matched={file.matched} style="--depth: {depth + 1}">
        <span class="file-icon">{file.matched ? "✓" : "?"}</span>
        <span class="file-name">{file.filename}</span>
        <span class="file-meta">
          {#if file.match_name}
            <span class="match-name">{file.match_name}</span>
          {/if}
          <span class="file-size">{formatBytes(file.size)}</span>
        </span>
      </div>
    {/each}
  {/if}
{/snippet}

<div class="page">
  <div class="header">
    <h1>Files</h1>
    {#if rootDirs.length > 0}
      <span class="count">
        {formatNumber(totalMatched)} matched / {formatNumber(totalFiles)} total
      </span>
    {/if}
  </div>

  {#if loading}
    <div class="loading">Loading...</div>
  {:else if error}
    <div class="error">
      <p>Failed to load files: {error}</p>
    </div>
  {:else if rootDirs.length === 0}
    <div class="empty-state">
      <h2>No Files Scanned</h2>
      <p>Scan your ROM collection using the CLI:</p>
      <code>romshelf scan /path/to/roms</code>
    </div>
  {:else}
    <div class="tree-container">
      {#each rootDirs as dir (dir.id)}
        {@render directoryNode(dir, 0)}
      {/each}
    </div>
  {/if}
</div>

<style>
  .page {
    max-width: 1200px;
  }

  .header {
    display: flex;
    align-items: baseline;
    gap: 12px;
    margin-bottom: 24px;
  }

  h1 {
    font-size: 24px;
    font-weight: 600;
    color: #fff;
  }

  .count {
    font-size: 14px;
    color: #888;
  }

  .loading {
    color: #888;
    padding: 40px;
    text-align: center;
  }

  .error {
    background-color: #3d1f1f;
    border: 1px solid #5c2828;
    border-radius: 8px;
    padding: 16px;
    color: #f87171;
  }

  .empty-state {
    background-color: #1e1e3f;
    border: 1px solid #2a2a4a;
    border-radius: 12px;
    padding: 24px;
  }

  .empty-state h2 {
    font-size: 18px;
    font-weight: 600;
    color: #fff;
    margin-bottom: 8px;
  }

  .empty-state p {
    color: #888;
    margin-bottom: 12px;
  }

  .empty-state code {
    display: block;
    background-color: #16162a;
    border: 1px solid #2a2a4a;
    border-radius: 6px;
    padding: 12px;
    font-family: "SF Mono", "Fira Code", monospace;
    font-size: 13px;
    color: #4ade80;
  }

  .tree-container {
    background-color: #1e1e3f;
    border: 1px solid #2a2a4a;
    border-radius: 12px;
    padding: 8px 0;
    overflow: hidden;
  }

  .tree-item {
    padding: 6px 16px;
    padding-left: calc(16px + var(--depth) * 20px);
    display: flex;
    align-items: center;
    gap: 8px;
  }

  .tree-toggle {
    display: flex;
    align-items: center;
    gap: 8px;
    background: none;
    border: none;
    color: #fff;
    cursor: pointer;
    padding: 4px 8px;
    margin: -4px -8px;
    border-radius: 4px;
    width: 100%;
    text-align: left;
    font-size: 14px;
  }

  .tree-toggle:hover {
    background-color: #2a2a4a;
  }

  .chevron {
    font-size: 10px;
    color: #666;
    width: 12px;
    transition: transform 0.15s ease;
  }

  .folder-icon {
    font-size: 14px;
  }

  .node-name {
    flex: 1;
  }

  .node-stats {
    font-size: 12px;
    background-color: #2a2a4a;
    padding: 2px 8px;
    border-radius: 4px;
  }

  .node-matched {
    color: #4ade80;
  }

  .node-separator {
    color: #555;
  }

  .node-total {
    color: #888;
  }

  .loading-item {
    color: #888;
    font-size: 13px;
    font-style: italic;
  }

  .loading-spinner {
    display: inline-block;
    width: 12px;
    height: 12px;
    border: 2px solid #444;
    border-top-color: #4ade80;
    border-radius: 50%;
    animation: spin 0.8s linear infinite;
    margin-right: 8px;
  }

  @keyframes spin {
    to {
      transform: rotate(360deg);
    }
  }

  .file-item {
    color: #ccc;
    font-size: 13px;
  }

  .file-item:hover {
    background-color: #2a2a4a;
  }

  .file-item.matched {
    background-color: rgba(74, 222, 128, 0.05);
  }

  .file-icon {
    font-size: 12px;
    margin-left: 20px;
    width: 14px;
    text-align: center;
  }

  .file-item.matched .file-icon {
    color: #4ade80;
  }

  .file-item:not(.matched) .file-icon {
    color: #666;
  }

  .file-name {
    flex: 1;
    color: #aaa;
  }

  .file-item.matched .file-name {
    color: #ccc;
  }

  .file-meta {
    display: flex;
    gap: 12px;
    font-size: 11px;
    color: #666;
  }

  .match-name {
    font-family: "SF Mono", "Fira Code", monospace;
    color: #888;
    max-width: 200px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .file-size {
    font-family: "SF Mono", "Fira Code", monospace;
    color: #666;
  }
</style>
