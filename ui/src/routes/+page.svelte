<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { onMount } from "svelte";

  interface Stats {
    dat_count: number;
    entry_count: number;
    scanned_files: number;
    matched_files: number;
    total_bytes_scanned: number;
  }

  let stats = $state<Stats | null>(null);
  let error = $state<string | null>(null);
  let loading = $state(true);

  onMount(async () => {
    try {
      stats = await invoke<Stats>("get_stats");
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  });

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

<div class="page">
  <h1>Dashboard</h1>

  {#if loading}
    <div class="loading">Loading...</div>
  {:else if error}
    <div class="error">
      <p>Failed to load stats: {error}</p>
    </div>
  {:else if stats}
    <div class="stats-grid">
      <div class="stat-card">
        <div class="stat-value">{formatNumber(stats.dat_count)}</div>
        <div class="stat-label">DATs Loaded</div>
      </div>

      <div class="stat-card">
        <div class="stat-value">{formatNumber(stats.entry_count)}</div>
        <div class="stat-label">DAT Entries</div>
      </div>

      <div class="stat-card">
        <div class="stat-value">{formatNumber(stats.scanned_files)}</div>
        <div class="stat-label">Files Scanned</div>
      </div>

      <div class="stat-card highlight">
        <div class="stat-value">{formatNumber(stats.matched_files)}</div>
        <div class="stat-label">Files Matched</div>
        {#if stats.scanned_files > 0}
          <div class="stat-pct">
            {((stats.matched_files / stats.scanned_files) * 100).toFixed(1)}% of scanned
          </div>
        {/if}
      </div>

      <div class="stat-card wide">
        <div class="stat-value">{formatBytes(stats.total_bytes_scanned)}</div>
        <div class="stat-label">Total Data Scanned</div>
      </div>
    </div>

    {#if stats.dat_count === 0}
      <div class="empty-state">
        <h2>Get Started</h2>
        <p>No DATs loaded yet. Use the CLI to import DAT files:</p>
        <code>romshelf dat import-dir /path/to/dats --prefix TOSEC</code>
      </div>
    {:else if stats.scanned_files === 0}
      <div class="empty-state">
        <h2>Scan Your Collection</h2>
        <p>DATs are loaded, but no files scanned yet. Use the CLI to scan:</p>
        <code>romshelf scan /path/to/roms</code>
      </div>
    {/if}
  {/if}
</div>

<style>
  .page {
    max-width: 1200px;
  }

  h1 {
    font-size: 24px;
    font-weight: 600;
    margin-bottom: 24px;
    color: #fff;
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

  .stats-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
  }

  .stat-card {
    background-color: #1e1e3f;
    border: 1px solid #2a2a4a;
    border-radius: 12px;
    padding: 20px;
  }

  .stat-card.highlight {
    background: linear-gradient(135deg, #1e3a5f 0%, #1e1e3f 100%);
    border-color: #2a4a6a;
  }

  .stat-card.wide {
    grid-column: span 2;
  }

  .stat-value {
    font-size: 32px;
    font-weight: 700;
    color: #fff;
    margin-bottom: 4px;
  }

  .stat-label {
    font-size: 14px;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }

  .stat-pct {
    font-size: 12px;
    color: #4ade80;
    margin-top: 8px;
  }

  .empty-state {
    margin-top: 32px;
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
    font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: 13px;
    color: #4ade80;
  }
</style>
