<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { listen } from "@tauri-apps/api/event";
  import { onDestroy, onMount } from "svelte";

  interface Stats {
    dat_count: number;
    entry_count: number;
    scanned_files: number;
    matched_files: number;
    total_bytes_scanned: number;
  }

  type DatImportEvent =
    | { type: "Started"; path: string }
    | { type: "DatDetected"; name: string; format: string }
    | { type: "SetStarted"; name: string; index: number }
    | { type: "RomProgress"; total_entries: number }
    | { type: "Completed"; name: string; entry_count: number; duration_ms: number; entries_per_sec: number }
    | { type: "Skipped"; reason: string };

  type ScanEvent =
    | { type: "Discovery"; directory: string }
    | { type: "FileStarted"; path: string; size: number }
    | { type: "FileProgress"; path: string; bytes_done: number; bytes_total: number }
    | { type: "FileCompleted"; path: string; size: number }
    | { type: "Summary"; discovered_files: number; processed_files: number; total_bytes: number; duration_ms: number; files_per_sec: number; bytes_per_sec: number }
    | { type: "Error"; message: string };

  let stats = $state<Stats | null>(null);
  let error = $state<string | null>(null);
  let loading = $state(true);

  let datPath = $state("");
  let datCategory = $state("");
  let importing = $state(false);
  let datEvents = $state<DatImportEvent[]>([]);

  let scanPath = $state("");
  let scanThreads = $state("");
  let scanning = $state(false);
  let scanEvents = $state<ScanEvent[]>([]);

  onMount(async () => {
    const unlistenDat = await listen<DatImportEvent>("dat_import", (event) => {
      datEvents = [...datEvents.slice(-200), event.payload];
    });
    const unlistenScan = await listen<ScanEvent>("scan", (event) => {
      scanEvents = [...scanEvents.slice(-200), event.payload];
    });

    try {
      stats = await invoke<Stats>("get_stats");
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }

    onDestroy(() => {
      unlistenDat();
      unlistenScan();
    });
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

  function renderDatEvent(event: DatImportEvent): string {
    switch (event.type) {
      case "Started":
        return `Started: ${event.path}`;
      case "DatDetected":
        return `Detected ${event.name} (${event.format})`;
      case "SetStarted":
        return `Set #${(event.index ?? 0) + 1}: ${event.name}`;
      case "RomProgress":
        return `${event.total_entries.toLocaleString()} entries processed`;
      case "Completed":
        return `Completed ${event.name} (${event.entry_count.toLocaleString()} entries in ${(event.duration_ms / 1000).toFixed(1)}s, ${event.entries_per_sec.toFixed(1)} entries/s)`;
      case "Skipped":
        return `Skipped: ${event.reason}`;
      default:
        return event.type;
    }
  }

  function renderScanEvent(event: ScanEvent): string {
    switch (event.type) {
      case "Discovery":
        return `Discovery: ${event.directory}`;
      case "FileStarted":
        return `File: ${event.path}`;
      case "FileProgress": {
        const pct = event.bytes_total ? ((event.bytes_done / event.bytes_total) * 100).toFixed(1) : "0.0";
        return `Progress: ${event.path} (${pct}%)`;
      }
      case "FileCompleted":
        return `Completed: ${event.path}`;
      case "Summary":
        return `Summary: ${event.processed_files}/${event.discovered_files} files (${event.files_per_sec.toFixed(1)} files/s, ${formatBytes(Math.round(event.bytes_per_sec))}/s, ${(event.duration_ms / 1000).toFixed(1)}s)`;
      case "Error":
        return `Error: ${event.message}`;
      default:
        return event.type;
    }
  }

  async function startDatImport(event?: SubmitEvent) {
    event?.preventDefault();
    if (!datPath) return;
    importing = true;
    datEvents = [];
    try {
      await invoke("import_dat", {
        path: datPath,
        category: datCategory || null
      });
    } catch (e) {
      datEvents = [...datEvents, { type: "Skipped", reason: String(e) }];
    } finally {
      importing = false;
    }
  }

  async function startScan(event?: SubmitEvent) {
    event?.preventDefault();
    if (!scanPath) return;
    scanning = true;
    scanEvents = [];
    try {
      await invoke("scan_directory", {
        path: scanPath,
        threads: scanThreads ? Number(scanThreads) : null
      });
    } catch (e) {
      scanEvents = [...scanEvents, { type: "Error", message: String(e) }];
    } finally {
      scanning = false;
    }
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

<div class="page">
  <section class="panel">
    <h2>Import DAT</h2>
    <form onsubmit={startDatImport}>
      <label>
        DAT File Path
        <input bind:value={datPath} placeholder="/path/to/file.dat" />
      </label>
      <label>
        Category (optional)
        <input bind:value={datCategory} placeholder="TOSEC/Amiga/Games/[ADF]" />
      </label>
      <button type="submit" disabled={!datPath || importing}>
        {importing ? "Importing…" : "Import DAT"}
      </button>
    </form>
    {#if datEvents.length > 0}
      <div class="event-log">
        <h3>DAT Progress</h3>
        <ul>
          {#each datEvents as evt}
            <li>{renderDatEvent(evt)}</li>
          {/each}
        </ul>
      </div>
    {/if}
  </section>

  <section class="panel">
    <h2>Scan Directory</h2>
    <form onsubmit={startScan}>
      <label>
        Directory Path
        <input bind:value={scanPath} placeholder="/path/to/roms" />
      </label>
      <label>
        Threads (optional)
        <input bind:value={scanThreads} type="number" min="1" placeholder="All cores" />
      </label>
      <button type="submit" disabled={!scanPath || scanning}>
        {scanning ? "Scanning…" : "Scan Directory"}
      </button>
    </form>
    {#if scanEvents.length > 0}
      <div class="event-log">
        <h3>Scan Progress</h3>
        <ul>
          {#each scanEvents as evt}
            <li>{renderScanEvent(evt)}</li>
          {/each}
        </ul>
      </div>
    {/if}
  </section>
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

  .panel {
    background-color: #131328;
    border: 1px solid #2a2a4a;
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 20px;
  }

  .panel h2 {
    color: #fff;
    margin-bottom: 16px;
  }

  form {
    display: grid;
    gap: 12px;
    margin-bottom: 16px;
  }

  label {
    display: flex;
    flex-direction: column;
    font-size: 14px;
    color: #cbd5f5;
  }

  input {
    margin-top: 4px;
    padding: 10px;
    border-radius: 6px;
    border: 1px solid #2a2a4a;
    background-color: #0f0f1f;
    color: #fff;
  }

  button {
    padding: 10px 16px;
    border-radius: 6px;
    border: none;
    background: linear-gradient(135deg, #2563eb, #4f46e5);
    color: #fff;
    font-weight: 600;
    cursor: pointer;
  }

  button[disabled] {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .event-log ul {
    list-style: none;
    padding: 0;
    margin: 0;
    max-height: 200px;
    overflow-y: auto;
    font-family: 'SF Mono', 'Fira Code', monospace;
    font-size: 13px;
    color: #cbd5f5;
  }

  .event-log li {
    padding: 4px 0;
    border-bottom: 1px solid #2a2a4a;
  }
</style>
