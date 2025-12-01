<script lang="ts">
  import { invoke } from "@tauri-apps/api/core";
  import { onMount } from "svelte";

  interface DatSummary {
    id: number;
    name: string;
    category: string | null;
    version: string | null;
    entry_count: number;
    set_count: number;
  }

  interface DatTreeNode {
    name: string;
    children: DatTreeNode[];
    dats: DatSummary[];
  }

  let tree = $state<DatTreeNode | null>(null);
  let error = $state<string | null>(null);
  let loading = $state(true);
  let expandedPaths = $state<Set<string>>(new Set());

  onMount(async () => {
    try {
      tree = await invoke<DatTreeNode>("get_dat_tree");
      // Auto-expand first level
      if (tree) {
        for (const child of tree.children) {
          expandedPaths.add(child.name);
        }
      }
    } catch (e) {
      error = String(e);
    } finally {
      loading = false;
    }
  });

  function toggleExpand(path: string) {
    if (expandedPaths.has(path)) {
      expandedPaths.delete(path);
    } else {
      expandedPaths.add(path);
    }
    expandedPaths = new Set(expandedPaths);
  }

  function isExpanded(path: string): boolean {
    return expandedPaths.has(path);
  }

  function formatNumber(n: number): string {
    return n.toLocaleString();
  }

  function countDatsInNode(node: DatTreeNode): number {
    let count = node.dats.length;
    for (const child of node.children) {
      count += countDatsInNode(child);
    }
    return count;
  }
</script>

{#snippet treeNode(node: DatTreeNode, path: string, depth: number)}
  {#each node.children as child (child.name)}
    {@const childPath = path ? `${path}/${child.name}` : child.name}
    {@const expanded = isExpanded(childPath)}
    {@const datCount = countDatsInNode(child)}
    <div class="tree-item" style="--depth: {depth}">
      <button class="tree-toggle" onclick={() => toggleExpand(childPath)}>
        <span class="chevron" class:expanded>{expanded ? "▼" : "▶"}</span>
        <span class="folder-icon">📁</span>
        <span class="node-name">{child.name}</span>
        <span class="node-count">{datCount}</span>
      </button>
    </div>
    {#if expanded}
      {@render treeNode(child, childPath, depth + 1)}
      {#each child.dats as dat (dat.id)}
        <div class="tree-item dat-item" style="--depth: {depth + 1}">
          <span class="dat-icon">📄</span>
          <span class="dat-name">{dat.name}</span>
          <span class="dat-meta">
            {#if dat.version}
              <span class="dat-version">{dat.version}</span>
            {/if}
            <span class="dat-entries">{formatNumber(dat.entry_count)} entries</span>
          </span>
        </div>
      {/each}
    {/if}
  {/each}
  {#if depth === 0}
    {#each node.dats as dat (dat.id)}
      <div class="tree-item dat-item" style="--depth: 0">
        <span class="dat-icon">📄</span>
        <span class="dat-name">{dat.name}</span>
        <span class="dat-meta">
          {#if dat.version}
            <span class="dat-version">{dat.version}</span>
          {/if}
          <span class="dat-entries">{formatNumber(dat.entry_count)} entries</span>
        </span>
      </div>
    {/each}
  {/if}
{/snippet}

<div class="page">
  <div class="header">
    <h1>DATs</h1>
    {#if tree}
      {@const totalDats = countDatsInNode(tree)}
      <span class="count">{formatNumber(totalDats)} loaded</span>
    {/if}
  </div>

  {#if loading}
    <div class="loading">Loading...</div>
  {:else if error}
    <div class="error">
      <p>Failed to load DATs: {error}</p>
    </div>
  {:else if tree && countDatsInNode(tree) === 0}
    <div class="empty-state">
      <h2>No DATs Loaded</h2>
      <p>Import DAT files using the CLI:</p>
      <code>romshelf dat import-dir /path/to/dats --prefix TOSEC</code>
    </div>
  {:else if tree}
    <div class="tree-container">
      {@render treeNode(tree, "", 0)}
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

  .node-count {
    font-size: 12px;
    color: #666;
    background-color: #2a2a4a;
    padding: 2px 6px;
    border-radius: 4px;
  }

  .dat-item {
    color: #ccc;
    font-size: 13px;
  }

  .dat-item:hover {
    background-color: #2a2a4a;
  }

  .dat-icon {
    font-size: 12px;
    margin-left: 20px;
  }

  .dat-name {
    flex: 1;
    color: #aaa;
  }

  .dat-meta {
    display: flex;
    gap: 12px;
    font-size: 11px;
    color: #666;
  }

  .dat-version {
    font-family: "SF Mono", "Fira Code", monospace;
    color: #888;
  }

  .dat-entries {
    color: #666;
  }
</style>
