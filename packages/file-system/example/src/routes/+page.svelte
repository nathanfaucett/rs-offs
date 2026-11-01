<script lang="ts">
import { type Entry, FileSystem } from "@offs/file-system";

let fs: FileSystem | null = $state(null);
let status = $state("Not initialized");
let error = $state("");

let writePath = $state("notes/demo.txt");
let writeContent = $state("Hello from offs!");
let readOutput = $state("");
let listOutput = $state("");
let dirPath = $state("notes");

async function init() {
  try {
    error = "";
    status = "Initializing WASM…";
    fs = await FileSystem.open("demo", "fs1", "node1");
    status = "File system opened: demo/fs1";
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
    status = "Error";
  }
}

async function write() {
  if (!fs) return;
  try {
    error = "";
    const entry = await fs.write(
      writePath,
      new TextEncoder().encode(writeContent),
    );
    status = `Wrote ${writePath} (rev: ${entry.meta.revision})`;
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
}

async function read() {
  if (!fs) return;
  try {
    error = "";
    const data = await fs.read(writePath);
    readOutput = new TextDecoder().decode(data);
    status = `Read ${data.length} bytes`;
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
}

async function append() {
  if (!fs) return;
  try {
    error = "";
    await fs.append(
      writePath,
      new TextEncoder().encode(`\nAppended at ${new Date().toISOString()}`),
    );
    status = "Appended";
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
}

async function mkdir() {
  if (!fs) return;
  try {
    error = "";
    await fs.createDir(dirPath);
    status = `Created directory ${dirPath}`;
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
}

async function list() {
  if (!fs) return;
  try {
    error = "";
    const entries: Entry[] = await fs.list(dirPath);
    listOutput =
      entries
        .map((e) => `${e.path} (${e.meta.kind}) rev:${e.meta.revision}`)
        .join("\n") || "(empty)";
    status = `Listed ${entries.length} entries`;
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
}

async function remove() {
  if (!fs) return;
  try {
    error = "";
    await fs.delete(dirPath);
    status = `Deleted ${dirPath}`;
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
}
</script>

<main class="mx-auto max-w-2xl p-8 font-sans">
  <h1 class="text-2xl font-bold mb-2">offs browser file system</h1>
  <p class="text-gray-600 mb-6">Offline-first file system in the browser using OPFS + IndexedDB via WASM.</p>

  <section class="border rounded-lg p-4 mb-4">
    <h2 class="font-semibold mb-2">1. Initialize</h2>
    <button
      class="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
      onclick={init}
      disabled={fs !== null}
    >
      Initialize WASM & Open File System
    </button>
    <p class="mt-2 text-sm" class:text-red-600={error !== ""}>{status}</p>
    {#if error}<p class="text-sm text-red-600">{error}</p>{/if}
  </section>

  <section class="border rounded-lg p-4 mb-4">
    <h2 class="font-semibold mb-2">2. Write / Read Files</h2>
    <input class="w-full border rounded px-3 py-2 mb-2 font-mono text-sm" bind:value={writePath} placeholder="Path" />
    <textarea class="w-full border rounded px-3 py-2 mb-2 font-mono text-sm" rows="3" bind:value={writeContent}></textarea>
    <div class="flex gap-2">
      <button class="px-3 py-1.5 bg-gray-800 text-white rounded text-sm disabled:opacity-50" onclick={write} disabled={!fs}>Write</button>
      <button class="px-3 py-1.5 bg-gray-800 text-white rounded text-sm disabled:opacity-50" onclick={read} disabled={!fs}>Read</button>
      <button class="px-3 py-1.5 bg-gray-800 text-white rounded text-sm disabled:opacity-50" onclick={append} disabled={!fs}>Append</button>
    </div>
    {#if readOutput}<pre class="mt-2 bg-gray-100 p-3 rounded text-sm overflow-auto max-h-48">{readOutput}</pre>{/if}
  </section>

  <section class="border rounded-lg p-4 mb-4">
    <h2 class="font-semibold mb-2">3. Directory Operations</h2>
    <input class="w-full border rounded px-3 py-2 mb-2 font-mono text-sm" bind:value={dirPath} placeholder="Directory path" />
    <div class="flex gap-2">
      <button class="px-3 py-1.5 bg-gray-800 text-white rounded text-sm disabled:opacity-50" onclick={mkdir} disabled={!fs}>Create Dir</button>
      <button class="px-3 py-1.5 bg-gray-800 text-white rounded text-sm disabled:opacity-50" onclick={list} disabled={!fs}>List</button>
      <button class="px-3 py-1.5 bg-red-600 text-white rounded text-sm disabled:opacity-50" onclick={remove} disabled={!fs}>Delete</button>
    </div>
    {#if listOutput}<pre class="mt-2 bg-gray-100 p-3 rounded text-sm overflow-auto max-h-48">{listOutput}</pre>{/if}
  </section>
</main>
