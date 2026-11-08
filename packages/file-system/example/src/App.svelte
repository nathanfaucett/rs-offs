<script lang="ts">
import { type Entry, FileSystem } from "@ofsdk/file-system";

let fs: FileSystem | null = $state.raw(null);
let error = $state("");
let ready = $state(false);

let cwd = $state("");
let entries: Entry[] = $state([]);
let newFolderName = $state("");
let previewPath = $state("");
let previewText = $state("");
let previewUrl = $state("");

let fileInput = $state<HTMLInputElement>();

const imageExts = ["png", "jpg", "jpeg", "gif", "webp", "svg", "bmp"];

function join(path: string, name: string) {
  return path ? `${path}/${name}` : name;
}

function parent(path: string) {
  const i = path.lastIndexOf("/");
  return i === -1 ? "" : path.slice(0, i);
}

function name(path: string) {
  const i = path.lastIndexOf("/");
  return i === -1 ? path : path.slice(i + 1);
}

function isImage(path: string) {
  const ext = path.split(".").pop()?.toLowerCase() ?? "";
  return imageExts.includes(ext);
}

async function run(fn: () => Promise<void>) {
  error = "";
  try {
    await fn();
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }
}

async function connect() {
  await run(async () => {
    fs = await FileSystem.open("demo-fs");
    await refresh();
    ready = true;
  });
}

connect();

async function refresh() {
  const f = fs;
  if (!f) return;
  await run(async () => {
    entries = (await f.list(cwd)).sort((a, b) => {
      if (a.meta.kind !== b.meta.kind)
        return a.meta.kind === "Directory" ? -1 : 1;
      return a.path.localeCompare(b.path);
    });
  });
}

async function upload(files: FileList | null) {
  const f = fs;
  if (!f || !files) return;
  await run(async () => {
    for (const file of Array.from(files)) {
      await f.write(
        join(cwd, file.name),
        new Uint8Array(await file.arrayBuffer())
      );
    }
    await refresh();
  });
}

async function makeFolder() {
  const f = fs;
  const name_ = newFolderName.trim();
  if (!f || !name_) return;
  await run(async () => {
    await f.createDir(join(cwd, name_));
    newFolderName = "";
    await refresh();
  });
}

async function remove(path: string) {
  const f = fs;
  if (!f) return;
  await run(async () => {
    await f.delete(path);
    if (previewPath === path || previewPath.startsWith(`${path}/`))
      closePreview();
    await refresh();
  });
}

async function openDir(path: string) {
  cwd = path;
  closePreview();
  await refresh();
}

async function view(path: string) {
  const f = fs;
  if (!f) return;
  await run(async () => {
    const data = await f.read(path);
    previewPath = path;
    if (isImage(path)) {
      previewUrl = URL.createObjectURL(new Blob([data.buffer as ArrayBuffer]));
      previewText = "";
    } else {
      previewText = new TextDecoder().decode(data);
      previewUrl = "";
    }
  });
}

function closePreview() {
  if (previewUrl) URL.revokeObjectURL(previewUrl);
  previewPath = "";
  previewText = "";
  previewUrl = "";
}

const crumbs = $derived(cwd === "" ? [] : cwd.split("/"));
</script>

<main class="mx-auto max-w-2xl p-8 font-sans">
    <h1 class="text-2xl font-bold mb-1">offs file manager</h1>
    <p class="text-gray-600 mb-6">
        Files persist in your browser (OPFS + IndexedDB via WASM).
    </p>

    {#if error}<p class="mb-4 text-sm text-red-600">{error}</p>{/if}

    {#if !ready}
        <p class="text-gray-600">Opening file system…</p>
    {:else}
        <div class="flex items-center gap-2 mb-4 flex-wrap">
            <button
                class="px-3 py-1.5 bg-blue-600 text-white rounded text-sm hover:bg-blue-700"
                onclick={() => fileInput?.click()}
            >
                Upload files
            </button>
            <input
                bind:this={fileInput}
                type="file"
                multiple
                class="hidden"
                onchange={(e) => upload(e.currentTarget.files)}
            />
            <input
                class="border rounded px-2 py-1.5 text-sm w-40"
                placeholder="New folder name"
                bind:value={newFolderName}
                onkeydown={(e) => e.key === "Enter" && makeFolder()}
            />
            <button
                class="px-3 py-1.5 bg-gray-800 text-white rounded text-sm disabled:opacity-50"
                onclick={makeFolder}
                disabled={!newFolderName.trim()}
            >
                Add folder
            </button>
            <button
                class="px-3 py-1.5 border rounded text-sm disabled:opacity-50"
                onclick={() => openDir(parent(cwd))}
                disabled={cwd === ""}
            >
                Up
            </button>
        </div>

        <nav class="text-sm mb-2 text-gray-600">
            <button class="hover:underline" onclick={() => openDir("")}
                >root</button
            >
            {#each crumbs as crumb, i (i)}
                / <button
                    class="hover:underline"
                    onclick={() => openDir(crumbs.slice(0, i + 1).join("/"))}
                    >{crumb}</button
                >
            {/each}
        </nav>

        <ul class="border rounded-lg divide-y">
            {#each entries as entry (entry.path)}
                {@const isDir = entry.meta.kind === "Directory"}
                <li class="flex items-center gap-2 px-3 py-2">
                    <span>{isDir ? "📁" : "📄"}</span>
                    {#if isDir}
                        <button
                            class="flex-1 text-left hover:underline"
                            onclick={() => openDir(entry.path)}
                            >{name(entry.path)}</button
                        >
                    {:else}
                        <button
                            class="flex-1 text-left hover:underline"
                            onclick={() => view(entry.path)}
                            >{name(entry.path)}</button
                        >
                    {/if}
                    <button
                        class="text-red-600 text-sm hover:underline"
                        onclick={() => remove(entry.path)}>delete</button
                    >
                </li>
            {:else}
                <li class="px-3 py-4 text-sm text-gray-500">
                    Empty folder — upload something.
                </li>
            {/each}
        </ul>

        {#if previewPath}
            <section class="border rounded-lg p-4 mt-4">
                <div class="flex items-center justify-between mb-2">
                    <h2 class="font-semibold text-sm">{previewPath}</h2>
                    <button
                        class="text-sm text-gray-600 hover:underline"
                        onclick={closePreview}>close</button
                    >
                </div>
                {#if previewUrl}
                    <img
                        src={previewUrl}
                        alt={previewPath}
                        class="max-h-96 rounded"
                    />
                {:else}
                    <pre
                        class="bg-gray-100 p-3 rounded text-sm overflow-auto max-h-96">{previewText}</pre>
                {/if}
            </section>
        {/if}
    {/if}
</main>
