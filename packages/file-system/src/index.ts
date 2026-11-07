/**
 * @ofsdk/file-system - TypeScript wrapper for the offs WASM file system
 *
 * Provides a typed, Promise-based API for offline-first file system operations
 * in the browser, using OPFS for content and IndexedDB for metadata.
 */

interface WasmModule {
  default(): Promise<void>;
  FileSystemHandle: {
    open(root: string, nodeId?: string): Promise<WasmFileSystemHandle>;
  };
}

export type Residency = "full" | "passthrough";

export interface FileMeta {
  file_id: string;
  revision: string;
  kind: "File" | "Directory";
  mode: number;
}

export interface Entry {
  path: string;
  meta: FileMeta;
}

export interface ScanPage {
  entries: Entry[];
  next?: string;
}

interface WasmFileSystemHandle {
  residency(path: string): Promise<Residency>;
  set_residency(path: string, residency: Residency): Promise<void>;
  entry(path: string): Promise<Entry>;
  read(path: string): Promise<Uint8Array>;
  write(path: string, data: Uint8Array): Promise<Entry>;
  write_at(
    path: string,
    expectedRevision: string,
    offset: bigint,
    data: Uint8Array,
  ): Promise<number>;
  append(path: string, data: Uint8Array): Promise<void>;
  create_dir(path: string): Promise<Entry>;
  list(path: string): Promise<Entry[]>;
  scan(path: string, cursor?: string, limit?: number): Promise<ScanPage>;
  delete(path: string): Promise<void>;
  rename(from: string, to: string): Promise<void>;
  revision(path: string): Promise<string>;
  export_snapshots(): Promise<unknown>;
  announce(transport: unknown): Promise<void>;
  fetch(transport: unknown, path: string): Promise<void>;
  pump(transport: unknown): Promise<void>;
  sync(transport: unknown): Promise<void>;
  close(): void;
}

let wasmModule: Promise<WasmModule> | undefined;

async function loadWasm(): Promise<WasmModule> {
  wasmModule ??= import("../pkg/file_system_wasm.js").then(async (module) => {
    await module.default();
    return module as unknown as WasmModule;
  });
  return wasmModule;
}

/** High-level file system backed by browser OPFS + IndexedDB. */
export class FileSystem {
  private constructor(private readonly handle: WasmFileSystemHandle) {}

  static async open(root = "default"): Promise<FileSystem> {
    const { FileSystemHandle } = await loadWasm();
    const handle = await FileSystemHandle.open(root);
    await handle.set_residency("", "full");
    return new FileSystem(handle);
  }

  async residency(path: string): Promise<Residency> {
    return this.handle.residency(path);
  }

  async setResidency(path: string, residency: Residency): Promise<void> {
    await this.handle.set_residency(path, residency);
  }

  async entry(path: string): Promise<Entry> {
    return this.handle.entry(path);
  }

  async read(path: string): Promise<Uint8Array> {
    return this.handle.read(path);
  }

  async write(path: string, data: Uint8Array): Promise<Entry> {
    return this.handle.write(path, data);
  }

  async writeAt(
    path: string,
    expectedRevision: string,
    offset: bigint,
    data: Uint8Array,
  ): Promise<number> {
    return this.handle.write_at(path, expectedRevision, offset, data);
  }

  async append(path: string, data: Uint8Array): Promise<void> {
    await this.handle.append(path, data);
  }

  async createDir(path: string): Promise<Entry> {
    return this.handle.create_dir(path);
  }

  async list(path: string): Promise<Entry[]> {
    return this.handle.list(path);
  }

  async scan(path: string, cursor?: string, limit?: number): Promise<ScanPage> {
    return this.handle.scan(path, cursor, limit);
  }

  async delete(path: string): Promise<void> {
    await this.handle.delete(path);
  }

  async rename(from: string, to: string): Promise<void> {
    await this.handle.rename(from, to);
  }

  async revision(path: string): Promise<string> {
    return this.handle.revision(path);
  }

  async exportSnapshots(): Promise<unknown> {
    return this.handle.export_snapshots();
  }

  close(): void {
    this.handle.close();
  }
}
