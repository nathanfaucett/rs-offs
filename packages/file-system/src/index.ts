/**
 * @offs/file-system - TypeScript wrapper for the offs File System WASM module
 *
 * Provides a modern, Promise-based API for offline-first file system operations
 * in the browser, using the Origin Private File System (OPFS) for storage.
 */

import type {
  FileSystemHandle as WasmFileSystemHandle,
  Residency,
  Entry,
  ScanPage,
  FileMeta,
} from "./wasm.js";

// Re-export WASM types
export type { Residency, Entry, ScanPage, FileMeta };

/**
 * File operations for session protocol
 */
export type FileOperation =
  | { type: "Open"; request: { path: string; revision?: string } }
  | { type: "Read"; handle: { id: string }; offset: bigint; length: number }
  | {
      type: "Write";
      handle: { id: string };
      expected_revision: string;
      offset: bigint;
      data: Uint8Array;
    }
  | { type: "Scan"; handle: { id: string }; cursor?: string; limit: number }
  | { type: "Close"; handle: { id: string } };

/**
 * File responses for session protocol
 */
export type FileResponse =
  | { type: "Opened"; handle: { id: string }; entry: Entry; revision: string }
  | { type: "ReadChunk"; data: Uint8Array }
  | { type: "ReadEnd" }
  | { type: "Written"; count: number; revision: string }
  | { type: "Page"; page: ScanPage }
  | { type: "Closed" }
  | { type: "Error"; error: FileServiceError };

/**
 * File service errors
 */
export type FileServiceError =
  | "NotFound"
  | "NotDirectory"
  | "IsDirectory"
  | "InvalidReadLength"
  | "InvalidScanLimit"
  | "InvalidScanCursor"
  | "StaleRevision"
  | "PassthroughWrite"
  | "ContentUnavailable"
  | "PermissionDenied"
  | "UnknownHandle"
  | "Io";

/**
 * Simple Transport interface that consumers implement
 * to provide network connectivity
 */
export interface FileTransport {
  peers(): string[];
  send(peer: string, message: unknown): Promise<void>;
  broadcast(message: unknown): Promise<void>;
  subscribe(handler: (peer: string, message: unknown) => void): void;
  openSession(
    peer: string,
    request: { path: string; revision?: string },
  ): Promise<{
    request(operation: unknown): Promise<unknown[]>;
    close(): Promise<void>;
  }>;
  subscribeSessions(
    handler: (
      peer: string,
      operation: unknown,
      respond: (response: unknown) => void,
    ) => void,
  ): void;
}

/**
 * Create a simple in-memory transport for testing
 */
export function createMemoryTransport(
  peers: string[],
  _onMessage?: (peer: string, message: unknown) => void,
): FileTransport {
  return {
    peers: () => peers,
    async send(_peer: string, _message: unknown) {},
    async broadcast(_message: unknown) {},
    subscribe(_handler: (peer: string, message: unknown) => void) {},
    async openSession() {
      return {
        async request() {
          return [];
        },
        async close() {},
      };
    },
    subscribeSessions() {},
  };
}

// Type alias for the WASM module
// The wasm-bindgen output has the classes on the wasm namespace (returned by initSync)
interface WasmModule {
  default: (module_or_path?: unknown) => Promise<unknown>;
  initSync: (
    module:
      | { module: BufferSource | WebAssembly.Module }
      | BufferSource
      | WebAssembly.Module,
  ) => WasmModule;
  FileSystemHandle: typeof WasmFileSystemHandle;
  memory: WebAssembly.Memory;
  __wbindgen_externrefs: WebAssembly.Table;
  __wbindgen_start: () => void;
  [key: string]: unknown;
}

/**
 * High-level TypeScript wrapper around the WASM File System
 */
export class FileSystem {
  #handle: WasmFileSystemHandle | null = null;

  /**
   * Initialize the WASM module and open a file system
   */
  static async open(
    namespace: string,
    name: string,
    _nodeId: string = "default",
  ): Promise<FileSystem> {
    const wasm = (await loadWasmModule()) as WasmModule;
    const handle = await wasm.FileSystemHandle.open(namespace, name, "default");
    await handle.set_residency("", "full");
    return new FileSystem(handle);
  }

  private constructor(private readonly handle: WasmFileSystemHandle) {}

  /**
   * Get the residency of a path
   */
  async residency(path: string): Promise<Residency> {
    this.assertOpen();
    return this.#handle?.residency(path) as Promise<Residency>;
  }

  /**
   * Set the residency of a path
   */
  async setResidency(path: string, residency: Residency): Promise<void> {
    this.assertOpen();
    await this.#handle?.set_residency(path, residency);
  }

  /**
   * Get a file entry by path
   */
  async entry(path: string): Promise<Entry> {
    this.assertOpen();
    return this.#handle?.entry(path) as Promise<Entry>;
  }

  /**
   * Read a file's contents
   */
  async read(path: string): Promise<Uint8Array> {
    this.assertOpen();
    return this.#handle?.read(path) as Promise<Uint8Array>;
  }

  /**
   * Write a file (creates or overwrites)
   */
  async write(path: string, data: Uint8Array): Promise<Entry> {
    this.assertOpen();
    return this.#handle?.write(path, data) as Promise<Entry>;
  }

  /**
   * Write at a specific offset
   */
  async writeAt(
    path: string,
    expectedRevision: string,
    offset: bigint,
    data: Uint8Array,
  ): Promise<number> {
    this.assertOpen();
    return this.#handle?.write_at(
      path,
      expectedRevision,
      offset,
      data,
    ) as Promise<number>;
  }

  /**
   * Append to a file
   */
  async append(path: string, data: Uint8Array): Promise<void> {
    this.assertOpen();
    return this.#handle?.append(path, data) as Promise<void>;
  }

  /**
   * Create a directory
   */
  async createDir(path: string): Promise<Entry> {
    this.assertOpen();
    return this.#handle?.create_dir(path) as Promise<Entry>;
  }

  /**
   * List a directory
   */
  async list(path: string): Promise<Entry[]> {
    this.assertOpen();
    return this.#handle?.list(path) as Promise<Entry[]>;
  }

  /**
   * Scan a directory with pagination
   */
  async scan(path: string, cursor?: string, limit?: number): Promise<ScanPage> {
    this.assertOpen();
    return this.#handle?.scan(path, cursor, limit) as Promise<ScanPage>;
  }

  /**
   * Delete a file or directory
   */
  async delete(path: string): Promise<void> {
    this.assertOpen();
    return this.#handle?.delete(path) as Promise<void>;
  }

  /**
   * Rename a file or directory
   */
  async rename(from: string, to: string): Promise<void> {
    this.assertOpen();
    return this.#handle?.rename(from, to) as Promise<void>;
  }

  /**
   * Get the current revision of a file
   */
  async revision(path: string): Promise<string> {
    this.assertOpen();
    return this.#handle?.revision(path) as Promise<string>;
  }

  /**
   * Export metadata snapshots
   */
  async exportSnapshots(): Promise<unknown> {
    this.assertOpen();
    return this.#handle?.export_snapshots() as Promise<unknown>;
  }

  /**
   * Announce metadata to peers via a transport
   */
  async announce(_transport: FileTransport): Promise<void> {
    this.assertOpen();
    console.warn("announce: transport bridging not fully implemented");
  }

  /**
   * Fetch a file from peers via a transport
   */
  async fetch(_transport: FileTransport, _path: string): Promise<void> {
    this.assertOpen();
    console.warn("fetch: transport bridging not fully implemented");
  }

  /**
   * Run one metadata sync pass
   */
  async pump(_transport: FileTransport): Promise<void> {
    this.assertOpen();
    console.warn("pump: transport bridging not fully implemented");
  }

  /**
   * Full sync loop until retired
   */
  async sync(_transport: FileTransport): Promise<void> {
    this.assertOpen();
    console.warn("sync: transport bridging not fully implemented");
  }

  /**
   * Close the filesystem, releasing ownership
   */
  close(): void {
    if (this.#handle) {
      this.#handle.close();
      this.#handle = null;
    }
  }

  private assertOpen(): void {
    if (!this.#handle) {
      throw new Error("FileSystem is not open. Call FileSystem.open() first.");
    }
  }
}

/**
 * Dynamically load the WASM module
 */
let wasmModulePromise: Promise<WasmModule> | null = null;

async function loadWasmModule(): Promise<WasmModule> {
  if (!wasmModulePromise) {
    wasmModulePromise = (async () => {
      const mod = await import("../dist/file_system.js");
      const wasmBytes = await (
        await fetch("../dist/file_system_bg.wasm")
      ).arrayBuffer();
      const exports = mod.initSync(new WebAssembly.Module(wasmBytes));
      // Attach the exports to the module for easy access
      return { ...mod, ...exports } as unknown as WasmModule;
    })();
  }
  return wasmModulePromise;
}
