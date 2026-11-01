/**
 * Type definitions for the offs file-system WASM module
 * Generated from Rust via wasm-bindgen
 */

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;

// ============================================================================
// Types matching the Rust browser_api exports
// ============================================================================

export type Residency = "full" | "passthrough";

export interface FileMeta {
    file_id: string;
    revision: string;
    kind: "file" | "directory";
    providers: string[];
    local: boolean;
    mode: number;
    owner: string;
    group: string;
}

export interface Entry {
    path: string;
    meta: FileMeta;
}

export interface ScanPage {
    entries: Entry[];
    next?: string;
}

/**
 * Main WASM class for file system operations
 */
export class FileSystemHandle {
    /**
     * Open a file system
     */
    static open(namespace: string, name: string, nodeId: string): Promise<FileSystemHandle>;

    /**
     * Get the residency of a path
     */
    residency(path: string): Promise<Residency>;

    /**
     * Set the residency of a path
     */
    set_residency(path: string, residency: Residency): Promise<void>;

    /**
     * Get a file entry by path
     */
    entry(path: string): Promise<Entry>;

    /**
     * Read a file's contents
     */
    read(path: string): Promise<Uint8Array>;

    /**
     * Write a file (creates or overwrites)
     */
    write(path: string, data: Uint8Array): Promise<Entry>;

    /**
     * Write at a specific offset
     */
    write_at(path: string, expectedRevision: string, offset: bigint, data: Uint8Array): Promise<number>;

    /**
     * Append to a file
     */
    append(path: string, data: Uint8Array): Promise<void>;

    /**
     * Create a directory
     */
    create_dir(path: string): Promise<Entry>;

    /**
     * List a directory
     */
    list(path: string): Promise<Entry[]>;

    /**
     * Scan a directory with pagination
     */
    scan(path: string, cursor?: string, limit?: number): Promise<ScanPage>;

    /**
     * Delete a file or directory
     */
    delete(path: string): Promise<void>;

    /**
     * Rename a file or directory
     */
    rename(from: string, to: string): Promise<void>;

    /**
     * Get the current revision of a file
     */
    revision(path: string): Promise<string>;

    /**
     * Export metadata snapshots
     */
    export_snapshots(): Promise<unknown>;

    /**
     * Announce metadata to peers via a transport
     */
    announce(transport: unknown): Promise<void>;

    /**
     * Fetch a file from peers via a transport
     */
    fetch(transport: unknown, path: string): Promise<void>;

    /**
     * Run one metadata sync pass
     */
    pump(transport: unknown): Promise<void>;

    /**
     * Full sync loop until retired
     */
    sync(transport: unknown): Promise<void>;

    /**
     * Close the filesystem, releasing ownership
     */
    close(): void;
}
