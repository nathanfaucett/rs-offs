#![forbid(unsafe_code)]

use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use file_system::browser_api::*;
