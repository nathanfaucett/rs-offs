# AGENTS.md

For Domain information, see the [Domain Model](GLOSSARY.md).

## Refactoring Protocol

- ALWAYS choose rewriting and deleting over modifying or wrapping old code.
- NEVER write glue code to support old versions or maintain past state.
- ALWAYS clean up and purge any files, functions, or blocks rendered obsolete by new implementations.

## Patterns & Conventions

- Default to `no_std`; enable `std` only when required (IO, threading, async runtimes).
- Group Rust imports by namespace and order: std/core/alloc → external → internal (`crate`, `super`).
- Prefer explicit imports and minimal dependencies.
- Avoid glob imports and hard-coded absolute paths.
- No non-essential comments; prefer refactoring over comments.
- Prefer expect("message") over unwrap(); always provide a clear message explaining the invariant.

## Module Organization

- Keep modules minimal: `mod.rs` should only declare/re-export modules.
- `lib.rs` and `mod.rs` must stay thin: crate-level attributes, `extern crate`, `pub use`, and `mod` declarations only.
- Do not place implementation logic (structs, enums, `impl` blocks, free `fn`, constants, statics, type aliases, traits) directly in `lib.rs` or `mod.rs`.
- If functionality is missing, create/update a sibling module (for example, `src/node.rs`) and wire it from `lib.rs`/`mod.rs`.
- When adding behavior, create/update a dedicated module file (`src/<feature>.rs`) and re-export as needed.

## Dependencies & Versioning

- All new dependencies must use only `major.minor` in the version field.
- Specify dependencies in full table form with `default-features = false` and enable only required features.
- Group dependencies by logical category with a comment header, alphabetize within groups, and separate groups with a blank line.

## Code Style

- Follow Rust's standard formatting and style guidelines.
- Use `clippy` for linting and adhere to its recommendations.
- Use `rustfmt` for consistent code formatting.

## Testing

- Write unit tests for all public functions and critical internal logic.
- Use integration tests for testing public API behavior and interactions between modules.
- Use `cargo hack test --feature-powerset --all-targets` to run tests to ensure all features combinations are tested.

## CRAP (Complexity, Risk, and Priority)

- Assess new features and changes for complexity, risk, and priority before implementation.
- Use `just crap` to evaluate code complexity and identify areas for refactoring.

## Agent skills

### Issue tracker

Issues live as markdown files under `.scratch/<feature>/` in this repo. See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical roles, label string equal to role name. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `GLOSSARY.md` at the repo root plus `docs/adr/`. See `docs/agents/domain.md`.
