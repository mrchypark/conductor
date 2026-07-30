# conductor

Rust durable workflow engine MVP workspace.

## Version and compatibility policy

`0.2.0` is the next pre-1.0 breaking boundary. The proposed `wait_for_event` and `emit_event` authoring APIs were removed before release and have no compatibility runtime.

The lifecycle target is:

- retain every published workflow version as immutable and inspectable
- treat stored JSON that does not match the current authoring schema as diagnostic-only, never executable
- reject unsupported definitions for new publication and new-run selection
- snapshot effective registry routing defaults into each newly published version, so later registry changes do not alter that version or its runs
- treat definitions published before routing snapshots as inspectable but non-executable until explicitly republished as a new version
- block destructive definition removal while active runs still reference the version

This is the compatibility contract for the `0.2.0` remediation. The [MVP verification matrix](docs/spec/mvp-verification-matrix.md) remains the source of truth for which safeguards have landed and been verified; this policy is not itself a claim of implementation completeness.

## Workspace checks

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Example

Run the end-to-end sample workflow:

```bash
cargo run -p workflow-engine --example order_sync
```

The sample publishes a Rust-authored workflow, ingests a start event, runs an in-process worker,
uses a durable `sleep` delay between synchronous tasks, finishes the workflow, and prints the final run summary.

## MVP Verification

Run the full local MVP verification sweep:

```bash
bash scripts/verify_mvp.sh
```

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```
