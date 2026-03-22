# conductor

Rust durable workflow engine MVP workspace.

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
waits for a resume event, finishes the workflow, and prints the final run summary.

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
