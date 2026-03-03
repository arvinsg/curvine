# Curvine Development Guide

## Cursor Cloud specific instructions

### Overview

Curvine is a Rust-based high-performance distributed cache system with a master-slave architecture. The workspace is a Cargo workspace with these key crates: `curvine-server`, `curvine-client`, `curvine-cli`, `curvine-fuse`, `curvine-web`, `curvine-ufs`, `curvine-s3-gateway`, `curvine-tests`, `curvine-libsdk`, and `orpc`.

### System dependencies (already installed in snapshot)

- Rust 1.92.0 (pinned via `rust-toolchain.toml`)
- GCC 13+, G++ 14 (required for RocksDB C++ compilation — clang searches for GCC 14 headers)
- `protoc` (protobuf compiler >= 3.0)
- `llvm-config` (symlinked to `llvm-config-18`)
- `libfuse3-dev` + `fuse3` (for FUSE filesystem module)
- Node.js 22 / npm 10+ (for Web UI)
- Python 3.12+ (for Python SDK, optional)
- Java SDK and Maven are **not** installed; pass `--skip-java-sdk` to `build/check-env.sh` and `build/build.sh`

### Building

- `make check-env ARGS='--skip-java-sdk'` — verifies all build dependencies
- `cargo build -p curvine-server -p curvine-client -p curvine-cli --no-default-features --features curvine-client/opendal-s3` — build core components
- Add `-p curvine-fuse --features curvine-fuse/fuse3` to also build FUSE
- See `build/build.sh -h` for all build options

### Linting

- `cargo fmt -- --check` — formatting check
- `cargo clippy --all-targets -- --deny warnings --allow clippy::uninlined-format-args` — clippy lint
- `cd curvine-web/webui && npx vue-cli-service lint --no-fix` — Web UI ESLint

### Testing

Tests require a running test cluster. The standard flow (see `build/run-tests.sh`):

1. `cargo run --example test_cluster` — starts an in-process 2-master + 3-worker cluster using dynamic ports
2. `cargo test` — runs all tests against the cluster

**Important**: The test cluster writes to `/testing/curvine-tests/` (resolved from `../testing/` relative to `/workspace`). This directory must exist and be writable by the `ubuntu` user: `sudo mkdir -p /testing && sudo chown ubuntu:ubuntu /testing`.

### Running the cluster (development)

Start an in-process test cluster:
```
cargo run --example test_cluster
```
The cluster config is written to `/testing/curvine-tests/<timestamp>-<pid>-<rand>/curvine-cluster.toml`. Use this config with the CLI:
```
cargo run --bin curvine-cli -- --conf <config_path> report
cargo run --bin curvine-cli -- --conf <config_path> fs ls /
cargo run --bin curvine-cli -- --conf <config_path> fs mkdir /mydir
cargo run --bin curvine-cli -- --conf <config_path> fs put <local_file> <remote_path>
cargo run --bin curvine-cli -- --conf <config_path> fs cat <remote_path>
```

### Web UI development

```
cd curvine-web/webui
npm install
npm run serve   # starts dev server on port 8080
```
The Web UI is a Vue 3 app. In production, its static assets are served by the master node on port 9000. The dev server does **not** connect to the test cluster backend by default (it shows "UNKNOWN" status because the master's Web API runs on a random port in test mode).

### Gotchas

- The `cargo fmt` check may show pre-existing formatting diffs in the repo. Do not modify code unrelated to your task.
- RocksDB compiles from C++ source; if you see `fatal error: 'limits' file not found`, ensure `libstdc++-14-dev` is installed (clang looks for GCC 14 headers).
- The `curvine-fuse` crate requires the `fuse3` (or `fuse2`) feature flag; without it, the build skips FUSE.
