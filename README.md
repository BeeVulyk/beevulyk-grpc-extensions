# beevulyk-grpc-extensions

BeeVulyk fork of [`ITYFT/my-grpc-extensions`](https://github.com/ITYFT/my-grpc-extensions) (upstream tag `0.6.4`).
Rewired to use [`beevulyk-rust-extensions`](https://github.com/BeeVulyk/beevulyk-rust-extensions)
in place of the upstream `yft-rust-extensions` dependency; version reset to `0.1.0`.

## Workspace members

| Crate | Path | Purpose |
|---|---|---|
| `beevulyk-grpc-extensions` | `beevulyk-grpc-extensions/` | Runtime gRPC helpers: channel pool, retries, stream utilities, server helpers. Built on `tonic` 0.14. |
| `beevulyk-grpc-client-macros` | `beevulyk-grpc-client-macros/` | Proc-macro `#[generate_grpc_client(...)]` that generates a tonic client with retry/ping loop from a `.proto` file. |
| `beevulyk-grpc-server-macros` | `beevulyk-grpc-server-macros/` | Proc-macros for server-side boilerplate (`#[with_telemetry]`, streaming helpers). |
| `external-dependencies` | `external-dependencies/` | Workspace-internal prelude that re-exports `futures_core::Stream`. Kept under its original name (workspace-internal, not published externally). |

## Features (runtime crate)

- `grpc-client` — pulls in `beevulyk-grpc-client-macros`.
- `grpc-server` — pulls in `beevulyk-grpc-server-macros`.
- `adjust-server-stream` — build-time toggle for server stream tuning.

## Usage

```toml
[dependencies]
beevulyk-grpc-extensions = { git = "https://github.com/BeeVulyk/beevulyk-grpc-extensions.git", tag = "0.1.0", features = ["grpc-client", "grpc-server"] }
```

See the per-crate READMEs in `beevulyk-grpc-client-macros/` and `beevulyk-grpc-server-macros/` for macro usage examples.

## Build

```bash
cargo build --workspace
cargo test --workspace
```
