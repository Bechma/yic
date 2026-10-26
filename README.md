# yic

A small axum API that returns videos from channels or playlists and searches YouTube Data API v3.

## Run

Enable **YouTube Data API v3** in your Google Cloud project and create an API key.

Playback JavaScript runs inside the Rust server through `deno_core` and embedded
V8. No Node/Deno executable or npm install is required. The first Cargo build
downloads the platform's prebuilt V8 library; the resulting server binary embeds
the DOM, parser and extraction helpers.

Provide the API key through the environment (no OAuth is needed for public data):

```sh
export YOUTUBE_API_KEY='your-api-key'
cargo run
```

The server listens on `127.0.0.1:3000`. Override this with `BIND_ADDRESS` if needed.

## Request logs

```sh
RUST_LOG=warn,yic=debug cargo run
```

Each request has a random UUID v4 in its tracing span and `X-Request-Id` response
header. Logs cover extraction stages and timings, worker reuse, upstream statuses,
and streamed-body completion/cancellation. Debug logging includes chunk ranges.
Keys, PO tokens, signed media URLs, query strings and raw JS errors are omitted.

## Embedded playback runtime

Two dedicated Rust worker threads execute JavaScript in V8 isolates. A shared
startup snapshot precompiles the Web API bootstrap and repository-local DOM,
parser and player helpers once. Each extraction restores a fresh isolate from
that immutable snapshot; healthy threads persist between requests. Extracted
player transforms are cached by player revision in Rust and shared by workers.
Successful extraction reports `extractor: "native_innertube_deno_core"`.

Axum, all HTTP requests, destination validation, PO-token orchestration and the
verified chunked downloader remain in Rust. The embedded runtime exposes only
the required Web APIs; it has no filesystem, subprocess, environment, fetch or
module-loader extensions. Native-to-MWEB fallback preserves the selected media
type, format ID, exact audio track, MIME type/codecs, and known content length.

Each JavaScript operation has a 15-second deadline, and an extraction has a
110-second total deadline. Cancellation and timeout interrupt the active isolate
through V8's thread-safe handle, then dispose it. Worker capacity stays reserved
until disposal actually finishes. Failed workers are replaced and idle threads
are stopped and joined when the pool is dropped. Each isolate has a 256 MiB
heap limit with a near-limit termination callback. Embedding shares the server
process, so fatal native failures or unrecoverable out-of-memory errors can
still terminate the application; these limits are not an OS process sandbox.

The player revision comes from the homepage configuration, using the MWEB ES6
variant; `/iframe_api` is a fallback when the homepage omits the player URL.
Only visitor/locale information is carried into the explicit MWEB client
context. Desktop experiment settings are excluded. YouTube may still return
format metadata without usable URLs or deny attestation. Those cases retain
sanitized errors/warnings; SABR decoding, DRM and authenticated playback remain
unsupported.

## Embedded runtime verification

Verified on 2026-10-26 with `deno_core` 0.413.0 and the development build, using
the original iOS audio selection for `P9iH4Wadwv4`, format `140`, track `es-US.4`.
The iOS URL was rejected, and the equivalent MWEB selection completed through
the Rust chunked downloader.

| Successful run | Headers ready | Complete download |
| --- | ---: | ---: |
| Cold snapshot, worker and transform cache | 2.543 s | 3.777 s |
| Warm 1 | 1.063 s | 1.839 s |
| Warm 2 | 1.068 s | 1.947 s |
| Concurrent 1 | 1.257 s | 2.781 s |
| Concurrent 2 | 1.023 s | 2.049 s |

All five full files contained **46,352,471 bytes** and matched SHA-256
`cd45d3dd7d1fbc6261d805957d6488660940531f94ce5e9af96801b21f2f275c`.
FFmpeg decoded the complete 2,864.065-second AAC file without errors. FFmpeg was
used only for verification and is not a server dependency. Middle, suffix and
open-ended resume responses were compared byte-for-byte against that file;
unsatisfiable ranges returned `416` with the correct file length.

These are local samples, not latency/availability guarantees. Additional probes
also observed upstream metadata-only responses producing sanitized errors; the
runtime migration does not add support for those formats. Worker IDs confirmed
thread reuse and use of both workers under concurrency. `/proc` inspection
confirmed the server had no child processes.

```sh
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --check
# Optional live extraction probe:
cargo test native_embedded_probe -- --ignored --nocapture
```

82 offline tests passed, with the optional live probe ignored. Coverage includes
runtime isolation, promises, cancellation, timeouts, heap-limit recovery, bounded
result conversion, thread shutdown, player transforms, fallback selection,
complete ordered transfers and range validation. The live endpoint checks above
were performed separately. Formatting and Clippy passed.

## Interactive API documentation

Once the server is running, open [Swagger UI](http://127.0.0.1:3000/swagger-ui/)
to explore and try all five endpoints. The generated OpenAPI 3.1 document is at
[/api-docs/openapi.json](http://127.0.0.1:3000/api-docs/openapi.json).
Swagger requests use the server's configured YouTube key; do not enter it in the UI.

The spec is generated from Rust code with `utoipa`: query structs derive
`IntoParams`, response structs derive `ToSchema`, and handlers declare their HTTP
metadata with `#[utoipa::path]`. `utoipa-axum` registers handlers and collects their
documentation together through `OpenApiRouter` and `routes!`, so paths do not need
to be maintained in a separate YAML/JSON file. Documented bounds describe the API;
the existing handler validation still enforces them. Add new documented handlers
through the same router and keep validation descriptions next to the relevant types.

Swagger UI assets are vendored and served locally, with no runtime CDN dependency
or separate Swagger download during the build. The documentation includes filters,
pagination, nullable metadata, the Shorts estimate, and JSON error responses.

