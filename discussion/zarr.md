# Draft: Zarr Interface Workers (gribjump + index-mapper)
## Requirements (confirmed)
- Two new workers following existing polytope-server Processor trait pattern
- Worker 1 (gribjump): batch requests with shared spans → ordered bytes + axes/view definition
- Worker 2 (index-mapper): batch requests with one feature → index mapping to global array
- Both inspired by FDB ChunkedDataView pattern
- Purpose: backend for a Zarr interface
## Technical Decisions
- Both workers implement `Processor` trait from `workers/common`
- Separate Rust crates under `workers/`
- Follow fdb-worker streaming pattern (mpsc channel → reqwest::Body)
## Research Findings
- **Polytope-server worker pattern**: Processor trait, WorkItem(job_id, request JSON, user, metadata) → Completion(content_type, body stream). Polling broker, heartbeat, streaming completion.
- **FDB ChunkedDataView**: ViewPart = MARS request + axes + extractor. AxisDefinition = {keys, chunking_type}. Axis = parameters with values, size, chunked flag. IndexMapper maps axis indices to buffer offsets (row-major, skipping chunked axes).
- **GribExtractor pattern**: Iterates ListIterator(Key, DataHandle), computes buffer index per key via axis.index() → axis_index_to_buffer_index(), copies GRIB float values into buffer at correct offset.
- **FDB Zarr integration (z3fdb)**: FdbSource wraps ChunkedDataView, FdbZarrStore implements zarr.abc.store.Store. Chunk key "c/i/j/k" → ChunkedDataView.at(index) → bytes.
- **GribJump**: C/Python library for extracting 1D index ranges from GRIB fields. API: extract(requests with ranges) → (values[], masks[]).
- **No gribjump Rust crate** exists in workspace.
## Open Questions
- GribJump access method: FFI from Rust, Python subprocess, or FDB retrieve+inspect?
- Request JSON payload format: What do "requests" and "spans" look like?
- Worker 2 response format: JSON indices? Binary?
- "Feature" meaning: single param/variable?
- Axes/view definition wire format: JSON AxisDefinition?
## Scope Boundaries
- INCLUDE: Two new worker crates, Cargo workspace integration, skaffold Docker builds
- EXCLUDE: Zarr store frontend/client, FDB schema changes, gribjump library development
Here's what I've pieced together from the codebase exploration. Let me verify my understanding and ask the few things I couldn't resolve from the code.
My interpretation of the two workers:
1. Gribjump worker — receives a batch of MARS-like requests that share the same grid index ranges ("spans"), plus an axes/view definition (JSON form of FDB's AxisDefinition). Uses gribjump to extract the data for each request at those spans, writes the bytes into a buffer respecting the axis ordering (like GribExtractor.writeInto does), and returns both the data bytes and the axes/view metadata.
2. Index-mapper worker — receives a batch of requests for a single variable/param ("feature"), plus axes/view definitions. Computes, for each request, its position in the global N-D array (like IndexMapper.axis_index_to_buffer_index), and returns that mapping so a Zarr client knows where each chunk lands.