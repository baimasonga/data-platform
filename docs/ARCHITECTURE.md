# Architecture and delivery plan

## Product intent

One platform for connecting, storing, preparing, querying, analysing and governing data. No billing or payment functionality is in scope. The product should make data provenance, quality, reproducibility and resource usage understandable.

## Implemented foundation

A same-origin browser application calls a FastAPI service. SQLite stores control metadata; DuckDB stores analytical tables. Import services validate CSV record widths, preserve nullable integers, infer schemas and compute basic profiles. Original source bytes and SHA-256 checksums are retained for new imports. File parsing is separated in `app/ingestion.py`; it still executes in the service thread pool rather than a resource-bounded worker. Read-only query workers execute in separate spawned processes. The server serialises these operations because the shared native DuckDB database is intentionally a single-process writer design.

This is an explicit development adapter, not an implementation of the distributed architecture. The backend is grouped in one module to keep the initial runnable slice small. Refactor into catalog, ingestion, query and policy interfaces before adding engines.

## Target architecture

| Area | Target | Current implementation |
|---|---|---|
| User interface | Unified dataset, SQL, pipeline and governance workspace | Dataset and SQL workbench |
| API | FastAPI with explicit service boundaries | FastAPI app factory |
| Metadata | PostgreSQL with migrations | SQLite bootstrap schema |
| Storage | S3-compatible object storage | Local DuckDB database |
| Table format | Iceberg over Parquet | Native DuckDB tables |
| Table catalog | Compatible Iceberg REST catalog; evaluate Apache Polaris | Local metadata registry |
| Shared SQL | Trino behind policy-aware query service | Isolated DuckDB query processes |
| Lightweight processing | Bounded DuckDB workers | Implemented for queries; imports still in service |
| Ingestion | Versioned connectors and resumable jobs | Four local file formats |
| Orchestration | Dagster adapter | Not implemented |
| Identity | OIDC with enforced workspace membership | Local single-user only |
| Quality | Contracts, assertions and publication gates | Null/distinct/completeness profiles |
| Observability | Structured logs, metrics and traces | HTTP logs and basic activity records |

No drop-in backend swap is claimed: SQL dialect, type mapping, permissions, transactional publication and catalogue identity all require migration work.

## Query execution history

Workbench/API SQL requests create a persisted run record before waiting for the query lock. Completion stores status, total wall-clock duration, returned-row count, truncation and errors; restart marks incomplete runs interrupted. Dataset previews do not create history records. The browser offers SQL search, status filters and explicit reopening into the editor. History is not a result cache, query scheduler or dataset-version snapshot.

## Dataset management

The browser supports display-name changes without changing SQL table identity or source provenance. Permanent deletion requires the current display name in the API request. Cleanup uses a persisted SQLite deletion marker before dropping the DuckDB table, removing the retained source, and deleting catalogue metadata. Retry and startup recovery are idempotent. Failed cleanup remains visible in the catalogue. Import history and saved-query text are retained; deleted datasets are no longer linked from import cards. This does not implement dataset versioning or soft deletion.

## Next milestone: durable service boundaries

1. Split ingestion, catalog and query code into service interfaces.
2. Add versioned metadata migrations and reconciliation of orphaned analytical tables.
3. Extend the implemented import attempt history (running, succeeded, failed, interrupted) into resumable ingestion jobs with staged, validated and published states. The web Imports page already exposes status, validation errors and dataset links; background execution and automatic retries remain planned.
4. Move file parsing into bounded workers; enforce request limits before multipart parsing.
5. Extend retained original files and checksums with versioned import settings and source schemas.
6. Add immutable dataset versions and atomic published-version pointers.
7. Materialise SQL transformations with recorded inputs and query definitions.

Acceptance: interrupt and retry an import without duplicates; recover after a crash between data write and metadata publication; failed validation leaves the old published version available.

## Milestone: lakehouse compatibility spike

Before migrating product storage, provision S3-compatible storage, an Iceberg REST catalog and Trino in an isolated integration environment. Pin compatible versions. Verify table creation, append, schema evolution, snapshot selection, time travel, concurrent commit conflict handling, object-store permissions and restoration. Define dataset UUID-to-catalog mappings and supported type conversions.

Acceptance: migrate a dataset and reproduce aggregate results; failed writes remain invisible; catalog and storage recovery works together. Snapshot history alone does not provide backup.

## Milestone: connectors and pipelines

Start with PostgreSQL read-only extraction, initial snapshots and watermark-based incremental loads. Record watermark advancement only after successful publication. Document deletes and late-arrival semantics before claiming change data capture. Introduce Dagster behind platform-owned APIs for schedules, dependencies, retries and backfills. Keep secrets encrypted and out of run logs.

Acceptance: source interruption and retry do not duplicate output; schema drift is surfaced; failed quality checks block publication; dependent jobs run against recorded inputs.

## Milestone: multi-user security

Add OIDC, organisations, workspaces and membership roles. Enforce authorisation in APIs, workers, query engines, catalog and object storage. Shared SQL access must not bypass table policies. Restrict source connector egress; prevent SSRF and credential leakage. Cache and export identities must include the authorised dataset version and policy context.

Acceptance: independent tenants cannot read, infer, query or export each other's data; expired jobs cannot retain credentials; integration tests exercise every route and data path.

## Later expansion

Shared semantic metrics, richer dashboards, notebooks in isolated execution environments, CDC, streaming and distributed processing. Choose Spark or Flink only after measured workloads justify them. AI assistance follows policy enforcement and reliable metadata: query previews, explicit execution and no unrestricted tool access.

## Evidence for claiming an advantage

Track time to first successful query, pipeline recovery time, failed-refresh visibility, import correctness, query latency at stated concurrency and resource usage per completed workload. Publish reproducible benchmarks with dataset size, hardware, engine versions and query definitions. The current project does not establish superiority over Microsoft Fabric.
