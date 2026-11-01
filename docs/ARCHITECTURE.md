# Architecture and delivery plan

## Product scope

A web platform for connecting, storing, preparing, querying, analysing and governing data. Billing/payments are excluded. The current release targets a self-hosted development installation with one application worker. It does not establish production readiness or superiority over Microsoft Fabric.

## Implemented architecture

| Area | Implementation |
|---|---|
| Browser | Same-origin JavaScript UI; dataset, SQL, version/quality, pipeline, connection, notebook, metric and dashboard views |
| Identity | Keycloak development realm; OIDC code flow + PKCE; validated signatures/issuer/audience/nonce; opaque server-side sessions |
| Isolation | Separate data directories and DuckDB/SQLite files per authenticated workspace; owner/editor/viewer membership |
| Metadata | Additive versioned SQLite migrations; session/security catalog separate from workspace metadata |
| Workspace storage | Immutable DuckDB version tables and stable published views; retained source files and hashes |
| Processing | Child-process parsing/materialisation/extraction; parent deadlines, engine limits, OS resource guards |
| Publication | Durable writing/ready journal; idempotent recovery; quality gates before view publication |
| Sources | Four file formats; read-only PostgreSQL snapshots and integer-watermark append extraction |
| Pipelines | Durable SQL/source definitions and run history; manual runs/retries; embedded interval scheduler or Dagster adapter |
| Lakehouse | Digest-pinned S3-compatible SeaweedFS, Iceberg REST catalog and Trino; immutable table publication and snapshots |
| Query service | Read-only DuckDB; Trino SELECT AST policies allow only registered tables in the current workspace |
| Analysis | Revisioned SQL/Python notebooks, scalar metric definitions and table/metric dashboards; explicit execution |
| Operations | Request IDs, request/catalog metrics, reconciliation findings, encrypted offline application backup/restore |

PostgreSQL is currently an identity database and a source connector, not the platform's control-plane metadata backend. Trino complements the workspace engine; it does not replace all DuckDB paths. The REST catalog is the verified development fixture; Apache Polaris has not been integrated. These distinctions are intentional and visible in the delivery matrix.

## Module boundaries

- `catalog.py`: additive migrations and short-lived metadata transactions.
- `flows.py`: background dependency workflows, frozen run plans/input versions, per-step journals, cooperative cancellation, explicit restart recovery and detached daily SQL backfills. Metadata-only workflow controls bypass dataset-recovery locks so progress/cancellation remain responsive during execution.
- `cdc.py`: pgoutput v1 transactions, exported-snapshot bootstrap, primary-key reconciliation, source identity/publication checks and post-publication WAL acknowledgement. Selected source resources are named by the connector UUID and verified before cleanup.
- `notebooks.py`: bounded version snapshots and disposable offline Docker Python runs; journaled status/output and interrupted-container recovery. No platform data directory or Docker socket is mounted in a runner.
- `workers.py` / `ingestion.py`: bounded parsing, SQL materialisation and PostgreSQL extraction.
- `workflows.py`: versions, stable views, publication journals, recovery, contracts, lineage and reconciliation.
- `pipelines.py`: durable run lifecycle, overlap prevention, retries, interval scheduling and external request keys.
- `connectors.py`: allowlisted PostgreSQL connections, encrypted credentials and checkpoint publication.
- `lakehouse.py`: Trino protocol client, Iceberg copy lifecycle, type mapping and workspace SQL policies.
- `auth.py`: OIDC verification, session/token lifecycle, membership enforcement and workspace routing.
- `analysis.py`: typed notebook/dashboard/metric definitions, optimistic revisions and explicit item execution.
- `limits.py`, `locking.py`, `observability.py`: request limits, directory leases and aggregate diagnostics.
- `main.py`: application assembly, APIs and remaining shared handlers.

## Publication invariants

A version receives a generated physical table and immutable source directory. A writing journal precedes storage creation. Validation/profile/quality results are prepared before the journal becomes ready. The stable DuckDB view changes only for a ready version; the catalogue, version history, source metadata, connector checkpoint and pipeline result are then committed together. Ready records can be completed after process death; incomplete records are cleaned up. API operations settle outstanding publication journals before exposing data.

There is no cross-database distributed transaction. Recovery markers bridge the DuckDB/SQLite boundary. The single operation lock serialises writes and queries. Multi-process application writers are explicitly unsupported; data-directory leases prevent accidental second workers.

Deletion retains its marker until version tables, source directories and metadata have been removed. Source/pipeline ownership, dependent version lineage and remote lakehouse copies must be resolved first. Original saved-query text is retained and can fail after a referenced dataset is deliberately removed.

## Source and orchestration semantics

PostgreSQL extraction uses repeatable-read, read-only transactions and a statement timeout. Connection hosts/ports are server-allowlisted; credentials are encrypted with a workspace-specific private key. Integer incremental watermarks require unique non-null values. Checkpoints and schema descriptors advance only with publication; quality failure leaves both unchanged. Empty incremental refreshes produce a successful no-op. This does not implement updates, deletes, late-arrival replay or CDC.

Pipelines can call SQL materialisation or a registered source connector. A run records its SQL/connector identity. Retries use the recorded operation against current authorized inputs and the current output target. Successful publications record exact input version lineage. This is not a general dependency-graph/backfill engine.

The Dagster sensor polls enabled definitions, submits stable run keys and calls the workspace API with a scoped automation token. API idempotency keys prevent a completed external retry from publishing another version. Embedded scheduling is disabled when external orchestration is selected. Token rotation and service uptime remain operator responsibilities.

## Access boundaries

Application roles apply to metadata, file downloads, workers and APIs. Workspace files are physically separate. Trino user SQL must parse to a single SELECT/CTE/set operation; only registered published tables in the assigned schema are allowed. System schemas, other workspace schemas, table functions and writes are rejected. Engine choice persists with saved queries and run history.

Direct development Trino, catalog, PostgreSQL, object-store and Dagster endpoints are trusted-operator interfaces bound to loopback. The application policy layer is not a replacement for production engine/catalog/object-store identity policies. Public deployments require TLS, restricted network exposure, reviewed identity policy, quotas and deployment-specific verification.

## Analytical interface contract

Dashboard widgets are HTML metric values and accessible tables, not high-density animated charts. Up to eight widgets refresh sequentially through bounded queries. Table previews cap DOM density at 50 rows × 12 columns; query caps and timestamps remain visible. Last good values remain visible and explicitly stale after a refresh error. Editing controls collapse behind the saved evidence on mobile. Saved URLs restore definitions without executing SQL. Notebooks provide SQL and text notes; arbitrary Python is not supported.

## Conditional later expansion

Python notebooks require a dedicated hostile-code execution design and acceptance tests. CDC/streaming require source/delete/late-arrival semantics and a latency target. Spark/Flink should follow a measured workload that exceeds the current engine envelope. AI assistance requires a selected provider/model, credentials and explicit execution/authorization boundaries. These items remain open requirements rather than simulated capabilities.

## Evidence for advantages

Measure time to first query, failed-refresh visibility, recovery time, import correctness, query latency at stated concurrency and resource cost. Functional tests do not establish comparative performance or production safety. Publish benchmarks only with dataset sizes, hardware, pinned versions and query definitions.
