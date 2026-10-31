# Delivery matrix

This checklist distinguishes implemented development features from conditional future work. It is not a claim that every item in an open-ended Fabric-scale roadmap is complete.

| Area | Development implementation | Evidence |
|---|---|---|
| Import correctness | Strict CSV records, integer preservation, file/schema bounds | Backend regression suite |
| Bounded processing | Spawned import/transform/source workers, deadlines and resource guards | Real workers exercised by backend and live-stack checks |
| Versioning | Immutable tables/sources, stable SQL names, replacement and restore | Version/restore/restart tests and OIDC browser workflow |
| Recovery | Additive migrations, publication/deletion journals, reconciliation reports | Injected failure/restart/retry tests |
| Quality | Non-null, uniqueness, numeric bounds; publication/checkpoint gates | Backend and live PostgreSQL failure/retry checks |
| Transformations/lineage | Full-result materialisation and recorded input version IDs | Backend and browser checks |
| Pipelines | SQL/source runs, retries, schedules, pause/edit/delete and request keys | Backend scheduler tests and live source pipelines |
| Orchestration | Dagster adapter, scoped token, sensor scheduling and idempotent replay | `verify_dagster.py` actual job + daemon run |
| PostgreSQL | Read-only snapshots/integer increments; encrypted credentials; drift checks | `verify_stack.py` and real browser import |
| Lakehouse | S3-compatible storage, REST catalog, Trino, immutable copies and snapshot inspection | Actual publication, aggregates, schema evolution and time travel |
| Trino policy | Single read-only SELECT; owned table/schema allowlist; preserved ordering/caps | Dedicated AST policy tests plus live API queries |
| Identity/workspaces | Real OIDC/PKCE, roles, membership, sessions and revocable automation tokens | Signature/audience/nonce/revocation/isolation tests and real Keycloak browser login |
| Saved analysis | Query CRUD, engine persistence, SQL/Python notebooks, scalar metrics, dashboards | Backend and browser suites |
| Python notebooks | Offline disposable containers, selected input versions, shared variables within run-all, stdout/tables, durable history and bounds | Runner contracts, real Docker isolation/timeout/flood checks and browser workflow |
| Dashboard usability | Explicit refresh, stale data, timestamps, mobile previews, text alternatives | `analysis-browser.cjs` and visual inspection |
| Observability | Request IDs, HTTP/catalog counts and readiness/recovery findings | Backend metrics tests |
| Application backup | Offline leases, streamed AES-GCM encryption, manifest checks, safe restore | Restore/integrity/overwrite/live-lock tests |
| Self-hosted stack | Digest-pinned services, generated ignored secrets, protocol readiness | Stack startup and live integration scripts |

## Explicitly not completed

- Jupyter protocol/persistent kernels, user-installed notebook packages and production sandboxes for hostile public tenants. Python notebook cells are implemented with development container isolation.
- CDC, streaming, update/delete replication, late-arrival replay and streaming SLAs.
- Spark/Flink workloads and distributed processing.
- AI provider integration, model credentials and AI execution policy.
- A full shared semantic-model language beyond scalar SQL metric definitions.
- General pipeline dependency graphs, partition backfills and multi-node application coordination.
- PostgreSQL control-plane metadata migration, Apache Polaris integration and production engine/object-store-side tenant policies.
- Production disaster-recovery acceptance covering application data plus PostgreSQL/catalog/object-store volumes together.
- Production security review, deployment hardening and competitive performance benchmarks.

Python notebook cells are now implemented. Live database updates are the next planned expansion; CDC/streaming, distributed processing and AI still need concrete acceptance criteria. None is represented by a fake success state or an inert product button.

## Verification commands

See README for unit, legacy browser, analysis browser, real OIDC workflow, source-to-lakehouse and Dagster checks. Live-stack tests use synthetic development data and are not silently treated as part of the default offline test suite. Authentication keys, local passwords and tokens remain in ignored runtime files and are not committed.

## Development validation — 2026-10-31

- Python 3.12 backend suite: **119 passed**, including the real Docker notebook checks with a configured test image. Without that image, the four live-Docker tests explicitly skip. Two dependency deprecation warnings remain (pytest/AnyIO and Starlette's httpx test-client adapter).
- Chromium checks cover existing import/query/export workflows, saved analysis, responsive layout, and the real OIDC/source/lakehouse workflow. The Python notebook browser check additionally verifies selected inputs, shared/fresh sessions, stdout/tables, retained history, non-executing reload, text-safe output and mobile layout.
- Live services verify signed S3 operations, PostgreSQL incremental publication and failed-quality checkpoint safety, Iceberg replay, Trino policy, schema evolution and snapshot time travel.
- The Dagster verification runs an authenticated job, replays its idempotency key, and observes a daemon-driven scheduled run before pausing it.
- Python compilation, JavaScript syntax and Git whitespace checks pass. CI is configured for Python 3.11 and 3.12; only 3.12 was run in this workspace.

These are development checks against synthetic fixtures, not production load, penetration or coordinated disaster-recovery acceptance tests.
