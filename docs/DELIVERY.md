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
| Multi-step workflows | Dependency validation, SQL/source steps, inline checks, pinned-version Iceberg publication, asynchronous progress, safe retry/cancel/pause and detached daily backfills | Workflow regression/browser suites; live source workflow; actual Dagster job/sensor and Iceberg version checks |
| Orchestration | Dagster adapter, scoped token, sensor scheduling and idempotent replay | `verify_dagster.py` actual job + daemon run |
| PostgreSQL | Read-only snapshots/integer increments; encrypted credentials; drift checks | `verify_stack.py` and real browser import |
| Scheduled PostgreSQL CDC | Exported snapshot + pgoutput, inserts/updates/deletes/truncation, quality-gated checkpoints, restart/ack recovery, health/history, password rotation and source-resource cleanup | Real PostgreSQL protocol/integration suite; scheduled-sync browser workflow |
| Lakehouse | S3-compatible storage, REST catalog, Trino, immutable copies and snapshot inspection | Actual publication, aggregates, schema evolution and time travel |
| Trino policy | Single read-only SELECT; owned table/schema allowlist; preserved ordering/caps | Dedicated AST policy tests plus live API queries |
| Identity/workspaces | Real OIDC/PKCE, roles, membership, sessions and revocable automation tokens | Signature/audience/nonce/revocation/isolation tests and real Keycloak browser login |
| Shared business models | Revisioned star models, checked many-to-one joins, reusable aggregates/ratios, typed filters, pinned inputs/history and metric/dashboard bindings | Semantic backend, authorization and Chromium tests |
| Saved analysis | Query CRUD, engine persistence, SQL/Python notebooks, scalar metrics, dashboards | Backend and browser suites |
| Python notebooks | Offline disposable containers, selected input versions, shared variables within run-all, stdout/tables, durable history and bounds | Runner contracts, real Docker isolation/timeout/flood checks and browser workflow |
| Dashboard usability | Explicit refresh, stale data, timestamps, mobile previews, text alternatives | `analysis-browser.cjs` and visual inspection |
| Observability | Request IDs, HTTP/catalog counts and readiness/recovery findings | Backend metrics tests |
| Application backup | Offline leases, streamed AES-GCM encryption, manifest checks, safe restore | Restore/integrity/overwrite/live-lock tests |
| Self-hosted stack | Digest-pinned services, generated ignored secrets, protocol readiness | Stack startup and live integration scripts |

## Explicitly not completed

- Jupyter protocol/persistent kernels, user-installed notebook packages and production sandboxes for hostile public tenants. Python notebook cells are implemented with development container isolation.
- Sub-second streaming SLAs, multi-table transactional publication, partitioned/RLS/generated-column CDC, additional source engines, general late-arrival/event-time processing and workloads beyond the bounded PostgreSQL CDC implementation.
- Spark/Flink workloads and distributed processing.
- AI provider integration, model credentials and AI execution policy.
- A full semantic expression language, composite/multi-hop relationships, multiple fact tables and model-specific row policies. Bounded shared star models and reusable measures are implemented.
- Distributed/multi-node workflow execution, hourly/custom partition schemes, automatic missed-schedule catch-up and historical reconstruction of external sources. Bounded single-worker dependency graphs and daily SQL backfills are implemented.
- PostgreSQL control-plane metadata migration, Apache Polaris integration and production engine/object-store-side tenant policies.
- Production disaster-recovery acceptance covering application data plus PostgreSQL/catalog/object-store volumes together.
- Production security review, deployment hardening and competitive performance benchmarks.

Python notebooks, scheduled PostgreSQL live updates and multi-step workflows are implemented. Broader semantic expressions, streaming, distributed processing and AI remain planned expansion areas. None is represented by a fake success state or an inert product button.

## Verification commands

See README for unit, legacy browser, analysis browser, real OIDC workflow, source-to-lakehouse and Dagster checks. Live-stack tests use synthetic development data and are not silently treated as part of the default offline test suite. Authentication keys, local passwords and tokens remain in ignored runtime files and are not committed.

## Development validation — 2026-10-31

- Python 3.12 backend suite: **119 passed**, including the real Docker notebook checks with a configured test image. Without that image, the four live-Docker tests explicitly skip. Two dependency deprecation warnings remain (pytest/AnyIO and Starlette's httpx test-client adapter).
- Chromium checks cover existing import/query/export workflows, saved analysis, responsive layout, and the real OIDC/source/lakehouse workflow. The Python notebook browser check additionally verifies selected inputs, shared/fresh sessions, stdout/tables, retained history, non-executing reload, text-safe output and mobile layout.
- Live services verify signed S3 operations, PostgreSQL incremental publication and failed-quality checkpoint safety, Iceberg replay, Trino policy, schema evolution and snapshot time travel.
- The Dagster verification runs an authenticated job, replays its idempotency key, and observes a daemon-driven scheduled run before pausing it.
- Python compilation, JavaScript syntax and Git whitespace checks pass. CI is configured for Python 3.11 and 3.12; only 3.12 was run in this workspace.

These are development checks against synthetic fixtures, not production load, penetration or coordinated disaster-recovery acceptance tests.

## Live-update validation — 2026-11-01

- Full Python 3.12 regression suite with real PostgreSQL CDC and the Python runner image enabled: **133 passed**. Final focused CDC/authentication checks: **24 passed**, including source timeline/permission checks and workspace control boundaries.
- The Chromium CDC workflow waited for an actual 60-second embedded schedule, captured one insert/update/delete, refreshed a saved dashboard against the stable dataset view, inspected health/history, rotated the encrypted password, paused scheduling and cleaned source resources. Mobile overflow checks passed.
- Existing import/query/export browser checks and the authenticated Dagster job, idempotent replay and daemon-driven sensor checks passed after restarting the development services.
- Source tests cover concurrent writes during exported-snapshot bootstrap, complete-transaction batch boundaries, unchanged external TOAST values, rollback, primary-key changes, truncation/empty tables, quality rejection without checkpoint advancement, restart after acknowledgement failure, schema/publication drift, missing slots, explicit reset/stop and cleanup.
- The default offline suite skips eleven live PostgreSQL tests unless `DATA_PLATFORM_TEST_CDC=1`; a separate CI job prepares PostgreSQL with logical WAL and runs them. The four real Python-container tests still require their explicit image setting.
- PostgreSQL 17 is the tested development server. PostgreSQL 15+ is required by the implementation; alternate versions, failover infrastructure and production throughput remain deployment acceptance work.

## Multi-step workflow validation — 2026-11-01

- Full Python 3.12 suite with live PostgreSQL and Python-container checks: **149 passed**. The workflow tests cover dependency validation, blocked/independent branches, frozen retries, interrupted-run recovery, cancellation/pause, input pins, empty/detached dated outputs and protection against overwriting newer live results.
- Chromium verifies the workflow editor, dependency/quality configuration, schedule pause, asynchronous status, pinned-version queries, two daily backfills, saved snapshots and mobile layout. Existing import/query/export browser checks also pass.
- A real PostgreSQL source workflow verifies ingestion → transformation → quality and preserves the source version on no-op refreshes.
- A live S3/Iceberg/Trino check publishes the checked version containing `5` after the native dataset is deliberately advanced to `99`, verifies remote `5`, confirms idempotent replay, recovers a deliberately interrupted staging table and cleans its exact remote objects.
- The actual Dagster `platform_flow` job and repeated run key pass. A separate daemon/sensor test completes a scheduled workflow while the application's embedded scheduler is disabled. These local adapter tests are separate from the existing OIDC authentication tests.
- Compilation, frontend syntax and Git whitespace checks pass. Distributed execution, production load/security acceptance, and historical reconstruction of external databases remain outside this release.

## Shared business model validation — 2026-11-01

- Regression run: **142 passed, 16 skipped**. Skips are the opt-in live PostgreSQL CDC and Python Docker tests; those integrations were not rerun for this feature. After the final binding-validation change and two additional cases, the semantic/authorization subset passed **23 tests**.
- Tests cover non-executing saves, direct lookup joins, duplicate/null key rejection before aggregation, unmatched rows, all aggregate families, ratios/zero denominators, integer preservation, typed/escaped filters, ordering/caps, same-schema refresh provenance, schema/revision drift, document references, retained history/restarts, dataset deletion guards and workspace/viewer policies.
- Chromium passes the new model editor → grouped query → shared metric/dashboard flow, including duplicate-key stale-result fallback and mobile layout. Existing analysis and import/query/export browser suites also pass. JavaScript syntax, Python compilation and diff whitespace checks pass.
- Scope is a bounded local star-model implementation, with limitations documented in README. No environment publication was performed.
