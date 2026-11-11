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
| Multi-step workflows | Dependency validation, SQL/source steps, inline checks, pinned-version Iceberg publication, asynchronous progress, safe retry/cancel/pause and detached hourly/daily/weekly/monthly backfills and opt-in completed-partition catch-up with durable checkpoints/generations | Workflow regression/browser suites; live source workflow; actual Dagster job/sensor and Iceberg version checks |
| Orchestration | Dagster adapter, scoped token, sensor scheduling and idempotent replay | `verify_dagster.py` actual job + daemon run |
| PostgreSQL | Read-only snapshots/integer increments; encrypted credentials; drift checks | `verify_stack.py` and real browser import |
| Scheduled PostgreSQL CDC | Exported snapshot + pgoutput, inserts/updates/deletes/truncation, quality-gated checkpoints, restart/ack recovery, health/history, password rotation and source-resource cleanup | Real PostgreSQL protocol/integration suite; scheduled-sync browser workflow |
| Lakehouse | S3-compatible storage, REST catalog, Trino, immutable copies and snapshot inspection | Actual publication, aggregates, schema evolution and time travel |
| Trino policy | Single read-only SELECT; owned table/schema allowlist; preserved ordering/caps | Dedicated AST policy tests plus live API queries |
| Identity/workspaces | Real OIDC/PKCE, roles, membership, sessions and revocable automation tokens | Signature/audience/nonce/revocation/isolation tests and real Keycloak browser login |
| Shared business models | Revisioned star models, checked many-to-one joins, reusable aggregates/ratios and arithmetic calculations with conditional/null-handling functions, typed filters, pinned inputs/history and metric/dashboard bindings | Semantic backend, authorization and Chromium tests |
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
- A full semantic expression language with arbitrary functions/time intelligence, composite/multi-hop relationships, multiple fact tables and model-specific row policies. Bounded shared star models, reusable measures, arithmetic calculations and four conditional/null-handling functions are implemented.
- Distributed/multi-node workflow execution, arbitrary/fiscal partition schemes and historical reconstruction of external sources. Bounded single-worker dependency graphs and hourly/daily/weekly/monthly SQL backfills and bounded automatic catch-up are implemented.
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


## Calculated measure validation — 2026-11-01

- Regression suite: **173 passed, 16 skipped**. The opt-in live PostgreSQL/Python Docker checks were not rerun. Final calculation-specific checks passed **30 tests**, including an additional large-integer test and actual worker execution of the maximum 30-measure dependency chain.
- Coverage includes unsupported syntax/code rejection, expression bounds, unknown IDs, cycles, numeric types, dependency order, grouped totals, filter scope, zero denominators/nulls, precision, model snapshots, scalar widgets, shared metrics and revision rebinding.
- Chromium passes formula editing, validation-error draft retention, nested profit/margin calculations, recorded output, shared metric refresh and stale-revision rebinding, plus mobile overflow checks. Existing model and analysis browser suites also pass. Python compilation, JavaScript syntax and Git whitespace checks pass.
- Calculated measures use bounded arithmetic rather than a full semantic-language runtime. No environment publication was performed.

## Hourly backfill validation — 2026-11-01

- Full Python regression suite: **191 passed, 16 skipped**. Opt-in live PostgreSQL CDC and Python Docker checks were not rerun for this feature.
- New tests cover UTC normalization, repeated daylight-saving hours, midnight/leap-day boundaries, whole-hour/range bounds, half-open SQL windows, canonical offset idempotency, pre-upgrade daily request keys, isolated outputs/live-target protection, empty hours, source rejection, atomic queue limits, frozen retry after restart, legacy daily-schema migration and workspace/viewer controls.
- Chromium passes hourly range selection, daily/hourly field retention through editor rerenders, separate outputs, saved windows/snapshots, rejection without extra runs and mobile layout. Existing daily workflow and import/query/export browser checks also pass. Compilation, frontend syntax and whitespace checks pass.
- Scope is explicitly requested SQL backfills of up to 48 UTC hours per batch. Automatic schedules still use daily partitions; custom partition schemes and missed-schedule catch-up remain outside this release. No environment publication was performed.

## Conditional measure validation — 2026-11-01

- Full serial regression: **225 passed, 16 skipped**. Opt-in live PostgreSQL CDC and Python Docker checks were not rerun for this feature.
- Function-specific checks cover argument counts, unsupported calls/syntax, positional-only arguments, rounding bounds, numeric inputs, condition/branch dependencies, cycles, node/depth limits, grouping/filter/null behavior, integer precision, scalar metrics, grouped dashboards and retained output after restart.
- Chromium passes the function reference, invalid-call draft retention, nested conditions/functions, null/zero handling, shared dashboards, non-executing reload and mobile layout. Existing calculation/rebinding and analysis browser checks pass. The formula editor now supports multiline input and stacks function help for narrow screens.
- The initial full regression run had one lookup-validation query hit its existing 10-second deadline during concurrent browser checks. The affected restart test passed in isolation; the deadline remains unchanged. The subsequent full serial regression passed.
- This release adds only the fixed numeric functions `abs`, `round`, `coalesce` and `if_else`; arbitrary functions and time intelligence remain outside scope. No environment publication was performed.

## Automatic workflow catch-up validation — 2026-11-02

- Full regression suite: **239 passed, 16 skipped**. Opt-in live PostgreSQL CDC and Python Docker checks were not rerun for this feature.
- Focused scheduler/authorization tests cover closed UTC windows, bounded backlog planning, atomic checkpoint/queue commits, injected rollback, concurrent dispatch, workspace capacity, source rejection/drift, pause/resume, explicit generation restart, stale revisions, idempotent replay and restart recovery.
- Chromium verifies opt-in configuration, metadata-only previews, actual embedded scheduling, separate output datasets, pause, visible failure blocking/retry and mobile layout. Existing daily and hourly workflow browser suites also pass. Async history/status reads use consistent metadata snapshots and ignore stale UI responses.
- The actual Dagster catch-up sensor/job verifies scheduled policy dispatch, multiple accepted partitions, waiting for batch completion, idempotent replay, later re-dispatch after an unaccepted poll and successful empty polls. The existing manual workflow job also passes with replay. These are local adapter checks, not production daemon/failover acceptance or external-source historical reconstruction.
- Scope is opt-in, single-worker, completed UTC day/hour catch-up over retained SQL inputs. Explicit checkpoint restarts retain earlier generations and outputs. No environment publication was performed.

## Calendar partition validation — 2026-11-02

- Full regression suite: **260 passed, 16 skipped**. Opt-in live PostgreSQL CDC and Python Docker checks were not rerun for this feature.
- Calendar-specific tests cover Gregorian leap-year/century boundaries, Monday alignment, ISO-year transitions, inclusive range limits, completed-only catch-up, half-open SQL filters, input-version pinning, isolated live targets, idempotent requests, restart/retry and viewer restrictions.
- Chromium passes weekly/monthly selection and retained ranges, real leap-month/year-boundary runs, invalid-Monday rejection, automatic monthly scheduling, checkpoint status and mobile layout. Existing daily, hourly and automatic catch-up browser suites pass.
- Actual Dagster sensor/job checks pass for weekly and monthly batches, including replay, later re-dispatch, batch completion and empty polls. Compilation, frontend syntax and whitespace checks pass.
- Limits are 26 Monday-start UTC weeks or 24 real UTC calendar months per batch. Fiscal/business calendars, alternate time zones and arbitrary custom partition rules remain outside scope. Existing metadata fields and daily/hourly request keys are retained; no new schema migration or environment publication was performed.

## Application readiness validation — 2026-11-11

- Full Python 3.12 regression with real PostgreSQL CDC and the digest-resolved Python container image enabled: **276 passed, no skips**. One Starlette test-client deprecation warning remains. Calendar/hourly timestamp regressions also passed separately (**38 tests**).
- Declared `pytz` directly in the application dependencies. DuckDB timestamp materialization requires it; pandas 3 no longer supplies it transitively. This removes the need for the separate onboarding installation workaround. Editable installation and dependency consistency checks pass.
- The pinned self-hosted services passed protocol readiness. `verify_stack.py` passed OIDC discovery, S3 operations, PostgreSQL snapshot/incremental/no-op extraction, quality-gated checkpoint safety, Iceberg publication, Trino aggregates, schema evolution and snapshot time travel.
- Chromium's authenticated workflow passed real Keycloak login, workspace switching, versions/restore, quality gates, transformations, pipelines, PostgreSQL import, Iceberg snapshots, Trino policy, lakehouse cleanup and scoped automation-token creation.
- The real-container notebook browser check passed selected dataset inputs, shared run-all variables, fresh individual sessions, stdout/tables, durable history, non-executing reload, revision guards, text-safe output and mobile layout.
- Actual Dagster workflow execution/replay and weekly/monthly catch-up passed. The authenticated pipeline job/replay and a daemon-driven scheduled run passed with the application's embedded scheduler disabled. `verify_flow_lakehouse.py` passed pinned-version publication, replay and interrupted staging recovery/cleanup.
- Python compilation, frontend/browser-script syntax and Git whitespace checks pass. Generated local credentials and verification data remain in ignored runtime/data directories. No environment configuration changes or publication were performed.

### Next delivery priorities

1. Extend the verified development cold-recovery drill to deployment-specific disaster recovery: off-site retention, realistic data sizes, independently hosted sources, recovery objectives and restore procedures.
2. Complete deployment security acceptance: TLS/OIDC configuration, service/network isolation, secret rotation, and engine/object-store tenant-policy review. The development stack remains loopback-only.
3. Select a bounded product expansion against an explicit use case; broader semantic expressions, AI, additional sources and distributed execution remain separate roadmap work rather than completed capabilities.

## Coordinated cold-recovery validation — 2026-11-11

- Added `scripts/verify_recovery.py`, a repeatable synthetic drill using private, disposable source and restore Docker projects. It never stops or restores into the existing development stack.
- The final end-to-end drill passed in **171.46 seconds** (before final resource cleanup). It stopped the application and source services, captured PostgreSQL/catalog/object-storage volumes, encrypted the application/configuration/volume bundle, authenticated it on restore, and recovered into fresh volumes with the pinned service images.
- Verified both immutable native tables and original sources, retained dataset/version IDs, PostgreSQL rows, connector-key/password recovery and source re-import, retained Keycloak identity ID, Trino aggregates for both published versions, and unchanged Iceberg snapshot IDs.
- Backup and recovery safety tests: **7 passed**. Guards cover foreign/external resources, live-volume capture, isolated loopback configurations, image-volume copy suppression, private non-overwriting reports, archive integrity and safe application restore. Compilation and diff whitespace checks pass.
- The drill disables image-to-volume copying because SeaweedFS's image includes a pre-populated `filerldb2` directory. Empty-volume restore checks remain enforced. Source containers are released before restored containers are created to bound disk use with Docker's VFS driver.
- All drill containers and volumes were removed after success; existing development containers remained running. The private report is written to the operator-selected path; temporary keys, credentials, plaintext volume archives and encrypted drill backups are removed.
- This verifies same-version cold recovery with synthetic data. Online/PITR/off-site backups, production recovery objectives and sizing, independently hosted source coordination, CDC failover and OIDC browser-session recovery remain unverified. No environment configuration changes or publication were performed.
