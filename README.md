# Data Platform

A browser-based data workspace with a FastAPI backend. Import files or PostgreSQL data, publish immutable dataset versions, query with DuckDB or workspace-scoped Trino, build SQL transformations and pipelines, enforce quality contracts, and save SQL/Python notebooks, metrics and dashboards. No billing or payment functionality is included.

This is a self-hosted **development platform**, not a claim of production readiness or superiority over Microsoft Fabric. The verified capabilities and remaining roadmap items are listed in [the delivery matrix](docs/DELIVERY.md).

## Start the self-hosted development stack

The self-hosted launcher targets Linux, macOS or WSL2 and requires Python 3.11+, Docker Compose, and approximately 8 GB available memory. One platform application worker is supported.

```bash
python -m venv .venv
# Run these self-hosted commands in a POSIX shell; use WSL2 on Windows.
source .venv/bin/activate
python -m pip install -e '.[dev]'
python scripts/dev_stack.py up
python scripts/dev_stack.py developer
python scripts/dev_stack.py notebooks
python scripts/dev_stack.py platform
```

The platform listens on loopback port 8000. Sign in through Keycloak. The optional `developer` command creates a development account and saves its generated credentials in `deploy/runtime/developer.env`; it does not print passwords or overwrite existing accounts. Keycloak also supports local account registration in this development realm.

`up` generates private local configuration in the Git-ignored `deploy/runtime/` directory, starts digest-pinned services, and waits for protocol readiness. Preserve that directory and Docker volumes between restarts. Repeating initialization preserves existing secrets.

| Service | Loopback port | Purpose |
|---|---:|---|
| Platform | 8000 | Browser workspace and APIs |
| Keycloak | 8081 | OIDC authorization-code login with PKCE |
| PostgreSQL | 5433 | Development identity database and sample source |
| SeaweedFS S3 | 8333 | S3-compatible Iceberg storage |
| Iceberg REST catalog | 8181 | Persistent Iceberg catalog |
| Trino | 8082 | Iceberg publication and workspace-scoped read-only SQL |
| Dagster, when started | 3000 | Orchestration UI and daemon |

All service ports bind to loopback. The development identity realm uses HTTP and enables registration; do not expose these defaults publicly. Existing single-user data remains in its original data directory; it is not automatically shared with newly authenticated users.

### Optional Dagster orchestration

SQL and PostgreSQL-source pipelines can run manually or on intervals of at least 60 seconds. The embedded scheduler is suitable for one application worker. To use Dagster instead:

1. Sign in as a workspace owner, open **Manage members**, and create an automation token. Tokens are workspace-scoped, revocable, expire, and have at most editor access.
2. Save `DATA_PLATFORM_SERVICE_TOKEN=<your token>` in `deploy/runtime/dagster.env` with private file permissions. Never commit it or paste it into chat.
3. Prepare the separate Dagster environment, restart the platform, then start the daemon/UI:

```bash
python -m venv .dagster-venv
.dagster-venv/bin/python -m pip install -r deploy/dagster-requirements.lock
# Restart the platform command after creating dagster.env:
.venv/bin/python scripts/dev_stack.py platform
# In a separate process:
.venv/bin/python scripts/dev_stack.py dagster
```

The launcher selects external orchestration when `dagster.env` exists, disabling the embedded scheduler to avoid duplicate scheduling. The Dagster sensor reads current enabled definitions every 30 seconds. Run requests use stable keys, and retries carry an idempotency key into the platform. Rotate the token before its displayed expiry. To switch back to embedded scheduling, remove the Dagster configuration file, stop Dagster, and restart the platform.

```bash
python scripts/dev_stack.py status
python scripts/dev_stack.py down
```

`down` preserves named volumes. It does not delete application data or stop a separately launched Python platform/Dagster process.

## Single-user development mode

```bash
python -m uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8000 --no-access-log
```

This default mode has no authentication and must remain loopback-only. `DATA_PLATFORM_DIR` changes the data directory. OIDC deployment settings are documented in [deployment guidance](docs/DEPLOYMENT.md).

## Web workflows

- **Datasets:** CSV, flat JSON row arrays, first-sheet XLSX and Parquet imports; catalogue search; schema/profile inspection; renaming; full CSV export; original-file downloads; confirmed deletion.
- **Versions & lineage:** replacement uploads retain immutable prior tables and source files while keeping a stable SQL table name. Restore creates another version. Transformations record their SQL and exact input version IDs.
- **Quality contracts:** non-null, unique/non-null, minimum and maximum numeric rules. Save a contract, check existing data, and gate subsequent versions. Failed validation leaves the published version unchanged.
- **Data workflows:** materialise a complete SELECT result; save SQL or PostgreSQL-source pipelines; run, retry, pause, edit or delete definitions; inspect durable run history.
- **Multi-step workflows:** connect saved pipelines, inline quality checks and version-specific Iceberg publication in a dependency graph. Queue background runs, inspect per-step progress, pause schedules, cancel at step boundaries, resume unsuccessful steps and create bounded daily SQL backfills.
- **Connections:** encrypted PostgreSQL credentials and password rotation; allowlisted backend hosts/ports; source tests; complete snapshots, append-only integer watermarks, or logical change capture for inserts, updates, deletes and truncation. Source checkpoints advance only after accepted publication. Idle refreshes do not create duplicate versions. Inspect last success, failures, checkpoint/retained-WAL health and durable sync history.
- **Lakehouse:** publish immutable dataset versions to Iceberg, inspect snapshots, query published copies in Trino, and explicitly remove remote copies. Local deletion is blocked while lakehouse copies, source/pipeline ownership, or dependent version lineage would be broken.
- **SQL workbench:** explicit DuckDB/Trino selection, read-only SELECTs, saved-query editing/deletion, current-result export and quick bar charts. History records the engine, errors, durations, row count and truncation.
- **Notebooks:** saved SQL, Python and note cells with revision checks. SQL cells run independently. Python cells execute in disposable offline containers with pandas and NumPy; run one cell in a fresh session or all Python cells together to share variables. Select workspace dataset inputs explicitly, inspect printed/table output and retained run history, and see the exact input version IDs. Notes and outputs render as text. There is no Jupyter server or persistent kernel.
- **Metric definitions:** named scalar SQL, a value column, a unit and a description. Metrics must return exactly one row; no hidden aggregation is applied.
- **Dashboards:** up to eight saved table/metric widgets; explicit sequential refresh; timestamped results; stale-data fallback; no automatic SQL execution on open. Preview tables show up to 50 rows and 12 columns. Saved links restore definitions, not result caches.
- **Workspace access:** isolated data directories, owner/editor/viewer roles, membership management, session logout, and scoped automation-token revocation.
- **Operations:** import retry-file cleanup, request IDs, aggregate request/catalog metrics, publication recovery, reconciliation findings, and encrypted offline backup/restore.

Saved code and notes can be drafts. Saving or opening a definition never executes it. Run all Python cells skips SQL and note cells and stops on the first Python error. Python variables last only for that run; individual cell runs do not reuse prior variables. SQL transformations use workspace dataset tables; published Iceberg copies are queried through the Trino workbench option.

### Python notebook runtime

`python scripts/dev_stack.py notebooks` builds the pinned Python 3.12/pandas/NumPy image. The self-hosted `platform` launcher resolves its immutable local image ID. For local mode, set `DATA_PLATFORM_PYTHON_IMAGE` to that image ID before launching Uvicorn. Without it, SQL/notes and Python draft editing still work; Python execution reports its unavailable status explicitly. Docker must run locally at `/var/run/docker.sock`; the launcher supports Linux, macOS Docker Desktop and WSL2.

In a Python cell, use `pd`, `np`, `print()` and `display()`. Selecting a dataset makes its complete current version available as `datasets["<dataset-id>"]`, a pandas DataFrame. The UI shows the reference after saving the selection. Input versions are snapshotted together under the workspace lock. Common JSON scalar types are passed without pandas type inference; nulls remain `None`, and large integers, decimals, dates and other non-JSON scalar types arrive as strings. Convert a column explicitly when your analysis needs another dtype.

Each run has a non-root user, no network, no host mounts, no platform credentials, read-only root filesystem, no extra Linux capabilities, and bounded scratch space. The controller can access Docker but no Docker socket enters the execution container. Treat this as development isolation on a trusted Docker host, not a production sandbox for mutually hostile public tenants; containers share the host kernel. No host-process fallback exists.

- One Python run at a time per application process; additional runs receive a retryable busy response.
- 30-second execution deadline, 35-second container watchdog, 512 MB memory, one CPU, 20 CPU-seconds, 64 processes and 64 MB scratch space. Container provisioning can add up to 30 seconds and cleanup up to 10 seconds to request duration.
- At most five input datasets, 100,000 input rows total, and a 16 MB request to the runner. Oversized inputs fail rather than being truncated.
- 20,000 printed characters and eight displays per cell; each table display returns up to 1,000 rows/50 columns with a truncation flag. The browser previews 50 rows/12 columns.
- 1 MB displayed-output budget per run and 2 MB transport cap; output floods and timeouts terminate and remove the container. There is no network/package installation during a run.
- Run status, submitted Python code, revision, input versions, duration and bounded output are retained in workspace metadata. History lists the latest 100 runs; saved outputs load only when requested. Deleting a notebook deletes its Python run history and is blocked while execution/cleanup is active.
- On restart, the application removes exact container names recorded by interrupted runs and marks those runs interrupted. If Docker cleanup cannot be confirmed, subsequent runs in that workspace remain blocked until recovery succeeds. A stopped application does not preserve Python variables.

## Permissions

| Action | Viewer | Editor | Owner |
|---|---|---|---|
| Read datasets, versions, history and saved definitions | Yes | Yes | Yes |
| Run read-only SQL and saved analysis items | Yes | Yes | Yes |
| Download authorized sources/results | Yes | Yes | Yes |
| Import, transform, edit definitions and manage data | No | Yes | Yes |
| Configure sources, pipelines and publish/remove Iceberg copies | No | Yes | Yes |
| Manage workspace members and automation tokens | No | No | Yes |

Each authenticated workspace has separate DuckDB/SQLite files. Trino workbench queries are parsed and restricted to that workspace's registered published tables; system/other schemas, arbitrary table functions, multiple statements and writes are rejected. Service tokens are bound to one workspace and checked against current membership. Development backend service endpoints remain trusted-operator interfaces, not public tenant APIs.

## Build a multi-step workflow

First save reusable SQL or source pipelines in **Data workflows**. In **Multi-step workflows**, choose **New workflow**, add steps, choose their saved pipelines, and select dependencies. A quality step names an upstream dataset-producing step and explicit column rules. A publication step should use the checked quality step as its input, so it cannot publish unless that check succeeds. Dependency cycles and missing references are rejected.

For new upstream outputs, saved SQL can use table placeholders:

```sql
SELECT id, amount * 2 AS amount
FROM {{steps.step_1}}
WHERE business_day = CAST({{partition_date}} AS DATE)
```

Use placeholders without surrounding quotes, and include `step_1` as a dependency. The date becomes a server-generated SQL date value, not executable user text. Existing registered tables are pinned to their retained immutable versions when work is queued. Where a dependency publishes a newer version of the same stable table, descendants use that dependency's accepted version. A run records its workflow revision, pipeline definitions, input pins and each accepted output version. Quality rules defined in the workflow are frozen with that run; native dataset publication contracts remain enforced separately.

**Run workflow** queues work and returns immediately. One workflow worker executes steps sequentially in topological order per workspace. Failed steps block their dependent branches; independent branches can finish. Progress and per-step errors are durable. SQL/source extraction uses the existing worker limits, and lakehouse publication uses the existing Trino adapter. There is no multi-step transaction: accepted outputs remain if a later step fails or is cancelled.

- **Retry failed steps** resumes the same run, preserving completed outputs and stable child idempotency keys. It uses the saved SQL, targets, input versions and inline checks, not later definition edits. Start a new run to use revised SQL/rules. New runs freeze new definitions.
- A failed workflow cannot replace a live target changed since it was queued, or supersede a newer successful run of that saved pipeline. Start a new workflow run instead. Historical backfills do not use live targets.
- **Pause workflow schedule** stops future scheduling even while work is active. It does not cancel accepted queued/running work. **Cancel workflow run** cancels queued work immediately or requests a stop between steps; an engine call/publication already in progress finishes first. Accepted outputs are not rolled back.
- On shutdown/restart, unfinished work becomes `interrupted`; it resumes only through an explicit retry. If an active operation cannot drain on shutdown, the data-directory lease remains held until process exit. This remains a single application worker design.
- Schedules use UTC, at least 60 seconds, and the configured embedded scheduler or Dagster. Dagster now has `platform_flow` / `flow_schedules` in addition to the original pipeline job/sensor. Its job queues an API run, waits for durable completion and reuses its run key after transport retries. Cancelling a Dagster watcher does not cancel server-side work; use the platform's run cancellation control.
- Definitions cannot be edited/deleted while work is queued/running, except for pausing their schedule. Workflow history protects referenced pipelines, input datasets and output datasets from deletion. Delete a workflow and its history to release those references; published datasets remain and other ownership/lineage guards still apply.

### Daily historical backfills

Choose an inclusive start/end date and **Run daily backfill** (maximum 31 days per request). Each date gets a separate durable run and separate SQL output datasets. Existing live dataset targets and saved pipeline target pointers remain unchanged. All dates use the input versions captured for that batch; apply `{{partition_date}}` in SQL to select records for each date. Retrying an unsuccessful date reuses that run's accepted steps rather than creating duplicates.

Backfills are SQL-only. Source connectors, including CDC, cannot reconstruct past source state, so workflows containing a source step are rejected before any backfill is queued. A date filter over retained input records is not a reconstruction of an external database at that date. Empty daily outputs retain a supported flat scalar column schema; numeric min/max checks still fail when there are no values to evaluate. Native contracts on a live target are not copied to newly created backfill datasets; include explicit quality steps in the workflow. A workspace allows at most 64 queued/running workflow runs, 20 steps per definition and 30 rules per quality step. Existing dataset/SQL limits still apply. The system does not provide distributed execution, hourly/custom partitions or automatic missed-schedule catch-up.

Ordinary saved SQL pipelines retain their existing requirement for at least one result row. Empty SQL outputs are supported for daily backfills; a regular workflow using a saved SQL pipeline still reports an empty result as a step failure.

The APIs are `/api/flows`, `/api/flows/{id}/run`, `/backfill`, `/pause`, and `/runs/{run_id}` with `/retry` or `/cancel`. Running/backfilling requires the saved revision and returns HTTP 202 plus run IDs. An `Idempotency-Key` is tied to the exact request; reusing it with changed dates/revision is rejected. Status reads never execute work. Editors/owners can write workflows; viewers may read definitions/history only.

## Keep PostgreSQL data current

In **Connections**, choose **Live changes**, save the encrypted connection, and click **Sync live changes now** for the initial snapshot. **Enable automatic sync** creates or enables a source pipeline at a 60-second interval. Processing and scheduler delays add to that interval; this is scheduled CDC, not a sub-second streaming service. The embedded scheduler runs in the application process. In Dagster mode, a running daemon and service token for that particular workspace are required; the UI identifies this prerequisite. Merely saving a schedule cannot prove an external daemon is running.

Live mode uses PostgreSQL `pgoutput` logical decoding and a dedicated publication/replication slot. An exported snapshot establishes the initial data and WAL position consistently. Subsequent complete transactions update the dataset by primary key, including key changes and unchanged TOAST values. The primary key may contain one or more integer/UUID columns. The first implementation supports PostgreSQL 15+ regular logged tables with ordinary flat scalar columns; the self-hosted stack tests PostgreSQL 17. Partitioning, inheritance, row-level security, generated/unlogged tables, row-filtered/column-list publications and unsupported types are rejected. Numeric columns need explicit precision up to 38 and a non-negative scale.

The source role needs logical-replication permission, SELECT access, CREATE permission on the database and ownership of the source table to create its publication. Snapshot/append modes continue to use read-only transactions. Live mode manages its dedicated replication objects; it does not install triggers or write to source rows. `dev_stack.py up` enables logical WAL in the supplied development PostgreSQL server and preserves its volume. External server configuration and privileges remain the operator's responsibility.

Accepted data, lineage and the local WAL checkpoint settle through the existing publication journal. Only then is source WAL acknowledged. A quality failure leaves the old dataset/checkpoint published; a later retry replays retained changes. An acknowledgement failure appears as `ack_pending`, and the next sync resumes it without creating a duplicate version. Idle batches can acknowledge fully decoded WAL without creating a dataset version. Transactions still in progress remain available after they commit.

- **Sync health** shows last attempt/success, last error, source commit time when known, schedules, source WAL lag and retained WAL bytes. **Sync history** retains attempts and row-change counts, including failures/interrupted runs.
- **Pause automatic sync** pauses source pipelines and retains WAL. **Stop live sync** pauses its pipelines and releases its dedicated replication slot/publication; published data remains. Resuming after a stop requires **Reinitialize live sync**, which takes a fresh snapshot and retains old versions. Reinitialization leaves schedules paused until enabled again.
- Schema drift, publication changes, missing/lost slots, replaced source identity or recovery timeline, and a source checkpoint ahead of local data fail explicitly. They never silently skip changes or auto-resnapshot. Review the source, then reinitialize deliberately. A quality failure may require revising the contract before reinitialization can publish new data.
- The development server sets `max_slot_wal_keep_size=256MB`, checked by PostgreSQL at checkpoints; retained WAL can exceed that between checkpoints. Paused/failed consumers can lose retained WAL and then require a fresh snapshot. Monitor source disk space and retention in external deployments.
- Deleting a live connection cleans its dedicated source resources first and is blocked if cleanup cannot be confirmed. Unstarted or already-stopped connections can be deleted without source access. Use **Update connection password** to recover after credential rotation without discarding checkpoints.
- While a CDC connection owns a dataset, manual replacements, restores and transformations into that dataset are rejected. Create a derived dataset or remove the connection before editing its output. This prevents local edits from diverging from the recorded source checkpoint.
- Dashboard refresh remains explicit. A refreshed dashboard queries the current stable dataset view after a successful sync; opening a dashboard still does not execute SQL automatically.

Each sync is bounded by the existing 30-second worker deadline, 100,000 destination rows, 200 columns and 128 MB prepared-file limit. Decoding batches target 5,000 protocol messages, keep transaction boundaries, and reject batches above 100,000 messages or 16 MB rather than acknowledging partial data. Large transactions or workloads exceeding these limits require a reviewed resnapshot or a larger ingestion architecture. Empty live datasets are supported, including deletion of all source rows; numeric min/max quality rules still fail when no values can be evaluated.

## Limits and data semantics

- File content: 20 MB; multipart request: 21 MB; other write-request bodies: 1 MB.
- File imports: 1–100,000 rows, 1–200 columns. Live CDC and daily backfill datasets may have zero rows. Column names are unique case-insensitively, non-empty, at most 500 characters, without control characters. Larger datasets are rejected rather than truncated.
- DuckDB queries: 10-second worker deadline, 256 MB engine limit, 1,000 returned rows and 8 MB response limit. Trino workbench queries request the same execution/memory budget and row/response caps; network overhead can add to total request duration.
- Parsing/materialisation/source extraction: child processes with a 30-second wall-clock deadline, engine limits and additional OS CPU/address-space/file-size limits where supported. A process boundary is not a general hostile-code sandbox.
- Full CSV export: all stored rows, with a 128 MB download limit. Query export contains only displayed result rows. CSV includes UTF-8 BOM, headers and quoted fields; formula-looking string cells/headers receive an apostrophe. Numeric negative values remain numeric. Nulls, empty strings and non-finite floats export as empty cells.
- Large integer digits are preserved; JSON integers outside JavaScript's safe range are represented as strings. Spreadsheet applications can infer their own numeric types when opening CSV.
- CSV is UTF-8; malformed record widths are rejected. Excel uses cached formula values. Types are inferred for file imports and should be reviewed. Original sources and SHA-256 hashes are retained per version.
- PostgreSQL incremental mode requires a unique, non-null integer watermark. Updates/deletes and late rows at or below that watermark are not captured. This is not CDC. Source schema drift blocks incremental publication until reviewed.
- Iceberg publication supports common flat scalar types. Unsupported types, oversized cells, non-finite numbers and out-of-range decimal values fail explicitly. Native DuckDB tables remain the primary workspace store.
- History pages show the latest 100 entries. Older history is retained in metadata. Retry files can be removed separately without deleting published versions or history.

## Storage and recovery

Under the configured data directory, a workspace retains `datasets.duckdb`, `catalog.sqlite3`, immutable `versions/`, staged retry files, and its private connector encryption key. OIDC mode additionally keeps a security catalog and separate `workspaces/<id>/` directories.

Publication writes a durable journal before creating version tables. Incomplete writes are discarded on recovery; ready publications are completed idempotently. Stable SQL views and catalogue pointers are settled before subsequent API operations. Unknown orphan/missing-table findings are reported, not silently deleted. Deletion has its own retryable recovery marker.

Stop the platform before backup. A directory lease prevents another worker or the backup tool from using an active data directory.

```bash
python scripts/backup.py init-key --key-file /secure/location/platform-backup.key
python scripts/backup.py backup --data-dir var --archive /secure/location/platform.dpb --key-file /secure/location/platform-backup.key
python scripts/backup.py restore --data-dir restored-var --archive /secure/location/platform.dpb --key-file /secure/location/platform-backup.key
```

Backups are streamed through authenticated AES-GCM encryption, with a checksummed archive manifest. Restore requires an empty destination, rejects unsafe archive paths/corrupt data, preserves lakehouse namespaces, and invalidates restored sessions and automation tokens. Keep the backup key separately. These commands back up the application data directory, not Docker volumes; production recovery must also protect PostgreSQL, catalog and object-storage volumes together.

## Verify

```bash
python -m pytest -q
node --check app/static/app.js
node --check tests/browser-smoke.cjs
# Local-mode instance at DATA_PLATFORM_URL, defaults to loopback port 8000:
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/browser-smoke.cjs
# Isolated local-mode analysis instance, defaults to port 8003:
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/analysis-browser.cjs
# Same instance with Python runtime enabled:
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/python-notebook-browser.cjs
# Run real Docker checks serially, separately from the Python browser check:
DATA_PLATFORM_TEST_PYTHON_IMAGE="$(docker image inspect data-platform-python:dev --format '{{.Id}}')" python -m pytest -q tests/test_python_notebooks.py
# Real PostgreSQL logical-replication checks (synthetic, isolated source schemas):
DATA_PLATFORM_TEST_CDC=1 python -m pytest -q tests/test_cdc.py
# Local instance with embedded scheduling on port 8004; waits for a real scheduled sync:
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/cdc-browser.cjs
# Isolated local workflow instance on port 8005:
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/flows-browser.cjs
.dagster-venv/bin/python scripts/verify_flows_dagster.py
# Existing S3/Iceberg/Trino development stack:
python scripts/verify_flow_lakehouse.py
# Real development stack + OIDC platform:
python scripts/verify_stack.py
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/workflows-browser.cjs
# After the browser prepares the development automation token:
.dagster-venv/bin/python scripts/verify_dagster.py
```

Playwright and Chromium are browser-test prerequisites. Live-stack checks are separate from the unit/integration suite and use synthetic `platform_demo.sales` source data. The Dagster check creates a temporary scheduled definition and pauses it after verification. See the delivery matrix for the exact validation evidence and limitations.
