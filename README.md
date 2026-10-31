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
- **Connections:** encrypted PostgreSQL credentials; allowlisted backend hosts/ports; read-only source tests; complete snapshots or append-only integer-watermark extraction. Watermarks and source schemas advance only with successful publication. Empty refreshes do not create duplicate versions.
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

## Limits and data semantics

- File content: 20 MB; multipart request: 21 MB; other write-request bodies: 1 MB.
- Datasets: 1–100,000 rows, 1–200 columns; column names are unique case-insensitively, non-empty, at most 500 characters, without control characters. Larger datasets are rejected rather than truncated.
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
# Real development stack + OIDC platform:
python scripts/verify_stack.py
CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/workflows-browser.cjs
# After the browser prepares the development automation token:
.dagster-venv/bin/python scripts/verify_dagster.py
```

Playwright and Chromium are browser-test prerequisites. Live-stack checks are separate from the unit/integration suite and use synthetic `platform_demo.sales` source data. The Dagster check creates a temporary scheduled definition and pauses it after verification. See the delivery matrix for the exact validation evidence and limitations.
