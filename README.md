# Data Platform

An independent, data-only web platform. The browser is the product interface; Python runs its backend service. This repository starts with a real local workbench, version 0.1.0. There are no payment, subscription or billing features.

## Run locally

Requires Python 3.11 or newer. Use one server worker.

```bash
python -m venv .venv
# Linux / macOS:
source .venv/bin/activate
# Windows PowerShell instead:
# .venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
python -m uvicorn app.main:create_app --factory --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000. API documentation is at http://127.0.0.1:8000/docs. No Node build or external frontend service is required.

1. Import `examples/sales.csv`.
2. Select **Inspect dataset** to see its schema, missing values, distinct counts and preview.
3. Select **Query this dataset**. The editor receives its actual SQL table name.
4. Run the query. Use SQL aggregation to calculate measures; use **Quick chart** to chart the result.
5. Save a query or export the displayed results.

Example SQL (replace the table identifier with the imported table name):

```sql
SELECT region, SUM(revenue) AS revenue
FROM ds_your_actual_table_identifier
GROUP BY region
ORDER BY revenue DESC;
```

## Working features

- CSV, JSON row arrays, first-worksheet XLSX and Parquet imports.
- Persisted analytical tables and metadata.
- Retained original import files with SHA-256 checksums and source downloads.
- Dataset catalogue and search, with dataset renaming and confirmed permanent deletion.
- Browser Imports page with persistent attempt history, status filters, error details, timestamps, and links to successful datasets.
- Column types, null counts, distinct counts and completeness.
- Read-only SQL with joins and aggregations across imported tables.
- Persistent query history with SQL search, status filters, errors, durations, result counts and explicit reruns.
- Saved queries with name/SQL editing, confirmed deletion, stale-edit protection, activity history and CSV result export.
- Basic bar charts for non-negative numeric result columns.
- Local host and browser-origin checks; SQL external access disabled.
- Dedicated query processes with a 10-second deadline, 256 MB engine memory limit and 1,000-row result cap.

## Boundaries of this release

This is a local single-user development foundation, not a production SaaS or a completed Fabric replacement. There is no authentication, tenant isolation, PostgreSQL source connector, scheduled pipeline engine, visual transformation editor, materialised SQL transformation, Iceberg storage, Trino cluster or multi-user governance yet. SQL queries do not create or publish new datasets. The only current quality measurement is per-column profiling; enforceable data contracts remain planned.

Use the loopback binding. Do not expose the service through a public reverse proxy. Browser-origin and host checks do not replace authentication. The server is intended for trusted local input; uploaded file parsing is not yet isolated in bounded workers. UploadFile parses multipart data before the application-level 20 MB check, so an ingress request-size limit is required before any network deployment.

Limits: 20 MB uploaded file content, 100,000 rows, 200 columns; flat JSON only. Large imports are rejected rather than truncated. CSV is UTF-8; records with missing or excess fields are rejected. Nullable integer columns retain their exact integer values. Query responses encode integers outside JavaScript’s safe integer range as strings so browsers and CSV exports retain their digits. Other column types are inferred; mixed numeric columns still require schema review. Excel uses cached formula values. Column types are inferred and should be reviewed. Uniqueness statistics count non-null distinct values. Charts show the first 20 returned rows, without implicit aggregation. CSV exports contain only the displayed query result (up to 1,000 rows); formula-looking string cells are prefixed with an apostrophe for spreadsheet safety.

## Storage and recovery

By default data is stored under `var/` in the working directory:

- `datasets.duckdb`: analytical tables.
- `catalog.sqlite3`: catalogue, source provenance, saved queries and activity.
- `sources/`: original imported files, named by their dataset UUID. Older installations may contain datasets without a retained source; these remain queryable.

Set `DATA_PLATFORM_DIR` to change this directory. Use one application process: a process-local lock serialises imports and queries to avoid DuckDB read/write conflicts. This intentionally sacrifices concurrency in the initial build.

Stop the application before copying the entire data directory for a consistent backup. Restore the entire directory together. Import publication has exception compensation but is not transactionally atomic across SQLite and DuckDB in the event of abrupt process death; catalog reconciliation is a required next milestone. No source data is included in the source archive beyond the synthetic example.

## Verify

```bash
python -m pytest -q
node --check app/static/app.js
```

The current implementation passes 51 backend tests. A Playwright browser smoke script is included at `tests/browser-smoke.cjs`; it needs Node, Playwright and an installed Chromium browser. The browser workflow has been verified with Chromium, including source downloads, SQL, charts, CSV exports, saved queries, mobile layout and the API reference. Start an installation on port 8000 and run:

```bash
npm install --no-save playwright
npx playwright install chromium
node tests/browser-smoke.cjs
# Or use an existing Chromium installation:
# CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/browser-smoke.cjs
```

Tests cover persisted data and queries, quality counts, CSV/JSON/Excel/Parquet ingestion, invalid input rejection, read-only SQL, blocked filesystem/network SQL access, result caps, missing datasets, host/origin checks and worker timeouts. Frontend JavaScript passes syntax checking, and the browser smoke workflow passes. `/docs` serves a local read-only API reference under the strict CSP; `/openapi.json` remains available to API tools. Neither requires CDN access.

## Structure

```text
app/main.py               API, source provenance, catalogue and query workers
app/ingestion.py          File parsing boundary and CSV validation
app/static/               Browser interface (no external CDN)
tests/test_platform.py    Backend integration and boundary tests
tests/browser-smoke.cjs   Browser workflow check
examples/sales.csv        Synthetic starter dataset
docs/ARCHITECTURE.md      Target architecture and implementation stages
```

See `docs/ARCHITECTURE.md` before adding distributed services. The project has no selected open-source license yet; choose one before public distribution. No remote GitHub repository was created by this package.

## First development increment

The uploaded starter kit is the implementation baseline. This increment adds strict CSV record validation, nullable integer preservation, safe browser serialization of large integers, retained source files/checksums, and a local API reference. CI runs backend regression tests and JavaScript syntax checks on Python 3.11 and 3.12.

The next increment is durable ingestion jobs and recovery: version metadata schemas, record job states, reconcile interrupted publication, and move parsing into resource-bounded workers. Then add versioned transformations and lineage before introducing the Iceberg/object-storage/Trino integration. See `docs/ARCHITECTURE.md` for acceptance criteria and the broader platform direction.


## Import history in the web workspace

Open **Imports** in the navigation to see the latest 100 upload attempts accepted by the application. The page refreshes every three seconds while visible and includes a manual refresh button and status filters. Successful attempts link to their dataset; failed attempts show the validation error. Correct the source file and upload it again to create a new attempt.

Import history and dataset publication are recorded in the same SQLite transaction. After a server restart, unfinished attempts are marked interrupted with a prompt to upload again. History starts with this feature; existing datasets are preserved without inventing historical attempts. Browser-side file-size rejection and requests rejected before the upload handler do not create attempt records.

Processing still runs during the upload request. This feature does not introduce a background queue, automatic retries, cancellation, or scheduled pipelines. Keep the documented single-worker server configuration. Crash reconciliation across source files, DuckDB, and SQLite remains a separate milestone.

## Manage datasets in the browser

Inspect a dataset and choose **Rename dataset** to change its display name. Its SQL table identifier, original filename, checksum and saved queries remain unchanged. Names must contain 1–200 characters after trimming and cannot include control characters.

Choose **Delete dataset**, then type its current display name to enable **Delete permanently**. Cancellation leaves the dataset intact. Confirmed deletion removes the analytical table, retained source file and catalogue metadata. Import history and activity remain available; import cards stop offering links to deleted datasets. Saved queries are retained and may need editing when they reference a removed table. There is no undo or recycle bin.

Deletion is recorded before cleanup begins. A failed cleanup appears as **Deletion pending** in the catalogue, where **Retry deletion** resumes it. The single-worker server also resumes pending deletions on startup. This recovery applies to dataset deletion; interrupted import publication reconciliation remains planned. Server-side deletion markers are recovery records, not a backup or a guarantee of forensic erasure from database files, filesystem storage, or backups.

## Manage saved queries

Open **Saved queries** to open, edit, or delete a query. **Edit query** lets you change its name and SQL without creating a duplicate. **Save changes** stores a draft without executing SQL; open it in the workbench and run it explicitly. Opening a saved query clears previous query results so exports and charts are not shown against unrelated SQL.

**Delete query** opens a confirmation dialog. Cancellation keeps the query; confirmation removes only that saved query. Dataset contents and the current SQL editor are unchanged. Updates and deletions are recorded in activity history and survive restarts.

If another tab changes the saved text while a dialog is open, the server rejects the stale update or deletion. Close the dialog, select **Refresh saved queries**, and open the latest version before trying again. Names allow 1–120 characters without control characters, and SQL drafts allow 1–20,000 characters; whitespace-only values are rejected. Drafts need not be executable SQL.

## Query history

Open **Query history** to review the latest 100 workbench/API SQL runs. Search the SQL text, filter by status, and inspect errors, total duration, returned-row count and whether results were capped. The page refreshes every three seconds while visible. **Open in SQL editor** loads the original SQL and clears previous displayed results; it does not run anything. Choose **Run query** to execute it against the current data.

History records running, succeeded, failed, timed-out and interrupted attempts. Total duration includes waiting for the shared operation lock and starting the query worker; the result panel's execution time measures engine execution. A running request may be waiting for another operation. On restart, unfinished records become interrupted. Internal dataset previews and requests rejected before the SQL handler are excluded.

History begins with this feature. It stores SQL and outcome metadata, not result rows or dataset snapshots. Reruns can produce different results or fail if a dataset was removed. The page shows the latest 100 entries; older entries remain in the database. History is shared within the current single-user workspace.
