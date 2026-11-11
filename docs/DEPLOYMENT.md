# Deployment and operations

## Modes

`DATA_PLATFORM_AUTH_MODE=local` is the default loopback-only, unauthenticated development mode. `oidc` enables the gateway and isolated workspaces. No mode supports multiple platform workers against the same data directory.

The self-hosted launcher configures these non-secret OIDC values for the supplied Keycloak realm:

- `DATA_PLATFORM_OIDC_ISSUER`: the realm issuer.
- `DATA_PLATFORM_OIDC_CLIENT_ID`: `data-platform-web`.
- `DATA_PLATFORM_OIDC_REDIRECT_URI`: the browser callback URI.
- `DATA_PLATFORM_ALLOW_HTTP_OIDC=1`: explicitly permits loopback-only development HTTP.
- `DATA_PLATFORM_TRINO_URL`: the loopback Trino endpoint.

External deployments must use HTTPS, an exact issuer/callback, and `DATA_PLATFORM_ALLOWED_HOSTS` containing only expected browser hosts. Additional discovery/token/JWKS hosts must be explicitly listed in `DATA_PLATFORM_OIDC_ALLOWED_HOSTS`. An optional `DATA_PLATFORM_OIDC_CLIENT_SECRET` belongs in secure environment configuration, never source control. Keep forwarded host/scheme handling consistent with the registered redirect. Configure identity verification/registration policy in the provider before public use.

The gateway validates RS256 identity tokens, required claims, issuer, audience, authorized party, expiry and nonce. Login state is single-use and PKCE-bound. Sessions store hashes of opaque cookies, not raw provider tokens. Cross-origin mutations are rejected; bearer automation tokens are separately scoped and do not rely on browser cookies.

## Data and source boundaries

`DATA_PLATFORM_DIR` defaults to `var`. OIDC creates a separate security catalog and separate workspace subdirectories. Keep connector encryption keys with the matching metadata when restoring. Losing a connector key makes existing stored credentials unusable; recreating a source must be an explicit action.

`DATA_PLATFORM_CONNECTOR_HOSTS` and `DATA_PLATFORM_CONNECTOR_PORTS` are exact server allowlists, defaulting to local development PostgreSQL. Non-loopback PostgreSQL connections require `verify-full` TLS. Configure the source role as read-only and grant only required schemas/tables. The development source account is not a recommended production role. Connection strings are not accepted in database/user fields.

Live CDC is an explicit exception to read-only source access: it needs PostgreSQL 15+, logical WAL, a regular integer/UUID-keyed table, a replication-capable role with SELECT, database CREATE permission and table ownership for its dedicated publication. It creates/drops only its named publication/slot and never mutates source rows. The supplied PostgreSQL 17 service enables logical replication and bounds slot WAL retention. Track retained WAL, source storage and failures; stopping/reinitializing a live source pauses its pipelines. A paused schedule keeps its slot, so prolonged pauses may require resnapshotting after WAL loss. Source connection password rotation is available without resetting checkpoints.

Application metadata and source slots must be recovered consistently. Restoring an older application backup after the source slot has advanced fails closed; explicitly reinitialize from a reviewed fresh snapshot. Do not run concurrent restored copies against one slot. The application acknowledges only locally accepted checkpoints and retries acknowledgement after interruption; its logical-decoding reader never consumes an unaccepted batch. This is not coordinated point-in-time recovery for the source database.

Trino application queries can reach only registered published tables in the assigned workspace schema. Direct backend interfaces remain loopback-only trusted-operator services. Network policy, TLS, backend identity and object-store privileges must be reviewed together before any public exposure.

The supplied development stack does **not** establish an independent tenant boundary at Trino or the object store: the Trino endpoint has no configured client authentication or access-control policy, and the generated SeaweedFS identity has administrative access to the shared warehouse. The `X-Trino-User` header identifies an application workspace for queries but is not an authenticated identity. Docker-host users and processes with direct access to these backend endpoints or credentials are trusted and can bypass the application policy. Loopback port binding limits host-network exposure; it does not isolate other containers or users with Docker access. A multi-tenant deployment needs authenticated backend identities, independently enforced per-tenant Trino and storage policies, TLS and network isolation, and negative access tests against the actual deployment before treating those services as tenant boundaries.

## Startup and lifecycle

Use the README commands. `dev_stack.py up` preserves secrets/volumes and waits for OIDC discovery, REST catalog configuration, Trino readiness and the S3 endpoint's expected authentication response. Full signed S3 and source/lakehouse checks are performed by `verify_stack.py`.

Data-directory leases prevent second workers and offline restore against a running directory. Stop the platform gracefully before application backup. Dagster service tokens expire and must be rotated; restored backups intentionally invalidate them. The launcher disables embedded scheduling whenever `deploy/runtime/dagster.env` selects external orchestration. Do not run both schedulers for the same workspace definitions.

Generated development files are under `deploy/runtime/`, mode-restricted and ignored by Git. Do not run `docker compose config` or environment dumps into shared logs: expanded configuration can contain credentials. Access logs are disabled by the platform launcher so authorization-code query strings are not logged. Request failures have non-secret request IDs; the metrics API contains aggregate counts rather than SQL or credentials.

Workflow workers run per loaded workspace and use the same data-directory lease. They execute accepted queued jobs even when Dagster supplies scheduling; embedded workflow scheduling is disabled in Dagster mode. Reload `deploy/dagster_definitions.py` to expose the new `platform_flow` job and `flow_schedules` sensor. The service token must belong to each intended workspace. The adapter watches API work for up to 30 minutes; work can remain active afterward and should be inspected/cancelled through its API run ID. The separate verification script can test a live sensor with `DATA_PLATFORM_VERIFY_SENSOR=1` and a matching `DAGSTER_HOME` while the test application uses `DATA_PLATFORM_ORCHESTRATOR=dagster`.

On restart, queued/running workflow runs are marked interrupted. They do not auto-resume. Retries reuse completed steps and frozen definitions, including publication's checked version; they do not roll back accepted native or remote outputs. Plan backups only after stopping/draining active work. General distributed leases, cross-engine transactions and production recovery acceptance remain outside this implementation.

## Backup scope

The encrypted backup tool protects the selected application directory only. Independently protect PostgreSQL, the Iceberg catalog and S3-compatible storage volumes, with tested point-in-time consistency and credential recovery. An Iceberg snapshot is not an object-store backup. `compose down` retains volumes; never add `--volumes` unless intentional data destruction is authorized.

### Coordinated cold-recovery drill

Run `.venv/bin/python scripts/verify_recovery.py --report /tmp/recovery-report.json`
to test the pinned development stack's recovery path. Choose a new report filename
for each run. Docker Compose, the pinned images (or registry access), Python
development dependencies, and enough memory/disk for one additional service stack
are required. With Docker's VFS driver, reserve at least 20 GB of free Docker storage;
the drill releases source containers before creating restored containers to limit
peak use. It leaves the existing development services and volumes untouched.

The command creates private, disposable Docker projects with generated credentials
and separate loopback ports. It seeds a Keycloak identity, PostgreSQL source rows,
an encrypted connector, and two native/published dataset versions. After closing
the application and stopping every source service, it captures PostgreSQL, Iceberg
catalog, and object-storage volumes with numeric file ownership preserved. The
application backup, service configuration and volume archives are bundled into an
authenticated, encrypted backup. Restore uses new empty volumes and configuration
recovered from that backup; image digests remain pinned.

Acceptance checks include native data and both immutable native tables, original
source files, version IDs, connector credential decryption and source re-import,
the retained Keycloak identity ID, Trino aggregates for both published versions,
and their original Iceberg snapshot IDs. A successful run writes a private JSON
report without credentials and removes only its own containers, volumes, keys and
temporary backup files. It refuses live-volume capture, foreign volumes, external
resources and existing report files. Interrupted runs may leave projects beginning
with `recovery-`; identify the exact reported project before cleaning it up.

This is a synthetic, same-version **cold restore** acceptance check. It does not
provide online/PITR backups, remote/off-site retention, production sizing, CDC-slot
failover acceptance, OIDC browser-session recovery, or coordinated production
disaster recovery. Real recovery requires stopping/draining all writers (including
external source writers), retaining the matching encryption keys and backend
credentials separately, restoring compatible images, and rotating invalidated
application sessions/service tokens. An independently hosted source database needs
its own coordinated recovery plan; do not point restored CDC copies at live slots.

## Image and package maintenance

The optional Python runtime requires a locally built immutable `DATA_PLATFORM_PYTHON_IMAGE` ID; the development launcher resolves it from `data-platform-python:dev`. Build with `dev_stack.py notebooks` before starting the platform. The Dockerfile pins the base digest and all Python dependency versions. In managed environments, the launcher supplies `CODEX_PROXY_CERT` as a build secret so pip retains TLS verification. Offline execution containers explicitly clear Docker client proxy defaults and receive only the submitted code/data through stdin. They have no host mounts or credentials. See README for resource and data/output limits.

Application operators with Docker-daemon access are trusted. This first implementation uses the host Docker kernel boundary and is not a security-reviewed multi-tenant public code-execution service. Do not expose it to hostile tenants without stronger runtime isolation, quotas and a deployment security review.

The stack pins the exact tested image digests. Application dependencies and the isolated Dagster lockfile are committed; credentials and prepared data are not. Revalidate integrations and security updates before production deployment. The pinned development versions are evidence of reproducibility, not a statement that they are suitable for every production security policy.
