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

## Startup and lifecycle

Use the README commands. `dev_stack.py up` preserves secrets/volumes and waits for OIDC discovery, REST catalog configuration, Trino readiness and the S3 endpoint's expected authentication response. Full signed S3 and source/lakehouse checks are performed by `verify_stack.py`.

Data-directory leases prevent second workers and offline restore against a running directory. Stop the platform gracefully before application backup. Dagster service tokens expire and must be rotated; restored backups intentionally invalidate them. The launcher disables embedded scheduling whenever `deploy/runtime/dagster.env` selects external orchestration. Do not run both schedulers for the same workspace definitions.

Generated development files are under `deploy/runtime/`, mode-restricted and ignored by Git. Do not run `docker compose config` or environment dumps into shared logs: expanded configuration can contain credentials. Access logs are disabled by the platform launcher so authorization-code query strings are not logged. Request failures have non-secret request IDs; the metrics API contains aggregate counts rather than SQL or credentials.

Workflow workers run per loaded workspace and use the same data-directory lease. They execute accepted queued jobs even when Dagster supplies scheduling; embedded workflow scheduling is disabled in Dagster mode. Reload `deploy/dagster_definitions.py` to expose the new `platform_flow` job and `flow_schedules` sensor. The service token must belong to each intended workspace. The adapter watches API work for up to 30 minutes; work can remain active afterward and should be inspected/cancelled through its API run ID. The separate verification script can test a live sensor with `DATA_PLATFORM_VERIFY_SENSOR=1` and a matching `DAGSTER_HOME` while the test application uses `DATA_PLATFORM_ORCHESTRATOR=dagster`.

On restart, queued/running workflow runs are marked interrupted. They do not auto-resume. Retries reuse completed steps and frozen definitions, including publication's checked version; they do not roll back accepted native or remote outputs. Plan backups only after stopping/draining active work. General distributed leases, cross-engine transactions and production recovery acceptance remain outside this implementation.

## Backup scope

The encrypted backup tool protects the selected application directory only. Independently protect PostgreSQL, the Iceberg catalog and S3-compatible storage volumes, with tested point-in-time consistency and credential recovery. An Iceberg snapshot is not an object-store backup. `compose down` retains volumes; never add `--volumes` unless intentional data destruction is authorized.

## Image and package maintenance

The optional Python runtime requires a locally built immutable `DATA_PLATFORM_PYTHON_IMAGE` ID; the development launcher resolves it from `data-platform-python:dev`. Build with `dev_stack.py notebooks` before starting the platform. The Dockerfile pins the base digest and all Python dependency versions. In managed environments, the launcher supplies `CODEX_PROXY_CERT` as a build secret so pip retains TLS verification. Offline execution containers explicitly clear Docker client proxy defaults and receive only the submitted code/data through stdin. They have no host mounts or credentials. See README for resource and data/output limits.

Application operators with Docker-daemon access are trusted. This first implementation uses the host Docker kernel boundary and is not a security-reviewed multi-tenant public code-execution service. Do not expose it to hostile tenants without stronger runtime isolation, quotas and a deployment security review.

The stack pins the exact tested image digests. Application dependencies and the isolated Dagster lockfile are committed; credentials and prepared data are not. Revalidate integrations and security updates before production deployment. The pinned development versions are evidence of reproducibility, not a statement that they are suitable for every production security policy.
