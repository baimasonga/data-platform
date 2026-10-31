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

Trino application queries can reach only registered published tables in the assigned workspace schema. Direct backend interfaces remain loopback-only trusted-operator services. Network policy, TLS, backend identity and object-store privileges must be reviewed together before any public exposure.

## Startup and lifecycle

Use the README commands. `dev_stack.py up` preserves secrets/volumes and waits for OIDC discovery, REST catalog configuration, Trino readiness and the S3 endpoint's expected authentication response. Full signed S3 and source/lakehouse checks are performed by `verify_stack.py`.

Data-directory leases prevent second workers and offline restore against a running directory. Stop the platform gracefully before application backup. Dagster service tokens expire and must be rotated; restored backups intentionally invalidate them. The launcher disables embedded scheduling whenever `deploy/runtime/dagster.env` selects external orchestration. Do not run both schedulers for the same workspace definitions.

Generated development files are under `deploy/runtime/`, mode-restricted and ignored by Git. Do not run `docker compose config` or environment dumps into shared logs: expanded configuration can contain credentials. Access logs are disabled by the platform launcher so authorization-code query strings are not logged. Request failures have non-secret request IDs; the metrics API contains aggregate counts rather than SQL or credentials.

## Backup scope

The encrypted backup tool protects the selected application directory only. Independently protect PostgreSQL, the Iceberg catalog and S3-compatible storage volumes, with tested point-in-time consistency and credential recovery. An Iceberg snapshot is not an object-store backup. `compose down` retains volumes; never add `--volumes` unless intentional data destruction is authorized.

## Image and package maintenance

The stack pins the exact tested image digests. Application dependencies and the isolated Dagster lockfile are committed; credentials and prepared data are not. Revalidate integrations and security updates before production deployment. The pinned development versions are evidence of reproducibility, not a statement that they are suitable for every production security policy.
