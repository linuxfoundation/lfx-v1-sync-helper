# LFX v1 Sync Helper Helm Chart

This Helm chart deploys the LFX v1 Sync Helper service, which monitors NATS KV stores for v1 data and synchronizes it with the LFX v2 platform APIs, handling data transformation and conflict resolution. The chart also includes an optional PostgreSQL WAL listener component for real-time database change streaming.

## Prerequisites

- Kubernetes 1.19+
- Helm 3.2.0+
- LFX v2 platform deployed (with NATS and required APIs)
- Access to LFX v1 data sources via Meltano pipeline

## Installing the chart

### Installing from local chart

For development or testing with local chart sources:

```bash
# Clone the repository
git clone https://github.com/linuxfoundation/lfx-v1-sync-helper.git
cd lfx-v1-sync-helper

# Create namespace (recommended)
kubectl create namespace lfx

# Create Auth0 secret with required credentials
kubectl create secret generic v1-sync-helper-auth0-credentials \
    --from-literal=client_id=your-auth0-client-id \
    --from-literal=client_private_key="$(cat auth0-private-key.pem)" \
    -n lfx

# Install the chart with required image tag and AUTH0_TENANT
helm install -n lfx lfx-v1-sync-helper \
    ./charts/lfx-v1-sync-helper \
    --set app.image.tag=latest \
    --set app.environment.AUTH0_TENANT.value=my_tenant
```

**Note**: When using the local chart, you must specify `--set app.image.tag=latest` because the committed chart does not have an appVersion, so a version must always be specified when not using the published chart. The AUTH0_TENANT environment variable and Auth0 secret are also required.

### Installing from OCI registry

For production deployments using the published chart:

```bash
# Create namespace (recommended)
kubectl create namespace lfx

# Create Auth0 secret with required credentials
kubectl create secret generic v1-sync-helper-auth0-credentials \
    --from-literal=client_id=your-auth0-client-id \
    --from-literal=client_private_key="$(cat auth0-private-key.pem)" \
    -n lfx

# Create PostgreSQL credentials secret (for wal-listener component)
kubectl create secret generic v1-platform-db-credentials \
    --from-literal=host=your-postgres-host \
    --from-literal=username=your-postgres-user \
    --from-literal=password=your-postgres-password \
    -n lfx

# Create values.yaml with required AUTH0_TENANT
cat > values.yaml << EOF
app:
  environment:
    AUTH0_TENANT:
      value: my_tenant
EOF

# Install from the OCI registry
helm install -n lfx lfx-v1-sync-helper \
    oci://ghcr.io/linuxfoundation/lfx-v1-sync-helper/chart/lfx-v1-sync-helper \
    -f values.yaml
```

## Uninstalling the chart

To uninstall/delete the `lfx-v1-sync-helper` deployment:

```bash
helm uninstall lfx-v1-sync-helper -n lfx
```

## Configuration

### Required Secrets

The chart requires the following secrets to be created before installation (if they don't already exist):

1. **Heimdall JWT signing key** (default name: `heimdall-signer-cert`):
   This secret should already exist from the LFX platform (lfx-v2-helm) umbrella chart deployment. If it doesn't exist, create it with:
   ```bash
   kubectl create secret generic heimdall-signer-cert \
       --from-file=signer.pem=/path/to/heimdall-private-key.pem \
       -n lfx
   ```

2. **Auth0 credentials** (default name: `v1-sync-helper-auth0-credentials`):
   ```bash
   kubectl create secret generic v1-sync-helper-auth0-credentials \
       --from-literal=client_id=your-auth0-client-id \
       --from-literal=client_private_key="$(cat auth0-private-key.pem)" \
       -n lfx
   ```

3. **PostgreSQL credentials** (default name: `v1-platform-db-credentials`):
   Required for the WAL listener component to connect to the PostgreSQL database.
   ```bash
   kubectl create secret generic v1-platform-db-credentials \
       --from-literal=host=your-postgres-host \
       --from-literal=username=your-postgres-user \
       --from-literal=password=your-postgres-password \
       -n lfx
   ```

### App Component

The following environment variables for the custom app component have defaults configured in the chart's `app.environment` section:

| Variable                | Default                                                                    | Description               |
|-------------------------|----------------------------------------------------------------------------|---------------------------|
| `NATS_URL`              | `nats://lfx-platform-nats.lfx.svc.cluster.local:4222`                      | NATS server URL           |
| `PROJECT_SERVICE_URL`   | `http://lfx-v2-project-service.lfx.svc.cluster.local:8080`                 | Project Service API URL   |
| `COMMITTEE_SERVICE_URL` | `http://lfx-v2-committee-service.lfx.svc.cluster.local:8080`               | Committee Service API URL |
| `HEIMDALL_JWKS_URL`     | `http://lfx-platform-heimdall.lfx.svc.cluster.local:4457/.well-known/jwks` | JWKS endpoint URL         |
| `LFX_API_GW`            | `https://api-gw.dev.platform.linuxfoundation.org/`                         | LFX API Gateway URL       |
| `DEBUG`                 | `false`                                                                    | Enable debug logging      |
| `PORT`                  | `8080`                                                                     | HTTP server port          |
| `BIND`                  | `*`                                                                        | Interface to bind on      |

For a complete list of all supported environment variables, including required ones like `AUTH0_TENANT`, see the [v1-sync-helper README](../../cmd/lfx-v1-sync-helper/README.md#environment-variables).

### v1-mappings Postgres store (LFXV2-2985)

The chart provisions the Postgres backing store used by the `MappingStore` port introduced in LFXV2-2985. Select the provisioning strategy via `database.mode`:

| Mode               | What the chart renders                                                        | When to use                                            |
|--------------------|-------------------------------------------------------------------------------|--------------------------------------------------------|
| `external`         | Nothing; the app reads `DATABASE_URL` (or the `PG*` quintuple) from a Secret. | You already have Postgres (RDS via ExternalSecrets, an umbrella-chart-managed CNPG cluster, etc.). |
| `database`         | A `postgresql.cnpg.io/v1` `Database` CR only.                                 | Umbrella deployments where the CNPG Cluster is provisioned by the umbrella chart. |
| `cluster+database` | Both a `postgresql.cnpg.io/v1` `Cluster` and `Database` CR.                   | Standalone deployments without an umbrella chart.      |

In `database` and `cluster+database` modes the CNPG operator auto-creates a `<clusterName>-app` Secret with `host` / `port` / `username` / `password` keys. The chart's app deployment forwards those as `PGHOST` / `PGPORT` / `PGUSER` / `PGPASSWORD` env vars (plus a static `PGDATABASE` from `database.cloudNativePG.databaseName`) and the service composes the libpq DSN in-process via `Config.ResolveDatabaseURL`. This is deliberate — passing the DSN as a literal env-var value would expose the password via `kubectl describe pod`.

In `external` mode with `database.external.shape=url` (the default) the deployment reads a single-key Secret as `DATABASE_URL`. In `external` mode with `shape=fields`, the deployment reads the `host` / `port` / `username` / `password` / `dbname` keys individually (compatible with the AWS RDS-via-ExternalSecrets pattern that surfaces each JSON field as its own Secret key).

**Runtime mode selection.** The app also honors `V1_MAPPINGS_STORE_MODE` (unset, `kv`, `dual`, or `postgres`) — set in `app.environment` to control the read/write routing independently from the chart-time provisioning. When `database.mode=external` with no `secretName` set, the chart emits `V1_MAPPINGS_STORE_MODE=kv` automatically so the app boots on clusters that haven't wired Postgres yet. Setting `app.environment.V1_MAPPINGS_STORE_MODE.value` overrides this safety fallback.

**Backfill.** Once the release is deployed and the CNPG cluster is ready, run `manifests/backfill-v1-mappings-to-postgres-job.yaml` to copy the KV bucket into `v1_mappings`. Then flip `V1_MAPPINGS_STORE_MODE` to `dual`; verify no drift for a soak period; flip to `postgres`.

### WAL Listener Component

The chart includes an optional PostgreSQL WAL (Write-Ahead Log) listener component that provides real-time streaming of database changes to NATS. This component is enabled by default and can be configured or disabled as needed.

#### WAL Listener Configuration

| Parameter                                 | Default                                        | Description                            |
|-------------------------------------------|------------------------------------------------|----------------------------------------|
| `walListener.enabled`                     | `true`                                         | Enable/disable WAL listener deployment |
| `walListener.replicas`                    | `1`                                            | Number of WAL listener replicas        |
| `walListener.image.repository`            | `ihippik/wal-listener`                         | WAL listener container image           |
| `walListener.image.tag`                   | `latest`                                       | WAL listener image tag                 |
| `walListener.config.listener.slotName`    | `lfx_v2`                                       | PostgreSQL replication slot name       |
| `walListener.config.database.secret.name` | `v1-platform-db-credentials`                   | Secret containing database credentials |
| `walListener.config.publisher.address`    | `lfx-platform-nats.lfx.svc.cluster.local:4222` | NATS server address                    |
| `walListener.config.publisher.topic`      | `wal_listener`                                 | NATS topic for publishing changes      |

The WAL listener monitors the following PostgreSQL tables by default (matching the meltano.yml tap-postgres configuration):
- `collaboration__c` (platform schema)
- `community__c` (platform schema)
- `project__c` (salesforce schema)
- `alternate_email__c` (salesforce schema)
- `merged_user` (salesforce schema)

To disable the WAL listener:
```yaml
walListener:
  enabled: false
```

To customize monitored tables:
```yaml
walListener:
  config:
    listener:
      filter:
        tables:
          your_table:
            - insert
            - update
            - delete
```

### Additional Configuration

For all available configuration options and their default values, please see the [values.yaml](values.yaml) file in this chart directory. You can override these values in your own `values.yaml` file or by using the `--set` flag when installing the chart.
