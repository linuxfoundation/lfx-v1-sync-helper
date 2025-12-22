# LFX v1 Sync Helper Helm Chart

This Helm chart deploys the LFX v1 Sync Helper service, which monitors NATS KV stores for v1 data and synchronizes it with the LFX v2 platform APIs, handling data transformation and conflict resolution.

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
    --set image.tag=latest \
    --set app.environment.AUTH0_TENANT.value=my_tenant
```

**Note**: When using the local chart, you must specify `--set image.tag=latest` because the committed chart does not have an appVersion, so a version must always be specified when not using the published chart. The AUTH0_TENANT environment variable and Auth0 secret are also required.

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

### Environment Variables

The following environment variables have defaults configured in the chart's `app.environment` section:

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

### Additional Configuration

For all available configuration options and their default values, please see the [values.yaml](values.yaml) file in this chart directory. You can override these values in your own `values.yaml` file or by using the `--set` flag when installing the chart.

