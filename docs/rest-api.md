# REST API

`aj` talks to Azure ML and ARM directly over HTTPS. No `azure-ai-ml` SDK is involved.

## Authentication

- Credential: `AzureCliCredential` from `azure-identity` — leverages the `az login` context, no extra MSAL setup.
- Token cache: refreshed 60 s before expiry, shared across the process.
- Scopes:

| Plane | Scope | Used for |
|-------|-------|----------|
| ARM management | `https://management.azure.com/.default` | jobs, environments, datastores, workspaces, compute, Resource Graph |
| Data plane | derived from workspace `discoveryUrl` (`https://ml.azure.com/.default`) | Run History (log download) |

China cloud is auto-detected from URLs containing `.cn` and switches both the data-plane scope (`ml.azure.cn`) and the blob host (`*.blob.core.chinacloudapi.cn`).

## Clients

`core/rest_client/` exposes two clients:

### `AzureARMClient` — cross-subscription ARM

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `get(url)` / `post(url, body)` | any | authenticated REST helpers |
| `list_subscriptions()` | `GET /subscriptions` | discover subscriptions |
| `resource_graph_query(query, subs)` | `POST /providers/Microsoft.ResourceGraph/resources` | KQL across subscriptions |
| `get_vc_quotas_raw(sub, rg, vc)` | `GET .../virtualclusters/{vc}` | Singularity VC quotas |
| `list_workspace_computes(sub, rg, ws)` | `GET .../workspaces/{ws}/computes` | AML cluster info |
| `list_ml_workspaces(subs)` | Resource Graph KQL | discover workspaces |

### `AzureMLClient` — workspace-scoped (api-version `2024-04-01`)

Composed of namespaces: `client.jobs`, `client.environments`, `client.datastores`, `client.blob`, `client.run_history`. Implementations live under `core/rest_client/api/`.

```python
client = AzureMLClient(subscription_id=..., resource_group=..., workspace_name=...)

# jobs
client.jobs.list(filters={...})            # GET .../jobs
client.jobs.get(name)                      # GET .../jobs/{name}
client.jobs.cancel(name)                   # POST .../jobs/{name}/cancel
client.jobs.create_or_update(name, body)   # PUT  .../jobs/{name}

# environments
client.environments.list()
client.environments.list_versions(name)
client.environments.create_or_update(name, version, image)  # PUT .../environments/{name}/versions/{ver}

# datastores
client.datastores.list()
client.datastores.create_or_update(name, ...)
client.datastores.list_secrets(name)       # POST .../datastores/{name}/listSecrets

# code upload
client.blob.upload_code(code_dir, ignore_patterns=..., extra_files=..., on_progress=...)
# 1. walk_code() + compute_code_hash()
# 2. PUT blob to azureml-blobstore-{ws}/LocalUpload/{hash}/<rel>
# 3. PUT .../codes/aj-code/versions/{hash}

# run history (data plane, for log download)
client.run_history.get_log_urls(job_name)
```

## Job submission body

```json
{
  "properties": {
    "jobType": "Command",
    "displayName": "job-name",
    "experimentName": "experiment",
    "command": "bash aj_runner.sh",
    "computeId": "/subscriptions/.../computes/cluster-name",
    "codeId":      "/subscriptions/.../codes/aj-code/versions/{hash}",
    "environmentId":"/subscriptions/.../environments/{name}/versions/{ver}",
    "environmentVariables": {
      "AJ_NAME": "...", "AJ_NODES": "4", "AJ_PROCESSES": "32", "...": "..."
    },
    "distribution": { "distributionType": "PyTorch", "processCountPerInstance": 8 },
    "identity":     { "identityType": "Managed" },
    "resources": {
      "instanceCount": 4,
      "shmSize": "2048g",
      "properties": { "AISuperComputer": { "...": "..." } }
    },
    "outputs": {
      "data": {
        "jobOutputType": "uri_folder",
        "uri": "azureml://datastores/aj_data/paths/job-name/",
        "mode": "ReadWriteMount"
      }
    },
    "tags": {},
    "properties": { "AZURE_ML_OUTPUT_PathOnCompute_data": "/mnt/data/" }
  }
}
```

## Dependencies

```
click>=8.2.1            # CLI framework
pyyaml>=6.0.2           # YAML parsing
rich>=14.3.3            # terminal UI
textual>=1.0.0          # TUI dashboard
azure-identity>=1.17.0  # AzureCliCredential
requests>=2.31.0        # HTTP client
```

No `azure-ai-ml`, no `amlt` runtime.

## Performance

| Path | Target | How |
|------|--------|-----|
| `aj --help` | < 200 ms | lazy imports, no heavy modules at top level |
| `aj list`, `aj template list` | < 200 ms | local file I/O only |
| `aj run` | 2–5 s | env register + code upload (skipped on cache hit) + job PUT |
| `aj job list/show/cancel` | 1–2 s | single REST call |
| `aj job logs` | 2–3 s | Run History + blob download |
