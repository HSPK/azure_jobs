# REST API Design

All Azure interactions use direct HTTP REST calls. No `azure-ai-ml` SDK or `amlt` dependency.

## Authentication

**Credential**: `AzureCliCredential` from `azure-identity` (lightweight, leverages `az login` context).

**Token caching**: Both REST clients cache tokens and refresh 60 seconds before expiry.

**Scopes**:

| Plane | Scope | Used for |
|-------|-------|----------|
| ARM management | `https://management.azure.com/.default` | All CRUD operations (jobs, environments, datastores, workspaces, compute) |
| Data plane | Derived from workspace `discoveryUrl` (e.g., `https://ml.azure.com/.default`) | Run History API (log downloads) |

China cloud is auto-detected: management URLs containing `.cn/` switch blob storage to `.blob.core.chinacloudapi.cn` and data plane scope to `https://ml.azure.cn/.default`.

## REST Clients

### AzureARMClient

Generic ARM client for cross-subscription operations.

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `get(url)` | Any | Authenticated GET → JSON |
| `post(url, body)` | Any | Authenticated POST → JSON |
| `list_subscriptions()` | `GET /subscriptions` | Discover enabled subscriptions |
| `resource_graph_query(query, subs)` | `POST /providers/Microsoft.ResourceGraph/resources` | KQL queries across subscriptions |
| `get_vc_quotas_raw(sub, rg, vc)` | `GET .../virtualclusters/{vc}` | Singularity VC properties and quotas |
| `list_workspace_computes(sub, rg, ws)` | `GET .../workspaces/{ws}/computes` | AML compute cluster details |
| `list_ml_workspaces(subs)` | Resource Graph KQL | Discover all ML workspaces |

### AzureMLJobsClient

Workspace-scoped client for ML operations. API version: `2024-04-01`.

#### Jobs

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `list_jobs(filters)` | `GET .../workspaces/{ws}/jobs` | List jobs with server-side filtering |
| `get_job(name)` | `GET .../jobs/{name}` | Get job details |
| `cancel_job(name)` | `POST .../jobs/{name}/cancel` | Cancel a running job |
| `create_or_update_job(name, body)` | `PUT .../jobs/{name}` | Submit a new job |

#### Environments

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `list_environments()` | `GET .../environments` | List environment containers |
| `list_environment_versions(name)` | `GET .../environments/{name}/versions` | List versions (ordered by creation time) |
| `get_environment_version(name, ver)` | `GET .../environments/{name}/versions/{ver}` | Get specific version |
| `create_or_update_environment(name, ver, image)` | `PUT .../environments/{name}/versions/{ver}` | Register Docker image as environment |

#### Datastores

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `list_datastores()` | `GET .../datastores` | List workspace datastores |
| `get_datastore(name)` | `GET .../datastores/{name}` | Get datastore details |
| `create_or_update_datastore(name, ...)` | `PUT .../datastores/{name}` | Create blob datastore (credential-less) |
| `list_datastore_secrets(name)` | `POST .../datastores/{name}/listSecrets` | Get storage account key or SAS token |

#### Code Upload

| Method | Details |
|--------|---------|
| `upload_code(code_dir, ignore_patterns)` | Zip → SHA256 hash → blob upload → register code version |
| `_get_default_storage()` | Extract account name and container from workspace ARM properties |
| `_get_blob_sas(account, container)` | Get SAS token via `listSecrets` on `workspaceblobstore` |
| `_upload_blob(account, container, path, data)` | PUT blob via Azure Storage REST API |

#### Run History (Data Plane)

| Method | Endpoint | Purpose |
|--------|----------|---------|
| `get_run_log_urls(job_name)` | `POST {data_plane}/history/v1.0/.../runs/{id}/artifacts/contentinfo` | Get signed blob URLs for log files |

## Job Submission Body

```json
{
  "properties": {
    "jobType": "Command",
    "displayName": "job-name",
    "experimentName": "experiment",
    "command": "bash -c '...'",
    "compute": "cluster-name",
    "codeId": "/subscriptions/.../codes/aj-code/versions/{hash}",
    "environmentId": "/subscriptions/.../environments/{name}/versions/{ver}",
    "environmentVariables": { "AJ_NODES": "4", ... },
    "distribution": { "distributionType": "PyTorch", "processCountPerInstance": 2 },
    "identity": { "identityType": "Managed" },
    "resources": {
      "instanceCount": 4,
      "shmSize": "2048g",
      "properties": { "AISuperComputer": { ... } }
    },
    "outputs": {
      "mount_name": {
        "jobOutputType": "uri_folder",
        "uri": "azureml://datastores/aj_mount_name/paths/job-name/",
        "mode": "ReadWriteMount"
      }
    },
    "tags": { ... },
    "properties": { "AZURE_ML_OUTPUT_PathOnCompute_mount_name": "/mnt/data/" }
  }
}
```

## Dependencies

```
click>=8.2.1           # CLI framework
pyyaml>=6.0.2          # YAML template parsing
rich>=14.3.3           # Rich terminal UI
textual>=1.0.0         # TUI framework (dashboard)
azure-identity>=1.17.0 # AzureCliCredential (lightweight)
requests>=2.31.0       # HTTP client
```

No `azure-ai-ml`, no `amlt`, no heavy transitive dependency tree.

## Performance

| Path | Target | How |
|------|--------|-----|
| `aj --help` | <200ms | No heavy imports at module level |
| `aj list`, `aj template list` | <200ms | Local file I/O only |
| `aj run` | ~2-5s | REST calls: env registration, code upload, job PUT |
| `aj job list/show/cancel` | ~1-2s | Single REST call |
| `aj job logs` | ~2-3s | Run History API + blob download |
