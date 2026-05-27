# REST API

`aj` calls Azure ML and ARM directly over HTTPS — no `azure-ai-ml` SDK.

## Auth

`AzureCliCredential` from `azure-identity`. Tokens cached per scope, refreshed
60 s early.

| Plane | Scope | Used for |
|-------|-------|----------|
| ARM | `https://management.azure.com/.default` | jobs, environments, datastores, computes, Resource Graph |
| Data | `https://ml.azure.com/.default` (China: `*.cn`) | Run History (logs, error details) |
| Storage | `https://storage.azure.com/.default` | Blob upload AAD fallback |

China cloud auto-detected from `*.cn` URLs (data-plane scope + blob host
switch).

## Clients

`az_client/` exposes two namespaced clients on top of a shared
`AuthSession` (retry, token cache, `raise_for_rest_error`).

### `AzureARMClient` — subscription-wide

| Namespace · method | Endpoint |
|--------------------|----------|
| `subscriptions.list()` | `GET /subscriptions` |
| `graph.query(query, subs)` | `POST /providers/Microsoft.ResourceGraph/resources` |
| `vc.list(sub_ids=None, *, with_raw=False)` | Resource Graph (`microsoft.machinelearningservices/virtualclusters`) |
| `vc.get(sub, rg, name)` | `GET .../virtualClusters/{name}` |
| `vc.quota.list()` / `vc.quota.get_by_name(name)` | Reads `properties.managedQuotas` |
| `workspace.list()` / `workspace.get(name)` | Resource Graph |
| `compute.list(sub, rg, ws)` / `compute.get(...)` | `GET .../workspaces/{ws}/computes` |
| `compute.list_all(workspaces)` | Parallel fan-out |
| `identity.list(sub, rg=None)` | User-assigned managed identities |
| `storage.list(sub)` | Storage accounts |
| `instance_types.list(sub, location)` | Singularity instance types |

`vc.list(..., with_raw=True)` additionally returns the raw GraphQL row for
quota parsing. `parse_managed_quotas(raw)` is a pure function and lives in
`arm/vc.py`.

### `AzureMLClient` — workspace-scoped (api `2024-04-01`)

Constructed with `WorkspaceCoords` (sub / rg / ws). Resolves `discoveryUrl`
lazily on first data-plane call.

```python
client.jobs.list(filters)                          # GET .../jobs
client.jobs.get(name)
client.jobs.cancel(name)
client.jobs.create_or_update(name, body)           # PUT .../jobs/{name}
client.jobs.get_run_log_urls(name)                 # → logs.get_urls

client.environments.list_versions(name)
client.environments.create_or_update(name, ver, image)

client.datastores.list()
client.datastores.create_or_update(name, body)
client.datastores.list_secrets(name)               # SAS / account key

client.blob.upload_code(code_dir, ignore_patterns, extra_files, on_progress)
# walk_code → compute_code_hash → PUT blob → register code version

client.logs.get_urls(job_name)                     # data plane (run-history)
client.logs.tail(url, *, bytes_back=65536)
client.logs.poll(url, *, offset)
```

`extract.py` holds the pure REST→`JobInfo` parsers; `context.py` owns
`RestContext` (auth + coords + dual scopes).

## Job body

```json
{
  "properties": {
    "jobType": "Command",
    "displayName": "...",
    "experimentName": "...",
    "command": "bash aj_runner.sh",
    "computeId":     "/subscriptions/.../computes/<name>",
    "codeId":        "/subscriptions/.../codes/aj-code/versions/{hash}",
    "environmentId": "/subscriptions/.../environments/{name}/versions/{ver}",
    "environmentVariables": { "AJ_NODES": "4", "AJ_PROCESSES": "32", "...": "..." },
    "distribution": { "distributionType": "PyTorch", "processCountPerInstance": 8 },
    "identity":     { "identityType": "Managed" },
    "resources":    { "instanceCount": 4, "shmSize": "2048g" }
  }
}
```

## Blob upload

`client.blob.upload_code(...)` walks the code dir (`utils/fs.walk_code`),
computes a content-addressed sha256, then tries credentials in order: SAS →
shared key → AAD bearer. Shared-key is skipped if the account disables it;
a mid-upload 403 retries once with bearer. The blob path is keyed by hash —
identical inputs reuse the prior upload.

## Dependencies

`click`, `pyyaml`, `rich`, `textual`, `azure-identity`, `requests`. Nothing
else at runtime.
