# Azure REST clients

The daemon calls Azure ML and ARM directly over HTTPS — no `azure-ai-ml` SDK.
The CLI and public SDK call the daemon's `/v2` FastAPI surface instead; see
[architecture.md](architecture.md) for that request path and OpenAPI contract.

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

### `AzureClient` — account-scoped

| Namespace · method | Endpoint |
|--------------------|----------|
| `subscription.list()` | `GET /subscriptions` |
| `vc.list(sub_ids=None, *, with_raw=False)` | Resource Graph (`microsoft.machinelearningservices/virtualclusters`) |
| `vc.get(sub, rg, name)` | `GET .../virtualClusters/{name}` |
| `quota.list()` / `quota.get_by_name(name)` | Reads `properties.managedQuotas` |
| `ws.list()` / `ws.get(name)` | Resource Graph |
| `ws(sub, rg, name)` | Return a scoped `AzureWorkspaceClient` |
| `compute.list(sub, rg, ws)` / `compute.get(...)` | `GET .../workspaces/{ws}/computes` |
| `compute.list_all(workspaces)` | Parallel fan-out |
| `uai.list(subs)` | User-assigned managed identities |
| `sa.list(subs)` | Storage accounts |
| `sku.list(location, subscription_id=...)` | Singularity instance types |
| `image.list(subs)` | Singularity images |

`vc.list(..., with_raw=True)` additionally returns the raw GraphQL row for
quota parsing. `parse_managed_quotas(raw)` is a pure function and lives in
`arm/vc.py`.

### `AzureWorkspaceClient` — workspace-scoped (api `2024-04-01`)

Constructed with `WorkspaceCoords` (sub / rg / ws). Resolves `discoveryUrl`
lazily on first data-plane call.

```python
client.job.fetch(limit, ...)                       # GET .../jobs
client.job.get(name)
client.job.cancel(name)
client.job.create_or_update(name, body)            # PUT .../jobs/{name}
client.job.get_run_log_urls(name)                  # → log.get_urls

client.env.list_versions(name)
client.env.create_or_update(name, ver, image)

client.ds.list()
client.ds.create_or_update(name, body)
client.ds.list_secrets(name)                       # SAS / account key

client.blob.upload_code(code_dir, ignore_patterns, extra_files, on_progress)
# walk_code → compute_code_hash → PUT blob → register code version

client.log.get_urls(job_name)                      # data plane (run-history)
client.log.list_files(job_name)
client.log.download(job_name)
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
