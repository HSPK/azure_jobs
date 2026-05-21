# REST API

`aj` calls Azure ML and ARM directly over HTTPS — no `azure-ai-ml` SDK.

## Auth

`AzureCliCredential` from `azure-identity`. Tokens cached, refreshed 60 s early.

| Plane | Scope | Used for |
|-------|-------|----------|
| ARM | `https://management.azure.com/.default` | jobs, environments, datastores, computes, Resource Graph |
| Data | from workspace `discoveryUrl` (`https://ml.azure.com/.default`) | Run History (logs) |

China cloud auto-detected from `*.cn` URLs (scope + blob host switch).

## Clients

`core/az_client/` exposes:

**`AzureARMClient`** — cross-subscription:

| Method | Endpoint |
|--------|----------|
| `list_subscriptions()` | `GET /subscriptions` |
| `resource_graph_query(query, subs)` | `POST /providers/Microsoft.ResourceGraph/resources` |
| `vc.list(subscription_ids=None, *, with_raw=False)` | Resource Graph (`microsoft.machinelearningservices/virtualclusters`) |
| `list_workspace_computes(sub, rg, ws)` | `GET .../workspaces/{ws}/computes` |

**`AzureMLClient`** — workspace-scoped (api-version `2024-04-01`). Composed of namespaces:

```python
client.jobs.list(filters)                          # GET .../jobs
client.jobs.get(name) / cancel(name)
client.jobs.create_or_update(name, body)           # PUT .../jobs/{name}

client.environments.list_versions(name)
client.environments.create_or_update(name, ver, image)

client.datastores.list() / create_or_update(...)
client.datastores.list_secrets(name)               # SAS / account key

client.blob.upload_code(code_dir, ignore_patterns, extra_files, on_progress)
# walk_code → compute_code_hash → PUT blob → register code version

client.run_history.get_log_urls(job_name)          # data plane
```

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

## Dependencies

`click`, `pyyaml`, `rich`, `textual`, `azure-identity`, `requests`. Nothing else.
