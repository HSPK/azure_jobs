# Daemon HTTP and Azure REST APIs

This page describes contracts and boundaries, not every generated endpoint.
Use the live OpenAPI schema for the complete local surface.

## Local daemon transport

The daemon is FastAPI/uvicorn serving ordinary HTTP over a private Unix domain
socket. `httpx` is the SDK transport.

```bash
if [ -n "${AJ_RUNTIME_DIR:-}" ]; then
  RUNTIME="$AJ_RUNTIME_DIR"
elif [ -n "${XDG_RUNTIME_DIR:-}" ]; then
  RUNTIME="$XDG_RUNTIME_DIR/aj"
else
  RUNTIME="/tmp/aj-$(id -u)"
fi
SOCKET="$RUNTIME/daemon.sock"
curl --unix-socket "$SOCKET" http://aj-daemon/v2/info
curl --unix-socket "$SOCKET" http://aj-daemon/openapi.json
```

`/v2/info` advertises `api_version` and `min_api_version`. Client and daemon
accept overlapping version ranges. `/v1/info` and `/v1/retire` exist only as
CLI upgrade fallbacks for stopping an old daemon; normal resources never
downgrade.

## SDK-sent headers

The SDK sends:

| Header | Meaning |
| --- | --- |
| `X-AJ-Root` | absolute project `AJ_HOME`; enforced by project/workspace routes |
| `X-AJ-Client` | client package version metadata; not currently enforced |

Example project-scoped request:

```bash
curl --unix-socket "$SOCKET" \
  -H "X-AJ-Root: $(pwd)/.azure_jobs" \
  "http://aj-daemon/v2/workspaces/_/jobs:fetch?limit=10"
```

`_` is the default-workspace sentinel and cannot collide with a valid Azure ML
workspace name.

## Resource shape

The API follows the SDK namespaces:

- daemon: info, retire, auth, events;
- account: subscriptions, workspaces, instance types, quota, storage accounts,
  identities, images, and compute discovery;
- workspace: jobs, logs, datastores, environments, compute, quota,
  submissions, queue, and watches.

Paths are versioned under `/v2`. SDK and FastAPI intentionally use direct path
strings rather than a shared route builder. `tests/test_openapi_contract.py`
drives every SDK operation through a recorder and verifies method, path, and
query parameters against generated OpenAPI, preventing silent drift.

## Submit and watch events

`GET /v2/events` is one server-sent event stream:

```text
data: {"topic": "queue.changed", ...}
```

It carries:

- persisted watch transitions;
- queue transitions;
- `submit.progress` events correlated by a client-generated stream ID.

Slow subscribers have bounded queues and may lose events rather than blocking
the daemon. The SDK reconnects the SSE stream after a drop.

Submission itself is a normal `POST` to the workspace submissions resource.
Queue submission is a separate persisted resource returning a ticket.

## Logs and HTTP Range

Log content uses byte ranges:

```http
GET /v2/workspaces/{ws}/jobs/{id}/logs/content?path=...
Range: bytes=-65536
```

Successful reads return:

```http
206 Partial Content
Content-Type: application/octet-stream
Content-Range: bytes START-END/TOTAL
X-AJ-Total-Size: TOTAL
```

Suffix, bounded, and open-ended ranges are supported. Each request opens and
closes its Azure reader, so clients cannot leak server-side log handles.

## Error envelope

Domain and unexpected failures return a typed envelope:

```json
{
  "error": {
    "type": "RestError",
    "message": "403: permission denied",
    "status_code": 403,
    "azure_code": "AuthorizationFailed"
  }
}
```

The SDK reconstructs an allowlisted exception type. Unknown types become
`RemoteError`. Tracebacks cross the socket only when `AJ_DEBUG=1`.

## Local security

- runtime directory: owned by the current UID, mode `0700`;
- socket: owned by the current UID, mode `0600`;
- clients reject non-sockets and sockets owned by another UID;
- token caches and daemon journals are private;
- `X-AJ-Root` scopes state but is not authentication.

The operating-system UDS permissions are the authentication boundary. Do not
expose the app on TCP.

## Internal Azure HTTP clients

Only `server/az_client/` talks to Azure.

`AzureClient` is account-scoped:

```text
subscription  ws  sku  sa  uai  image  quota  compute
```

`AzureWorkspaceClient` is workspace-scoped:

```text
job  log  ds  env  blob  info()
```

The clients use `requests` and `azure-identity`, not `azure-ai-ml`.
Authentication scopes are:

| Plane | Scope |
| --- | --- |
| ARM/Azure ML control | `https://management.azure.com/.default` |
| ML data plane | workspace discovery host, normally `https://ml.azure.com/.default` |
| Blob storage | `https://storage.azure.com/.default` |

Tokens refresh 60 seconds early and use private cross-process caches. Sessions
retry throttling and transient 5xx responses with backoff. Azure REST errors
retain HTTP status and Azure error code.

Workspace clients use Azure ML API `2024-04-01`, lazily resolve the data-plane
discovery URL, and close deterministically. Native code upload uses one
content-addressed Blob object; see [Submitting](submitting.md).

## Remote HTTP is not implemented

A remote bind requires more than TLS:

- mTLS or OIDC authentication and authorization;
- tenant/project isolation instead of trusting `X-AJ-Root`;
- content-addressed source artifact upload;
- server-owned journals and state;
- principal-filtered SSE;
- an explicit delegated or service identity model;
- mutation idempotency keys;
- a ban on reading server-home SSH keys.

Until those contracts exist, UDS is the only supported transport.
