# System design

## Status

These documents describe the current architecture. User-facing behavior
remains documented in the User guide and Reference sections.

## Architecture

Azure Jobs has four layers and one execution path:

```text
CLI / TUI / Python
        │
        ▼
      SDK
        │ HTTP over a private Unix socket
        ▼
     daemon
        │
        ├── Azure REST
        ├── submission backends
        └── persistent queue and watches
```

| Layer | Responsibility |
| --- | --- |
| `shared/` | wire models, templates, `JobSpec`, typed backend options |
| `sdk/` | public resource namespaces and HTTP-over-UDS transport |
| `client/` | Click, Textual, Rich/JSON, and host-local tools |
| `server/` | FastAPI routes, contexts, Azure clients, and execution |

Host changes stay local. Agent Skill management and Kubernetes tool
installation do not cross the daemon. Kubernetes task management is also
local in the current hybrid design; Volcano submission is daemon-backed.

## Documents

### Core

| System | Design |
| --- | --- |
| Runtime and API boundary | [Client and daemon](client-daemon.md) |
| Template-to-wire translation | [Templates and JobSpec](job-description.md) |
| Execution strategies | [Submission backends](submission-backends.md) |
| Heterogeneous Volcano roles | [Heterogeneous Volcano tasks](volcano-heterogeneous-tasks.md) |

### Client systems

| System | Design |
| --- | --- |
| Interactive dashboard | [TUI dashboard](tui.md) |
| Cluster access and operations | [Kubernetes management](kubernetes.md) |
| Managed agent guidance | [Agent Skill lifecycle](agent-skill.md) |

## Cross-cutting invariants

1. Azure authentication and execution are daemon-only.
2. `shared` is transport-neutral; `sdk` knows HTTP but no implementation.
3. Client and server never import each other.
4. Backend description and execution register separately.
5. Mutations are not retried after an ambiguous response.
6. Errors retain type, detail, and recovery guidance.
7. New behavior extends narrow contracts instead of adding service branches.

Architecture, OpenAPI, registry, and daemon-only tests enforce these rules.
