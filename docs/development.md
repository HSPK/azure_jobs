# Development

## Setup

```bash
git clone https://github.com/HSPK/azure_jobs.git
cd azure_jobs
uv sync --dev
uv run aj --help
uv run pytest -q
uv run mkdocs build --strict
```

Python 3.10 and 3.12 are exercised in CI.

## Source map

| Area | Location |
| --- | --- |
| shared wire contracts | `src/azure_jobs/shared/contract/` |
| template engine | `src/azure_jobs/shared/template/` |
| JobSpec construction | `src/azure_jobs/shared/job/` |
| public SDK | `src/azure_jobs/sdk/` |
| Click commands | `src/azure_jobs/client/cli/` |
| Textual dashboard | `src/azure_jobs/client/tui/` |
| Rich/JSON rendering | `src/azure_jobs/client/ui/` |
| FastAPI daemon | `src/azure_jobs/server/app.py` |
| context lifecycle | `src/azure_jobs/server/context.py` |
| wire adapters | `src/azure_jobs/server/resources.py` |
| Azure REST clients | `src/azure_jobs/server/az_client/` |
| submission backends | `src/azure_jobs/server/submit/` |

See [System design](design/index.md) for request, submission, and subsystem
boundaries.

## Hard design invariants

### Four-layer boundary

- `shared/` is transport- and presentation-neutral.
- `sdk/` knows HTTP, not client or server implementation.
- `client/` imports neither Azure clients nor `server/`.
- `server/` imports no frontend.
- Azure authentication and commands are daemon-only.
- Rich/Textual are client-only.
- There is no in-process execution mode.

Run:

```bash
uv run pytest -q tests/test_api_architecture.py tests/test_api_daemon_only.py
```

### Backend extension seam

Backend description and execution register separately:

- shared `register_spec(...)`: typed option builder/loader and name normalizer;
- server `register_backend(...)`: executable submit function and label.

`shared/job/build.py` must never branch on service. It constructs `JobSpec`
directly from shared hooks.

### `backend_spec` versus `extra`

- `JobSpec.backend_spec`: typed backend options (`AmlOpts`, `VolcanoOpts`).
- `JobSpec.extra`: verbatim template `_extra`, interpreted only by its consumer.
- amlt rendering strips `_extra`.

Do not add strategy-specific top-level `JobSpec` fields.

### Narrow contracts

Concrete backends, SDK namespaces, and uploaders must preserve their protocol:
return shape, errors, cleanup, and side effects. Keep interfaces narrow. Add a
separate optional protocol instead of growing unrelated methods.

## Extension recipes

### Add an SDK/daemon operation

1. Add the FastAPI route in `server/app.py`.
2. Add the narrow resource operation in `server/resources.py` if translation
   is needed.
3. Add the SDK namespace method using a direct HTTP path string.
4. Add the operation to `tests/test_openapi_contract.py`.
5. Add typed wire fields/models under `shared/contract/` only when required.
6. Expose it in CLI/TUI as presentation over the SDK, not server imports.

### Add an Azure resource

1. Put account or workspace HTTP behavior under `server/az_client/`.
2. Keep authentication/retry in the existing session abstractions.
3. Adapt Azure rows to `CatalogItem` or another shared model in
   `server/resources.py`.
4. Add daemon route, SDK namespace, CLI renderer, and contract tests.

### Add a submission backend

1. Define typed options beside shared backend options.
2. Register shared build/load/name hooks.
3. Implement daemon execution under `server/submit/<backend>/`.
4. Register with `server.submit.register_backend`.
5. Test the backend contract and prove `build.py` remains service-agnostic.

Do not add `if service == ...` to CLI, SDK, routes, or shared build.

### Add a Volcano upload strategy

The two current strategies intentionally use `pick_uploader(extra)` with
literal branches. Add another branch and keep its options parser beside the
consumer. Switch to a registry only when the strategy count is large enough to
justify it.

`submit_via_volcano` depends only on the `CodeUploader` contract.

### Add a template field

- Backend-owned field: add it to the backend `Opts` dataclass and
  `from_template`.
- Opaque aj-only feature: place it under `_extra.<feature>` and parse it in the
  consumer.
- amlt-owned compatibility field: preserve it in `Template.raw` and rendering.

Never teach shared build about consumer-specific `_extra`.

### Add a TUI feature

Use command metadata, controllers, stores, typed events, and view ports.
Controllers perform I/O but do not own mutable canonical state. Background
workers must return through the UI dispatcher; they cannot mutate stores
directly.

## Scripts injected into jobs

Any new Bash, Python, or YAML snippet of roughly 15 lines or more must be a
standalone resource under the owning backend. New parameterized resources use
the local `_scripts.load_script(name, **subs)` helper. The existing Volcano
distributed preamble and blobfuse mount script predate that helper and are
loaded directly; do not copy that pattern for new scripts.

Placeholders use literal `{KEY}` substitution. Do not introduce a templating
engine without a real need for conditionals or loops.

Pod-side scripts must install required binaries or fail with a direct message.
Do not assume `azcopy`, `tar`, `uv`, `jq`, or similar tools exist.

## Error rules

- Broad `except` blocks call `log.exception(...)`.
- User-visible errors include `Type: message`.
- Add `AJ_DEBUG=1` guidance when the traceback is hidden.
- Subprocess errors include command, exit status, stdout, and stderr.
- Preserve typed errors across HTTP; do not flatten `RestError`.
- Do not automatically retry ambiguous mutations.

## Type and state rules

- Prefer dataclasses/shared wire types over unstructured cross-layer dicts.
- Keep `CatalogItem.raw` for resource-specific display data.
- Close HTTP clients, readers, contexts, and task resources deterministically.
- Bound long-lived caches, queues, workers, and subscriber buffers.
- Keep pure parsers and selectors separate from network calls where possible.

## Documentation rules

- `aj --help`, code, and tests are authoritative.
- Use short sections and copyable examples.
- Link to the owning page instead of repeating backend flows.
- Do not document planned features as available.
- Update `mkdocs.yml` when adding, renaming, or deleting a page.

## Targeted verification

```bash
# Templates and JobSpec
uv run pytest -q tests/test_merge_confs.py tests/test_read_conf.py \
  tests/test_backend_registry.py tests/test_submit.py

# SDK and HTTP contract
uv run pytest -q tests/test_sdk.py tests/test_openapi_contract.py \
  tests/test_api_contract.py tests/test_api_transport.py

# Native submission
uv run pytest -q tests/test_server_submit_archive.py \
  tests/test_server_azureml_bootstrap_payload.py tests/test_sing_auto_selection.py

# Volcano
uv run pytest -q tests/test_volcano_uploaders.py tests/test_volcano_storage.py

# TUI
uv run pytest -q tests/test_tui_architecture.py tests/test_dashboard.py

# Documentation
uv run mkdocs build --strict
```

Run the full hermetic coverage suite before merging cross-cutting changes; see
[Testing](testing.md).
