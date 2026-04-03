# Configuration

Azure Jobs uses two types of configuration: a tool config file and YAML job templates.

## aj_config.json

Located at `.azure_jobs/aj_config.json`. Stores three things:

**Defaults** — the template, nodes, and processes to use when not specified on the command line. Updated automatically each time you run a job (last-used template becomes the new default).

**Repository** — the Git URL for `aj pull`. Saved after the first pull so subsequent pulls don't need the URL again.

**Workspace** — Azure subscription, resource group, and workspace name. Prompted interactively on first use.

## Templates

Templates live in `.azure_jobs/template/` as YAML files. Each template has two top-level keys:

- **`base`** — optional. A template name (or list of names) to inherit from.
- **`config`** — the actual job configuration passed to amlt.

### Inheritance

A template can extend one or more bases. The config from all bases is merged together, then the child's config is merged on top. Chains can be arbitrarily deep (grandparent → parent → child).

Base names are resolved as follows:
- Plain name like `gpu` → looks for `gpu.yaml` in the same directory as the current template
- Dotted name like `envs.gpu` → looks for `.azure_jobs/envs/gpu.yaml`

Circular inheritance is detected and raises an error.

### Merge Rules

When configs are merged (base first, child last):

- **Dicts** merge recursively — keys from both sides are kept, overlapping keys merge deeper
- **Lists of dicts** merge by position — the first item merges with the first item, second with second, etc.
- **Lists of scalars** concatenate
- **Scalars** — last value wins

### _extra Section

The `_extra` key inside `config` holds default resource values:

- `nodes` — default node count (overridable with `-n`)
- `processes` — default process count (overridable with `-p`)

This key is consumed by `aj` and stripped before submission.

### SKU Templates

The `sku` field in a job supports two formats:

**String template** — placeholders `{nodes}` and `{processes}` are substituted at submit time.

**Range dict** — keys define node ranges, values are the SKU to use. Supports exact match (`"1"`), ranges (`"2-4"`), and open-ended (`"8+"`).

## Override Priority

When determining template, nodes, and processes, the priority is:

1. CLI flags (`-t`, `-n`, `-p`) — highest
2. Template `_extra` values
3. `aj_config.json` defaults
4. Fallback: 1 node, 1 process

## Environment Variables

These are exported into the job command environment:

| Variable | Value |
|----------|-------|
| `AJ_NODES` | Number of nodes |
| `AJ_PROCESSES` | Total processes (nodes × processes per node) |
| `AJ_NAME` | Job name |
| `AJ_ID` | Submission ID (8-char hex) |
| `AJ_TEMPLATE` | Template name used |
| `AJ_SUBMIT_TIMESTAMP_UTC` | ISO 8601 submission time |
| `AJ_HOME` | Path to `.azure_jobs` directory (for customizing the root) |
