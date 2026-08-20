# Resources and configuration

Use this for auth, daemon, workspace, quota, SKU, image, datastore,
environment, identity, storage-account, config, and code inspection.

## JSON rule

Place the global flag first:

```bash
aj --json ws list
aj --json job list
```

Tables return rows/metadata; detail/actions return typed envelopes. `aj init`,
`aj template init`, and `aj dash` are interactive, not JSON workflows.

## Auth and daemon

If authentication is missing, have the user complete the interactive login;
never script or request credentials:

```bash
az login
aj --json auth status
aj daemon status
aj daemon start
aj daemon restart
aj daemon stop
aj daemon stop --timeout 60
```

Auth reports the daemon's credential; the CLI does not acquire Azure tokens.
Normal stop drains work. `daemon stop --force` can lose active outcomes and
requires confirmation unless explicitly requested. Daemon environment changes
require restart:

```bash
AJ_DEBUG=1 aj daemon restart
```

## Workspaces

```bash
aj --json ws list
aj --json ws show
aj --json ws show WORKSPACE
aj ws set
aj --json ws set WORKSPACE
```

No-name `ws set` is interactive. Selection is local config.

## Quota and SKUs

```bash
aj --json quota list --aml
aj --json quota list --aml --all
aj --json quota list --sing
aj --json quota list --sing --all --full
aj --json sku list
aj --json sku list --all
```

`--all` includes zero-quota families; `--full` adds Sing VC coordinates.

Sing auto-selection with omitted/empty `target.name`:

1. filters optional target subscription/resource group;
2. matches GPU count exactly;
3. matches accelerator/per-GPU memory exactly when specified;
4. requires user and tier quota;
5. when accelerator is unspecified and both vendors match within one VC,
   prefers NVIDIA;
6. ranks tier, NVLink, remaining quota, stable coordinates.

Error interpretation:

- no VCs: sign-in/access/discovery;
- no exact size: adjust `-p` or SKU;
- accelerator/memory mismatch: inspect `sku list --all`;
- user/tier quota exhausted: requested capacity unavailable;
- zero filtered candidates: check subscription/resource group.

NVLink may fall back with a warning. Capture selected VC, effective tier, and
matched instance.

## Images

```bash
aj --json image list
aj --json image list -f cuda
aj --json image list --filter torch
```

This lists native Sing images/aliases. AML/Volcano images are direct template
values and must meet runtime/registry requirements.

## Datastores and environments

```bash
aj --json ds list
aj --json ds list --ws WORKSPACE
aj --json ds show DATASTORE --ws WORKSPACE
aj --json env list
aj --json env list --ws WORKSPACE
aj --json env show ENVIRONMENT
aj --json env show ENVIRONMENT -n 20 --ws WORKSPACE
```

These inspect Azure ML resources without exposing datastore credentials.

## Identities and storage accounts

```bash
aj --json uai list
aj --json uai list --full
aj --json sa list
```

Identity output may contain client/ARM IDs; treat as private metadata and do
not paste real values into public templates. Never request/persist identity
credentials. Storage discovery does not print keys/SAS.

## Configuration

```bash
aj --json config show
aj --json config timezone
aj --json config timezone UTC
aj --json config experiment
aj --json config experiment EXPERIMENT
```

Local config may contain workspace coordinates and template remote. Never
publish `.azure_jobs/aj_config.json`.

## Code payload

```bash
aj --json code stats -t TEMPLATE_NAME
aj --json code stats -t TEMPLATE_NAME -d PATH -n 20
aj --json code stats -t TEMPLATE_NAME --list-all
```

Use file count/size/hash/largest files to catch datasets, checkpoints,
credentials, or SSH material before submission.

## Compact discovery

```bash
aj --version
aj daemon status
aj --json auth status
aj --json config show
aj --json ws show
aj --json template list
aj --json quota list --sing --all --full
aj --json sku list --all
aj --json image list
```

Run only backend-relevant commands.
