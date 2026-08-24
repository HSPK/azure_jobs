# Troubleshooting

Use after failed discovery, validation, submission, logs, or Kubernetes work.
Preserve concrete error type, command, stdout, and stderr.

## Safe first pass

```bash
aj --version
aj daemon status
aj --json auth status
aj --json config show
aj --json ws show
aj --json template list
```

Record cwd, `git status --short`, command, backend/template, approximate
timestamp, AJ ID/cloud name/ticket/Volcano name, status, code, and stderr. Do
not collect daemon journals, `record.jsonl`, Kubernetes Secret output, or SSH
files.

## Daemon login/startup

```bash
aj --json auth status
```

If status reports no usable login, the user runs `az login`; then restart and
recheck the daemon:

```bash
az login
aj daemon restart
aj --json auth status
```

For server tracebacks, restart so the daemon inherits debug mode:

```bash
AJ_DEBUG=1 aj daemon restart
AJ_DEBUG=1 aj --json auth status
```

Changes to `AJ_DEBUG`, `AJ_SHIP_SSH`, `AJ_LOG_LEVEL`, or daemon-side settings
need restart. For a stale socket, use `daemon stop`, then `daemon start`; avoid
force while submissions run unless explicitly requested.

## Protocol mismatch

Symptoms: incompatible API ranges, a known `/v2` route 404 after upgrade, or
immediate post-upgrade command failure.

```bash
aj daemon restart
aj daemon status
aj --version
```

Do not bypass the daemon or call private socket routes.

## Missing/invalid template

```bash
aj --json config show
aj --json template list
aj --json template validate
```

Check path `.azure_jobs/template/TEMPLATE_NAME.yaml`, leaf `base` key, base
resolution/cycles, first job/`sku`, and target/service. AML needs non-empty
string `target.name`; Sing name may be omitted/empty/string. Validation does
not prove quota, permissions, image, Kubernetes, or storage.

When merged config is needed:

```bash
aj --json template show TEMPLATE_NAME
```

## Missing workspace

```bash
aj --json ws list
aj ws set
aj --json ws set WORKSPACE
aj --json ws show
```

For Sing, verify supplied `target.workspace_name`; for AML, ensure
`target.name` identifies accessible compute.

## Quota/SKU rejection

```bash
aj --json quota list --aml --all
aj --json quota list --sing --all --full
aj --json sku list --all
aj --json image list
```

Compare nodes, SKU process selector, CPU/GPU family, accelerator, per-GPU
memory, tier, and VC filters. Errors may mean no matching CPU size or exact GPU
count, accelerator/memory mismatch, insufficient user quota, insufficient tier
quota, or filtered/inaccessible VCs. Do not repeatedly queue the same
chargeable request; explain and adjust deliberately.

## Archive/input/bootstrap

```bash
aj --json code stats -t TEMPLATE_NAME --list-all
```

Use the dry-run procedure in [job operations](job-operations.md#run-flags).

Check that ignores leave files; code has no unsupported directory symlinks or
non-regular files; datasets/checkpoints are excluded; image has `bash` and
runtime/setup tools; `.py` shorthand has `uv` or uses `python`; native input
downloads; logs show no hash, extraction, setup-barrier, or runner failure.
Native flow is one code archive plus a static bootstrap input; do not
substitute per-file Code Assets.

## Ambiguous submit/queue

Transport can fail after remote success. Never blind retry.

```bash
aj --json list -n 50
aj --json job list -n 100 -e EXPERIMENT --ws WORKSPACE
aj queue list
```

For AML/Sing compare AJ ID, name, experiment, timestamp, target, and portal.
For Volcano:

```bash
CTX=<KUBECTL_CONTEXT>
kubectl get jobs.batch.volcano.sh -n <KUBERNETES_NAMESPACE> \
  --context "$CTX" \
  --sort-by=.metadata.creationTimestamp
```

Inspect `app` labels. Retry only after proving no queue entry/remote job exists.

## Logs unavailable

Queued/not-started/provisioning/preparing jobs may have no logs:

```bash
aj --json job status JOB_OR_AJ_ID --ws WORKSPACE
sleep 15
```

Run `aj --json job logs JOB_OR_AJ_ID --ws WORKSPACE`. Use deadline/backoff. If
terminal without aggregate logs, inspect detail/portal or the dashboard
log-file picker. Log reads may be retried; submissions may not.

## Volcano context/queue/PVC

Start with the bounded wrapper:

```bash
aj k status --check-cluster
aj k queues
aj k jobs
```

```bash
CTX=<KUBECTL_CONTEXT>
NS=<KUBERNETES_NAMESPACE>
kubectl config current-context
kubectl get crd jobs.batch.volcano.sh --context "$CTX"
kubectl get queues.scheduling.volcano.sh <VOLCANO_QUEUE> --context "$CTX"
kubectl get pvc <PVC_NAME> -n "$NS" --context "$CTX"
kubectl auth can-i create jobs.batch.volcano.sh -n "$NS" --context "$CTX"
kubectl auth can-i create pods/exec -n "$NS" --context "$CTX"
```

Check exact context, namespace, queue, PVC, mount directory, and permissions.
PVC code transfer requires both PVC environment variables. Preserve
kubectl/tar output; never delete unrelated pods.

## Volcano Blob/SAS

Check daemon login, `az` availability, account/container placeholders, ability
to mint user-delegation SAS and read/write the container, pod Blob egress,
`azcopy` install, hash verification, and extraction.

Never print SAS URLs, Secret manifests, decoded Secrets, or raw Blob job/pod
descriptions: container arguments contain the read SAS. Do not use client-side
`kubectl apply` for credential manifests because last-applied annotations may
persist plaintext. For blobfuse, also check `/dev/fuse`, privileged-pod policy,
and installation; use narrow JSONPath/custom-column status, events, and logs,
not Secret output.

## Diagnostic report

Include versions, command, template shape, error/status/code, bounded
stdout/stderr, and Kubernetes status/events. Do not attach the entire
`.azure_jobs/` directory or Kubernetes Secret objects.
