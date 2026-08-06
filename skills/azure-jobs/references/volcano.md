# Volcano

Use this before submitting/managing Volcano. `aj` submits; `aj job` does not
manage Volcano resources.

## Target fields
```yaml
target:
  service: volcano
  namespace: <KUBERNETES_NAMESPACE>
  queue: <VOLCANO_QUEUE>
  context: <KUBECTL_CONTEXT>
  gpus_per_node: 8
  cpus_per_node: 96
  memory: 512Gi
  rdma: true
  priority_class: ""
  labels:
    workload: training
```

- Namespace defaults to context namespace, then `default`; queue defaults to
  `default`; context is passed to kubectl.
- `gpus_per_node: 0` is CPU-only. RDMA defaults off for CPU-only and on for GPU
  unless explicit.
- `target.name` is not a Kubernetes compute name.

See [templates](templates.md) for a complete leaf.

## Code strategies
Default PVC/`kubectl-exec`:
```yaml
jobs:
  - name: train
    sku: "{nodes}xG{processes}"
    submit_args:
      env:
        AMLT_PERSISTENT_VOLUME_NAME: <PVC_NAME>
        AMLT_PERSISTENT_VOLUME_MOUNT_DIR: /mnt/shared
```

The daemon creates a helper pod, streams selected files through
`tar | kubectl exec`, removes the helper, and copies the PVC tree into a
per-job work directory. Without both variables, it transfers no code.

Blob archive:
```yaml
_extra:
  code_upload:
    strategy: blob
    blob:
      storage_account: <STORAGE_ACCOUNT>
      container: <BLOB_CONTAINER>
      upload_dir: aj-code
      sas_expiry_days: 1
      pod_download_retries: 5
```

The daemon archives code, mints blob-scoped user-delegation SAS values, uploads,
and injects bootstrap that force-installs `azcopy`, downloads, verifies
SHA-256, and extracts. Never print the generated URL/SAS.

## Preflight
```bash
CTX=<KUBECTL_CONTEXT>
NS=<KUBERNETES_NAMESPACE>
QUEUE=<VOLCANO_QUEUE>
PVC=<PVC_NAME>
kubectl config current-context
kubectl config get-contexts "$CTX"
kubectl config view --minify --context "$CTX" \
  -o jsonpath='{.contexts[0].context.namespace}'; echo
```

Verify Volcano/queue:

```bash
kubectl get crd jobs.batch.volcano.sh --context "$CTX"
kubectl get queues.scheduling.volcano.sh "$QUEUE" --context "$CTX"
kubectl get jobs.batch.volcano.sh -n "$NS" --context "$CTX"
kubectl auth can-i create jobs.batch.volcano.sh -n "$NS" --context "$CTX"
kubectl auth can-i get pods -n "$NS" --context "$CTX"
kubectl auth can-i get pods/log -n "$NS" --context "$CTX"
```

PVC strategy:

```bash
kubectl get pvc "$PVC" -n "$NS" --context "$CTX"
kubectl auth can-i create pods -n "$NS" --context "$CTX"
kubectl auth can-i create pods/exec -n "$NS" --context "$CTX"
kubectl auth can-i delete pods -n "$NS" --context "$CTX"
```

Blob strategy: verify login/container access without requesting a SAS:

```bash
aj --json auth status
az storage container show \
  --account-name <STORAGE_ACCOUNT> \
  --name <BLOB_CONTAINER> \
  --auth-mode login --only-show-errors \
  --query '{name:name,publicAccess:properties.publicAccess}'
```

Stop if context, namespace, queue, PVC, CRD, or authorization is unexpected.

## Submit and capture the generated name
```bash
aj --json template validate TEMPLATE_NAME
aj --json code stats -t TEMPLATE_NAME
```

Use the private-output dry-run procedure from
[job operations](job-operations.md#run-flags). After confirmation, capture the
structured submission result privately:

```bash
AJ_SUMMARY="$(
  python3 <SKILL_DIR>/scripts/run-aj-json.py -- \
    aj --json run -t TEMPLATE_NAME -n 2 -p 8 --ppn 1 \
    python train.py
)"
printf '%s\n' "$AJ_SUMMARY"
JOB="$(
  printf '%s' "$AJ_SUMMARY" |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["azure_name"])'
)"
```

The manifest uses `metadata.generateName`; the actual name has a suffix.
`aj` returns that generated resource name as `azure_name`. Underlying kubectl
output has this form:

```text
job.batch.volcano.sh/<GENERATED_JOB_NAME> created
```

That name is authoritative; the normalized aj stem is not the full resource.

## Inspect and read logs
```bash
JOB=<GENERATED_JOB_NAME>
kubectl get jobs.batch.volcano.sh "$JOB" -n "$NS" --context "$CTX" -o wide
```

Do not use `describe` or `get -o yaml/json` on Jobs or pods. Blob command
arguments contain a short-lived read SAS, and PVC jobs can still contain
sensitive environment or command values. Query only narrow metadata/status
fields:

```bash
kubectl get jobs.batch.volcano.sh "$JOB" -n "$NS" --context "$CTX" \
  -o custom-columns='NAME:.metadata.name,STATE:.status.state.phase'
```

Jobs/pods use `app=<normalized-job-stem>`. Read it rather than guessing:

```bash
STEM="$(kubectl get jobs.batch.volcano.sh "$JOB" \
  -n "$NS" --context "$CTX" \
  -o jsonpath='{.metadata.labels.app}')"
kubectl get pods -n "$NS" --context "$CTX" -l "app=$STEM" \
  -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,NODE:.spec.nodeName,RESTARTS:.status.containerStatuses[*].restartCount'
```

Use [Kubernetes analysis](kubernetes-analysis.md) for conditions, bounded
redacted events, resources, PVC state, and layered diagnosis.

Logs from either strategy may contain credentials, environment values, or
signed URLs. Capture them privately and redact before display:

```bash
umask 077
RAW_LOG="$(mktemp)"
if kubectl logs -n "$NS" --context "$CTX" \
  -l "app=$STEM" --all-containers --prefix --tail=500 \
  >"$RAW_LOG" 2>&1; then
  LOG_STATUS=0
else
  LOG_STATUS=$?
fi
python3 <SKILL_DIR>/scripts/redact-log.py \
  --tail 200 --delete "$RAW_LOG"
test "$LOG_STATUS" -eq 0
```

Never print raw Job/pod descriptions or logs.

Do not use `aj job status/logs/cancel` for Volcano.

## Cancel/delete

Re-state context, namespace, and generated name; obtain confirmation unless
deletion was explicitly requested:

```bash
kubectl delete jobs.batch.volcano.sh "$JOB" -n "$NS" --context "$CTX"
```

Never delete by broad selector or remove a namespace/queue as cleanup.

## Storage/blobfuse security

`config.storage` uses blobfuse2 inside the container:

- daemon mints short-lived SAS values;
- Kubernetes receives a mounted Secret, not environment variables;
- storage pods are privileged for `/dev/fuse`;
- bootstrap installs blobfuse2 or fails;
- replacing the Secret can refresh a long job.

Never run `kubectl get secret ... -o yaml/json`, describe/decode a Secret, or
copy SAS-bearing manifests, raw Blob job/pod descriptions, or raw bootstrap
logs into Git, issues, or chat.
