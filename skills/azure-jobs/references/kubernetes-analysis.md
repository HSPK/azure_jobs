# Kubernetes and Volcano task analysis

Use this for an existing Volcano task or pod. The workflow is read-only unless
the user explicitly requests a mutation.

## Principles

- Confirm context, namespace, generated Volcano Job name, and strategy first.
- Prefer narrow JSONPath/custom-column fields over full YAML/JSON.
- Run one layer at a time. Preserve failed reads as uncertainty; do not append
  `|| true`, replace `kubectl`, or try broader output to hide an error.
- Use the Job's returned `app` label as the pod selector; do not guess labels.
- Aggregate node capacity without node names unless a targeted placement check
  specifically requires identities.
- Specify the container for `exec` and pod-specific logs.
- Retry only short, idempotent reads when a proxy returns transient HTML or
  gateway errors.
- Do not run several full log/storage scans concurrently.
- Do not restart/delete pods, Jobs, queues, namespaces, PVC data, or Secrets
  without explicit confirmation.
- Blob strategy: never use Job/pod `describe` or full YAML/JSON because command
  arguments contain a read SAS.

Use `aj k status`, `aj k jobs`, `aj k pods --job`, `aj k logs`, and
`aj k events` for standard bounded diagnostics. Fall back to the explicit
`kubectl` commands below only when the wrapper does not expose the required
field. `aj k install` changes the host and requires a reviewed plan plus
confirmation. `aj k login` only merges kubeconfig and performs OIDC login.

## 1. Establish the target

```bash
CTX=<KUBECTL_CONTEXT>
NS=<KUBERNETES_NAMESPACE>
JOB=<GENERATED_VOLCANO_JOB_NAME>

kubectl config current-context
kubectl config get-contexts "$CTX"
kubectl get jobs.batch.volcano.sh "$JOB" -n "$NS" --context "$CTX" \
  -o custom-columns='NAME:.metadata.name,QUEUE:.spec.queue,STATE:.status.state.phase'
```

Read the stable pod selector:

```bash
STEM="$(kubectl get jobs.batch.volcano.sh "$JOB" -n "$NS" --context "$CTX" \
  -o jsonpath='{.metadata.labels.app}')"
kubectl get pods -n "$NS" --context "$CTX" -l "app=$STEM" \
  -o custom-columns='NAME:.metadata.name,PHASE:.status.phase,NODE:.spec.nodeName,RESTARTS:.status.containerStatuses[*].restartCount'
```

If the Job does not exist, stop before retrying submission. Reconcile
namespace, context, creation time, normalized stem, and prior `azure_name`.

## 2. Classify before reading large logs

### Job/queue

```bash
QUEUE=<VOLCANO_QUEUE>
kubectl get queues.scheduling.volcano.sh "$QUEUE" --context "$CTX" \
  -o custom-columns='NAME:.metadata.name,STATE:.status.state'
kubectl get jobs.batch.volcano.sh "$JOB" -n "$NS" --context "$CTX" \
  -o jsonpath='{.status.state.phase}{"\t"}{.status.state.reason}{"\t"}{.status.state.message}{"\n"}'
```

### Pod conditions and container state

```bash
POD=<POD_NAME>
kubectl get pod "$POD" -n "$NS" --context "$CTX" \
  -o jsonpath='{range .status.conditions[*]}{.type}{"="}{.status}{":"}{.reason}{" "}{end}{"\n"}'
kubectl get pod "$POD" -n "$NS" --context "$CTX" \
  -o jsonpath='{range .status.containerStatuses[*]}{.name}{" waiting="}{.state.waiting.reason}{" terminated="}{.state.terminated.reason}{" exit="}{.state.terminated.exitCode}{" restarts="}{.restartCount}{"\n"}{end}'
```

Classify:

| Evidence | Likely layer |
| --- | --- |
| Pending + Unschedulable | GPU/CPU/memory, taint, affinity, queue |
| Pending + PVC unbound/mount error | storage/PVC/CSI/blobfuse |
| ImagePullBackOff | registry/image/pull secret |
| Init/setup failure | code archive, package install, mount/bootstrap |
| Running + restarts | process crash/OOM/liveness |
| Failed exit code | user command/runtime |

## 3. Events

Events are often more useful than logs for Pending tasks. Capture and redact
them because admission errors can echo user-provided values:

```bash
umask 077
RAW_EVENTS="$(mktemp)"
if kubectl get events -n "$NS" --context "$CTX" \
  --field-selector "involvedObject.name=$POD" \
  --sort-by=.lastTimestamp >"$RAW_EVENTS" 2>&1; then
  EVENT_STATUS=0
else
  EVENT_STATUS=$?
fi
python3 <SKILL_DIR>/scripts/redact-log.py \
  --tail 100 --delete "$RAW_EVENTS"
test "$EVENT_STATUS" -eq 0
```

Redact internal names before sharing.

## 4. Logs safely

List containers:

```bash
kubectl get pod "$POD" -n "$NS" --context "$CTX" \
  -o jsonpath='{.spec.containers[*].name}{"\n"}'
```

Capture current and previous logs privately:

```bash
CONTAINER=<CONTAINER_NAME>
umask 077
RAW_LOG="$(mktemp)"
if kubectl logs "$POD" -n "$NS" --context "$CTX" -c "$CONTAINER" \
  --tail=500 >"$RAW_LOG" 2>&1; then
  LOG_STATUS=0
else
  LOG_STATUS=$?
fi
python3 <SKILL_DIR>/scripts/redact-log.py \
  --tail 200 --delete "$RAW_LOG"
test "$LOG_STATUS" -eq 0
```

For restarted containers:

```bash
umask 077
RAW_LOG="$(mktemp)"
if kubectl logs "$POD" -n "$NS" --context "$CTX" -c "$CONTAINER" \
  --previous --tail=500 >"$RAW_LOG" 2>&1; then
  LOG_STATUS=0
else
  LOG_STATUS=$?
fi
python3 <SKILL_DIR>/scripts/redact-log.py \
  --tail 200 --delete "$RAW_LOG"
test "$LOG_STATUS" -eq 0
```

Treat logs as sensitive: redact URL queries, tokens, credentials, and
environment values. Omit user paths and unrelated payloads from the report.
Blob jobs may print signed URLs; never print raw logs or descriptions.

## 5. Short exec diagnostics

Only exec when logs/events are insufficient. Verify a harmless command:

```bash
python3 <SKILL_DIR>/scripts/kubectl-exec-retry.py \
  --context "$CTX" --namespace "$NS" --container "$CONTAINER" \
  "$POD" -- echo ok
```

Then use short, idempotent reads such as `pwd`, `id`, `df -h <mount>`, or
`test -r <path>`. Never run `env`, `set`, `cat .env`, Secret reads, broad
filesystem scans, or mutation commands.

If the API proxy returns HTML, Cloudflare, bad gateway, timeout, or upgrade
errors, the bundled script retries. It does not retry a real command failure.

For expensive diagnostics, do not hold `kubectl exec` open. Prefer one
low-priority process writing a bounded report to `/tmp`, then retrieve that
report with a short read. Obtain confirmation before launching anything that
could compete with training I/O or CPU.

## 6. Resource and storage checks

Read requested resources without full pod JSON:

```bash
kubectl get pod "$POD" -n "$NS" --context "$CTX" \
  -o jsonpath='{range .spec.containers[*]}{.name}{" requests="}{.resources.requests}{" limits="}{.resources.limits}{"\n"}{end}'
set -o pipefail
kubectl get nodes --context "$CTX" \
  -o custom-columns='GPU:.status.allocatable.nvidia\.com/gpu,RDMA:.status.allocatable.rdma/rdma_shared_device_a' \
  --no-headers | sort | uniq -c
```

PVC strategy:

```bash
kubectl get pvc <PVC_NAME> -n "$NS" --context "$CTX" \
  -o custom-columns='NAME:.metadata.name,STATUS:.status.phase,VOLUME:.spec.volumeName'
```

Blob/blobfuse strategy:

- inspect only status/events/sanitized logs;
- check egress, `/dev/fuse`, privileged policy, and bootstrap errors;
- never inspect Secret contents, full command arguments, or SAS-bearing
  manifests.

## 7. Report

Separate:

1. **Observed facts**: exact phase, condition, reason, exit code, restart count,
   event, and bounded redacted log lines.
2. **Conclusion**: only what the evidence establishes.
3. **Hypotheses**: ranked alternatives with the check that would distinguish
   them.
4. **Uncertainty**: missing logs/events, proxy failures, inaccessible storage,
   or stale state.
5. **Next action**: read-only checks first; mutations listed separately and
   confirmation-gated.

Never call a scheduling problem a user-code failure, or a proxy HTML response
the command's output.
