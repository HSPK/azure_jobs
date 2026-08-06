# Quota checks and common answers

Use this reference to evaluate capacity and answer common Azure Jobs questions
without guessing.

## Quota workflow

Collect only the relevant backend data:

```bash
aj --json quota list --aml --all
aj --json quota list --sing --all --full
aj --json sku list --all
aj --json image list
```

Do not transfer flags between groups: `sku list` has `--all`, but no `--sing`,
`--aml`, or `--full`.

### AML

AML templates select a named compute. Check:

- workspace and compute name;
- provisioning state;
- idle, busy, and maximum nodes;
- requested node count;
- image/runtime readiness.

An idle node is fastest. A cluster with scale capacity may still queue while
Azure provisions a node. AML quota output is not proof that a particular VM
SKU is immediately allocatable.

### Singularity

Parse the requested SKU:

```text
<nodes>x<per-GPU-memory>G<GPUs-per-node>-<accelerator>[-NvLink]
```

For `1x40G1-A100`, require:

- one-GPU instance type;
- A100 accelerator;
- 40 GB per GPU;
- current user quota and selected tier quota both at least one.

Availability is `limit - used`. GPU candidates consume
`nodes * instance GPUs`; CPU candidates consume `nodes * instance vCPUs`.
When accelerator or memory is omitted, it is not an exact filter.

Auto-selection order:

1. requested tier, then allowed fallback tiers;
2. when accelerator is unspecified and both vendors match, per-VC NVIDIA
   preference over AMD;
3. NVLink when requested;
4. greater remaining effective quota;
5. stable VC coordinates.

Never claim a candidate is available from `limit` alone. Use current `used`,
the concrete instance type, and both user/tier availability.

## Common replies

### “Why did no VC get selected?”

Reply with:

1. requested SKU, nodes, GPUs/node, and tier;
2. number of VCs checked and optional subscription/RG filters;
3. bounded rejection reasons: model, memory, instance size, user quota, tier
   quota, or discovery access;
4. exact next commands: `quota list --sing --all --full` and
   `sku list --all`.

Do not answer only “quota insufficient” when hardware matching failed first.

### “Why did it use Standard instead of Premium?”

The requested tier lacked enough current capacity for the matched instance,
and the selector followed the tier fallback chain. Report both requested and
effective tier plus current availability. A zero tier limit requires a quota
grant/increase or another VC; exhausted `limit - used` may recover when usage
falls. This is expected behavior, not a silent random choice.

### “Quota looks free; why is the task queued?”

Quota is an upper bound, not immediate hardware availability. Possible causes:

- another allocation changed usage after inspection;
- node provisioning or image pull;
- VC/queue scheduling pressure;
- Kubernetes admission, affinity, taint, GPU, or RDMA constraints.

For AML/Sing, poll status/logs. For Volcano, use the layered Kubernetes
analysis reference.

### “Why are there no logs?”

Queued, NotStarted, Provisioning, and Preparing jobs may not have user logs.
Check status first, wait with a deadline, then retry log download. A log read
is safe to retry; a submission is not.

### “Can I retry after a timeout?”

Read-only calls: usually yes. Submit, queue, cancel, delete: no blind retry.
Reconcile local records, cloud jobs, queue tickets, or Kubernetes resources by
name and timestamp first.

### “Does code stats show everything uploaded?”

It shows project-selected files. Native AML/Sing may additionally inject the
runner and, unless disabled, a home SSH whitelist. Agent workflows should use
`AJ_SHIP_SSH=0` and inspect project `.ssh/`.

### “How do I answer a failure report?”

Keep scheduling/quota failures separate from runtime failures. In particular,
changing Premium/Standard does not increase per-GPU memory; for CUDA OOM,
reduce memory use or choose a GPU SKU with more per-GPU memory.

Use this compact structure:

```text
Observed:
- exact status/error/code and timestamp
- target workspace/VC/namespace and requested resources

Evidence:
- relevant quota/SKU row, event reason, or bounded log excerpt

Conclusion:
- one supported root cause, or clearly labeled hypotheses

Next action:
- one or two concrete read-only checks or a proposed mutation requiring consent

Uncertainty:
- missing logs, stale quota snapshot, inaccessible resource, or ambiguous outcome
```

Never paste credentials, full environment maps, raw SAS URLs, Secret output,
or unrelated logs.
