# Heterogeneous Volcano tasks

## Status

**State:** Implemented.

## Context

Some workloads need a CPU coordinator and multiple GPU worker types inside one
gang-scheduled Job. Independent submissions lose one lifecycle, shared service
discovery, and all-or-nothing scheduling.

## Goals

- Render one Volcano Job with multiple heterogeneous Tasks.
- Configure topology entirely in YAML.
- Allow per-Task image, setup, command, env, resources, and node selector.
- Share Job name, queue, code upload, storage, and deletion.
- Preserve the current homogeneous path.

## Non-goals

- Multiple independent `jobs[]` submissions or workflow dependencies.
- Automatic balancing of mixed GPU generations.
- Arbitrary Pod spec passthrough.
- Per-Task code archives or storage in the first version.
- AML or Singularity heterogeneous nodes.

## Design

### Schema

Tasks are keyed mappings under the Volcano-owned `_extra` namespace:

```yaml
base:
config:
  target:
    service: volcano
    namespace: training
    queue: default
    context: <KUBECTL_CONTEXT>

  environment:
    image: common-runtime:latest
    setup: [python -m pip install -e .]

  jobs:
    - name: train
      sku: heterogeneous
      submit_args:
        env: {LOG_LEVEL: INFO}
        container_args: {shm_size: 64Gi}

  _extra:
    volcano:
      tasks:
        master:
          replicas: 1
          cpus_per_node: 16
          memory: 64Gi
          gpus_per_node: 0
          rdma: false
          processes_per_node: 1
          node_selector: {node-type: cpu}
          environment:
            image: coordinator-runtime:latest
            setup: [python -m pip install coordinator]
          command: [python coordinator.py]
          env: {SERVICE_PORT: "9000"}

        a100-worker:
          replicas: 2
          cpus_per_node: 96
          memory: 512Gi
          gpus_per_node: 8
          rdma: true
          processes_per_node: 8
          node_selector:
            nvidia.com/gpu.product: A100-SXM4-80GB
          # command omitted: inherit the template and CLI command

        h100-worker:
          replicas: 4
          cpus_per_node: 96
          memory: 1Ti
          gpus_per_node: 8
          rdma: true
          processes_per_node: 8
          node_selector:
            nvidia.com/gpu.product: H100-80GB-HBM3
          environment:
            image: h100-runtime:latest
            setup:
              - python -m pip install -e .
              - python -m pip install transformer-engine
          command: [python h100_train.py]
```

A mapping fits template inheritance: child templates override fields by Task
name without list-index coupling. V1 does not add reusable node profiles.

With:

```bash
aj run -t heterogeneous python train.py
```

`a100-worker` runs `python train.py`; `master` and `h100-worker` use their
explicit commands.

### Fields and inheritance

| Field | Rule |
| --- | --- |
| replicas and resources | required per Task |
| `node_selector` | empty by default |
| image | inherits top-level environment |
| setup | inherits; explicit Task list replaces |
| command | inherits the template plus CLI command; explicit Task replaces |
| env and container args | merge; Task wins |

Job-level namespace, queue, context, priority, labels, code, and storage remain
shared. Global target CPU, memory, GPU, and RDMA fields are rejected in this
mode to avoid ambiguous inheritance.

Typed data remains backend-local:

```text
Template._extra.volcano.tasks
  → VolcanoOpts.tasks: dict[str, VolcanoTaskOpts]
  → JobSpec.backend_spec
  → one PodSpec per Volcano Task
```

No Volcano field is added to `JobSpec`.

A backend-neutral run-shape hook derives totals and rejects CLI conflicts
before SKU resolution. It sets `JobSpec.nodes` to total replicas and uses
neutral scalar placeholders for the non-uniform per-node fields. The Volcano
renderer reads actual values only from typed Tasks.

### CLI shape

YAML is the only topology source. When Tasks exist:

- explicit `-n`, `-p`, and `--ppn` fail;
- `jobs[0].sku` must not use `{nodes}` or `{processes}`;
- `--amlt` is rejected;
- total nodes, GPU nodes, GPUs, and processes derive from Tasks.

The CLI command remains required in V1 and acts as the default Task command.
Homogeneous templates retain current CLI behavior.

### Manifest and rank

Each mapping entry becomes one `spec.tasks[]` entry. `minAvailable` is the sum
of replicas. Ordering is deterministic: `master`, then remaining names sorted
lexicographically.

`master` has rank base `0`. Every later base is the sum of preceding replicas:

```text
master       base 0
a100-worker  base 1
h100-worker  base 3
```

Each Pod receives:

| Variable | Meaning |
| --- | --- |
| `AJ_TASK_NAME` | Task name |
| `AJ_TASK_INDEX` | replica index within the Task |
| `AJ_TASK_REPLICAS` | replicas in this Task |
| `AJ_NODE_RANK` | unique node rank across Tasks |
| `AJ_NODES` | total replicas |
| `AJ_GPU_NODES` | replicas with GPUs |
| `AJ_TOTAL_GPUS` | sum of replicas × GPUs per node |
| `AJ_GPUS_PER_NODE` | current Task value |
| `AJ_PROCESSES_PER_NODE` | current Task value |

`AJ_PROCESSES` remains the historical alias for total GPUs. `WORLD_SIZE`
remains node-level, `NODE_RANK` defaults to `AJ_NODE_RANK`, and
`MASTER_ADDR` remains `<job>-master-0.<job>`.

aj does not create a process-level mixed-hardware DDP group. User code must
choose participating roles and remap ranks where needed.

## Invariants

- Exactly one `master` Task with one replica.
- Task names are valid DNS-1035 labels.
- Replicas and process counts are positive.
- CPU counts are positive, GPU counts are non-negative, and memory is a valid
  quantity.
- Mount paths do not overlap after inheritance.
- Raw security context, hostPath, arbitrary volume, and privileged fields are
  rejected.
- `shared/job/build.py` remains service-agnostic.
- Legacy manifests do not change when Tasks are absent.

## Failure handling

All schema, resource, CLI-conflict, selector, quantity, and mount validation
runs before upload or cluster mutation. Dry-run shows the expanded Task shape.
An unavailable node selector remains a normal schedulability failure and is
diagnosed through Job, Pod, and event views.

## Evolution

Deferred extensions include named profiles, Task storage, Task archives,
optional CLI command, custom affinity/tolerations, and other backends.
