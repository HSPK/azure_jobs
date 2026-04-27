# aj vs amlt Feature Comparison

This document compares **aj** (Azure Jobs) with **amlt** (Amulet), the two CLI tools for submitting and managing Azure ML jobs.

## Philosophy

| | aj | amlt |
|---|---|---|
| **Approach** | Minimal, pure REST API | Full-featured, Azure ML SDK |
| **Dependencies** | click + pyyaml only | Heavy SDK + many extras |
| **Startup time** | ~30ms (lazy imports) | ~2s |
| **Config style** | Template-based inheritance | Single YAML with imports |

## Command Coverage

### aj has, amlt doesn't

| Command | Description |
|---------|-------------|
| `aj job stats` | GPU Hours stats by experiment/compute/user |
| `aj quota list` | View Singularity VC and AML cluster quotas |
| `aj sku list` | Browse available SKU instance types |
| `aj dash` | Interactive TUI dashboard with keyboard nav |
| `aj image list` | Browse Singularity curated base images |
| `aj ds list/show` | Browse workspace datastores |
| `aj env list/show` | Browse registered environments |

### amlt has, aj doesn't

#### High Priority Gaps

| Feature | amlt command | Notes |
|---------|-------------|-------|
| SSH into running job | `amlt ssh` | Essential for debugging |
| Download results | `amlt results download` | Most commonly needed |
| List/view results | `amlt results list/view` | Browse job outputs |
| Follow logs in real-time | `amlt logs -f` | aj has `job logs` but no streaming |
| Rerun experiment | `amlt rerun` | Avoids re-configuring |

#### Medium Priority Gaps

| Feature | amlt command | Notes |
|---------|-------------|-------|
| Conda/pip declaration | YAML `conda_file` / `pip` | aj uses setup commands instead |
| Dockerfile build | `docker.build` in config | aj uses pre-built images only |
| YAML variable substitution | `$VAR` + `env_defaults` | No equivalent in aj |
| Hyperdrive search | `--search` flag | Grid/random/bayesian |
| Code management | `amlt code view/clone/list` | Browse uploaded code snapshots |
| Storage operations | `amlt storage upload/download/rm` | Direct blob management |
| Metrics extraction | `amlt metrics` | Tabulate job metrics with expressions |
| Pause/Resume jobs | `amlt pause/resume` | Job lifecycle control |
| Interactive debug | `amlt debug` | tmux-based debug session |

#### Low Priority Gaps

| Feature | amlt command | Notes |
|---------|-------------|-------|
| Shell completion | `amlt completion` | bash/zsh/fish |
| Multi-job parallel submit | Thread pool | aj submits one job at a time |
| Job tagging | `amlt tag` | Organize jobs with labels |
| Move jobs | `amlt move` | Between experiments |
| Node power management | `amlt power` | Restart/drain nodes |
| Code size validation | Built-in limits | Warn on large uploads |

## Run Command Comparison

### Configuration

| Feature | aj | amlt |
|---------|:--:|:----:|
| Template inheritance | ✅ `base` chain | ✅ `imports` directive |
| Environment variables | ✅ export in commands | ✅ `env_defaults` + `$VAR` |
| Docker image | ✅ | ✅ |
| Conda environment | ❌ | ✅ |
| Pip requirements | ❌ | ✅ |
| Dockerfile build | ❌ | ✅ |
| Multi-job config | ✅ jobs list | ✅ jobs list + search |
| SKU override | ✅ template placeholders | ✅ `--sku` flag |
| Dry run | ✅ `-d` | ❌ (`--dump` only) |
| Local execution | ✅ `-L` | ✅ local target |

### Code Upload

| Feature | aj | amlt |
|---------|:--:|:----:|
| Individual file upload | ✅ | ❌ (zip) |
| Content-addressed dedup | ✅ SHA hash | ✅ checksum |
| HEAD-before-upload skip | ✅ | ❌ |
| Git info tracking | ✅ | ✅ |
| Custom ignore patterns | ✅ `code.ignore` | ✅ `.amltignore` |
| SSH key injection | ✅ auto ~/.ssh | ❌ |

### Distributed Training

| Feature | aj | amlt |
|---------|:--:|:----:|
| PyTorch DDP env vars | ✅ auto | ✅ auto |
| MPI rank detection | ✅ OMPI vars | ✅ native |
| NCCL tuning | ✅ | ✅ |
| Rank-0 setup barrier | ✅ 600s timeout | ✅ |
| Master address resolution | ✅ auto | ✅ auto |

## Job Management Comparison

| Feature | aj | amlt |
|---------|:--:|:----:|
| List jobs | ✅ `job list` | ✅ `list` |
| Show job details | ✅ `job show` | ✅ `show` |
| Cancel job | ✅ `job cancel` | ✅ `cancel` |
| View logs | ✅ `job logs` | ✅ `logs` |
| Follow logs | ❌ | ✅ `-f` |
| Job statistics | ✅ `job stats` | ❌ |
| Download results | ❌ | ✅ `results` |
| SSH into job | ❌ | ✅ `ssh` |
| Rerun | ❌ | ✅ `rerun` |
| Pause/Resume | ❌ | ✅ |
| Move between experiments | ❌ | ✅ `move` |

## When to Use Which

**Choose aj if you:**
- Want fast startup and minimal dependencies
- Prefer template-based workflows with inheritance
- Need quota monitoring, SKU browsing, or GPU Hours stats
- Want an interactive TUI dashboard
- Run primarily on Singularity clusters

**Choose amlt if you:**
- Need SSH into running jobs for debugging
- Want to download results directly from CLI
- Require conda/pip/Dockerfile environment builds
- Use hyperdrive for hyperparameter search
- Need comprehensive job lifecycle (pause/resume/rerun)
- Work with storage management and metrics extraction
