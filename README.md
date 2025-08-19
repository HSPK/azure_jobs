# Azure Jobs

A simple interface to manage Azure Jobs. Features include:

1. Create jobs from templates with flexible command line overrides.
2. Support global and local configurations by using `.azurejobs` files. (Credentials may be stored here.)
3. Pretty print job details, export status, link to custom dashboards.
4. Set up alerts for job events using simple hooks.

# Get started

1. Install the Azure Jobs CLI tool.

```bash
pipx install azure_jobs
```

2. Setup your credentials by running:

```bash
aj init
```

3. Pull templates from private repositories:

```bash
aj pull <repository-url>
```

4. Run jobs using the CLI:

```bash
aj run -t <template> <command> [args...]
```

# Template

A template consists of a YAML file defines:
1. The job's environment (e.g., AML, Singularity)
2. GPU, CPU resources required
3. Storages used.
4. Pre-execution commands (Environment preparation commands)
5. Account information (e.g., Azure subscription ID)

By default, `azure_jobs` will store environments in the `~/.azurejobs/environments` directory. Storage credentials will be stored in the `~/.azurejobs/storage` directory. Account information will be stored in the `~/.azurejobs/account` directory.

Templates will be stored in the `~/.azurejobs/templates` directory. A reserved template name is `default`, it will be updated automatically with the latest configuration.

# Configuration Structure

```yaml
base: base_configuration or a list of configurations to merge
config:
    example_a: value_a
    example_b: value_b
```

# Configuration Merging

Configuration can be merged by the following rules:
1. Dict configuration will be merged recursively.
2. List configuration will be merged by appending elements from the source list to the target list.
3. Scalar values will be replaced by the target value.

Here is the code for merging configurations:

```python
def merge_confs(*data):
    # merge multiple dictionaries or lists recursively
    if all(isinstance(d, dict) for d in data):
        merged = {}
        for d in data:
            for key, value in d.items():
                if key in merged:
                    merged[key] = merge_confs(merged[key], value)
                else:
                    merged[key] = value
        return merged
    elif all(isinstance(d, list) for d in data):
        merged = []
        for d in zip(*data):
            merged.append(merge_confs(*d))
        return merged
    return data[-1]
```
