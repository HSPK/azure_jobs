# Kubernetes setup and management

`aj k8s` manages local access to an existing Kubernetes/Volcano cluster and
provides narrow task operations. `aj k` is the short alias.

It does **not** create control-plane or worker nodes. `setup` installs client
tools and merges an OIDC context into kubeconfig.

## Architecture boundary

The first release intentionally uses a hybrid model:

- `setup` is client-local because it uses sudo, interactive OIDC, and the
  user's kubeconfig;
- status and task-management commands also call the local `kubectl` for now;
- Volcano submission through `aj run` remains daemon-backed.

Future work will move status, queues, Jobs, pods, logs, events, and deletion
behind daemon APIs while keeping setup local. This is tracked in
[issue #15](https://github.com/HSPK/azure_jobs/issues/15).

## Set up the msr02 profile

Preview without changing the host:

```bash
aj --json k setup --dry-run
```

Apply after reviewing the plan:

```bash
aj k setup
```

The built-in `lambda-msr02` profile configures:

- Kubernetes client repository `v1.32`;
- `kubectl`, Krew, and `oidc-login`;
- context `oidc@msr02`;
- device-code OIDC authentication;
- initial namespace `bonete01`, replaced by the matching `bonete*` group after
  successful authentication.

Setup may run `sudo apt-get`, add a `pkgs.k8s.io` source/key, install user-local
Krew plugins, back up kubeconfig, and atomically merge the profile. It never
blindly overwrites unrelated contexts. Existing conflicting Kubernetes apt
sources stop setup unless `--force-repo` is explicit.

Useful options:

```bash
aj k setup --skip-tools            # merge kubeconfig only
aj k setup --no-verify             # skip OIDC whoami
aj k setup --kubernetes-minor v1.33
aj k setup --server https://... --issuer-url https://... --client-id ...
aj k setup --kubeconfig /path/to/config
```

`setup` is interactive and rejects JSON mode except for `--dry-run`. Agents
must present the plan and obtain confirmation before using `--yes`.

## Status

```bash
aj k status
aj --json k status
aj k status --check-cluster
```

Without `--check-cluster`, status only inspects local tools and kubeconfig.
Cluster checking calls `/readyz` and may start OIDC device authentication.

Every management command accepts:

```text
--kubeconfig PATH
--context CONTEXT
-n, --namespace NAMESPACE
```

Omitted namespace resolves from the selected/current context.

## Volcano resources

```bash
aj k queues
aj k jobs
aj k jobs <generated-job-name>
aj k pods
aj k pods --job <generated-job-name>
```

These commands use narrow custom columns and do not retrieve full Job or Pod
manifests. Use the generated `azure_name` returned by Volcano submission.

## Logs and events

```bash
aj k logs <pod> -c master --tail 200
aj k logs <pod> -c master --previous
aj k events <pod>
```

Logs are bounded and credential-redacted by default, including signed URL
queries and token-like assignments. `--raw` disables redaction and should only
be used for private local diagnosis.

Events are filtered to one pod and their messages are redacted.

## Delete

```bash
aj k delete <generated-job-name>
```

Deletion targets one exact Volcano Job and requires confirmation. For
noninteractive use, review context, namespace, and name first, then pass
`--yes`. Never delete by broad selector. If the Job references aj's mounted
Blob credential Secret, the command removes that exact Secret after deleting
the Job.

## Nested container runtime

Podman/Docker-in-Pod support is configured in the Volcano template using
`capabilities`, `scratch_mount_path`, and `scratch_size`. See
[Templates](templates.md#nested-container-runtime).

## Troubleshooting

```bash
kubectl --kubeconfig ~/.kube/config config get-contexts
aj k status --check-cluster
kubectl oidc-login clean
```

If tool installation fails, preserve the complete setup output. If
authentication succeeds but namespace discovery does not, inspect
`kubectl auth whoami -o json` and pass the namespace explicitly.
