# Kubernetes setup and management

`aj k8s` manages local access to an existing Kubernetes/Volcano cluster and
provides narrow task operations. `aj k` is the short alias.

It does **not** create control-plane or worker nodes. Tool installation and
OIDC login are deliberately separate.

## Architecture boundary

The first release intentionally uses a hybrid model:

- `install` and `login` are client-local because they use sudo, interactive
  OIDC, and the user's kubeconfig;
- status and task-management commands also call the local `kubectl` for now;
- Volcano submission through `aj run` remains daemon-backed.

Future work will move status, queues, Jobs, pods, logs, events, and deletion
behind daemon APIs while keeping setup local. This is tracked in
[issue #15](https://github.com/HSPK/azure_jobs/issues/15).

See [Kubernetes management design](design/kubernetes.md) for the boundary,
safety invariants, and migration plan.

## Install client tools once

Preview without changing the host:

```bash
aj --json k install --dry-run
```

Apply after reviewing the plan:

```bash
aj k install
```

Install configures:

- Kubernetes client repository `v1.32`;
- `kubectl`, Krew, and `oidc-login`;

It may run `sudo apt-get`, add a `pkgs.k8s.io` source/key, and install
user-local Krew plugins. Existing conflicting Kubernetes apt sources stop
installation unless `--force-repo` is explicit.

Useful options:

```bash
aj k install --kubernetes-minor v1.33
aj k install --force-repo
aj k install --reinstall
```

`install` is interactive and rejects JSON mode except for `--dry-run`. Agents
must present the plan and obtain confirmation before using `--yes`. Repeating
install is a no-op when both kubectl and oidc-login already exist;
`--reinstall` is required to rerun apt/Krew.

## Log in to msr02

Daily login does not run apt or Krew:

```bash
aj --json k login --dry-run
aj k login
```

The built-in `lambda-msr02` profile merges context `oidc@msr02` with
device-code authentication. Login:

1. resolves the existing `oidc-login` plugin;
2. atomically merges and backs up kubeconfig only when content changes;
3. preserves an already discovered namespace such as `bonete04`;
4. asks for confirmation before clearing stale OIDC tokens, because clearing
   the cache may sign out the current Kubernetes user;
5. displays the device-code prompt directly, waits up to ten minutes, verifies
   `kubectl auth whoami`, and derives the matching `bonete*` group;
6. restores the previous kubeconfig if authentication fails.

Useful options:

```bash
aj k login --cached               # keep cached tokens; no sign-out confirmation
aj k login --no-verify            # merge only
aj k login --reset-namespace      # restore profile default before detection
aj k login --server https://... --issuer-url https://... --client-id ...
aj k login --kubeconfig /path/to/config
```

`aj k setup` remains a deprecated alias for `aj k login`; it no longer
installs tools.

If login appears idle, check the terminal for the device-code URL and code.
The prompt is written on stderr while aj keeps stdout for the final structured
identity response.

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

If tool installation fails, preserve the complete install output. If
authentication succeeds but namespace discovery does not, inspect
`kubectl auth whoami -o json` and pass the namespace explicitly.
