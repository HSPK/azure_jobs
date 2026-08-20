# Public template repository

Use this before creating or publishing a shared Azure Jobs template repository.
Public publication is safety-gated.

## Repository shape

The repository root mirrors shareable `AJ_HOME`; do not add an outer
`.azure_jobs/` directory:

```text
README.md
.gitignore
template/
  aml.yaml
  sing-auto.yaml
  volcano.yaml
account/
environment/
storage/
scripts/
```

Only create needed directories. `aj template pull` copies them into the local
`.azure_jobs/` tree.

## Public-content rules

Use placeholders such as:

```text
<AML_COMPUTE> <AML_WORKSPACE> <VC_NAME> <KUBECTL_CONTEXT>
<KUBERNETES_NAMESPACE> <VOLCANO_QUEUE> <STORAGE_ACCOUNT>
<BLOB_CONTAINER> <PVC_NAME> <CONTAINER_IMAGE>
```

Never publish:

- secrets, tokens, SAS values, keys, or credentialed URLs;
- real identity client IDs, UAI credentials, or tenant-specific ARM IDs;
- registry credentials, pull secrets, private images, or internal hostnames;
- queue/watch journals, records, submissions, logs, daemon state, or config;
- `.ssh/`, private keys, certificates, copied home material, or absolute paths.

Do not use symlinks. `aj` rejects symlink roots/entries so sync cannot copy
content outside the tree.

## `.gitignore`

```gitignore
aj_config.json
record.jsonl
submission/
logs/
daemon/
.ssh/
**/.ssh/
.env
*.pem
*.key
*.pfx
*.p12
```

Keep intended templates, components, and `scripts/` tracked. Ignore rules are
defense in depth; inspect staged content.

## README expectations

Document:

- purpose/backend of each leaf template;
- prerequisites (`aj`, `az`, and `kubectl` for Volcano);
- placeholders users must replace;
- Sing auto-selection behavior;
- required namespace, queue, PVC, Blob, and image capabilities;
- validation/dry-run commands and credential-publication warning;
- that Volcano is managed with `kubectl`, not `aj job`.

## Create and publish

Build locally:

```bash
mkdir azure-jobs-templates
cd azure-jobs-templates
git init -b main
mkdir -p template account environment storage scripts
```

Add README, ignore rules, templates, and components. Validate the same layout
under a test project's `.azure_jobs/`:

```bash
aj --json template validate
```

Stage exactly the intended publication snapshot first:

```bash
git add README.md .gitignore template
for path in account environment storage scripts; do
  [ ! -e "$path" ] || git add "$path"
done
SUSPECTS="$(
  git grep --cached -l -I -i \
    -E 'BEGIN .*PRIVATE KEY|sharedaccesssignature[[:space:]]*=|(^|[?&])sig=|(access[_-]?token|api[_-]?key|client[_-]?secret|account[_-]?key|password)[[:space:]]*[:=]|authorization[[:space:]]*:[[:space:]]*bearer' \
    || true
)"
if [ -n "$SUSPECTS" ]; then
  printf 'Potential secret files; stop and inspect privately:\n%s\n' "$SUSPECTS"
  exit 1
fi
git status --short
git diff --cached --check
git diff --cached --stat
gh auth status
```

The scan prints filenames only. If it finds anything, stop, inspect privately,
unstage, and remediate it. Manually inspect the exact staged YAML/scripts;
never echo suspected values or a raw staged diff into diagnostics.
Obtain confirmation immediately before public creation unless already
explicitly requested. Then commit and publish that reviewed snapshot:

```bash
git commit -m "Add Azure Jobs templates"
gh repo create OWNER/REPOSITORY --public --source=. --remote=origin --push
```

Never put credentials in a remote URL.

## Pull into a project

```bash
aj --json template pull OWNER/REPOSITORY
aj --json template list
aj --json template validate
```

GitHub shorthand expands to SSH. Stored/displayed HTTP remotes omit
userinfo/query credentials.

Use force only after confirmation and reviewing local changes:

```bash
aj --json template diff
aj template pull --force OWNER/REPOSITORY
```

Force pull may remove stale shareable files in synchronized directories.

## Edit, diff, and push

Edit `.azure_jobs/template/` and reusable component directories, then:

```bash
aj --json template validate
aj --json template diff
```

Review for credentials/local-only data. Obtain confirmation before:

```bash
aj --json template push -m "Describe template change"
```

Push clones the remote, replaces its shareable tree with the local shareable
tree, commits, and pushes. It refuses local symlinks.

## Local-only exclusions

Pull/diff/push exclude:

```text
aj_config.json
record.jsonl
submission/
logs/
daemon/
.git/
```

The daemon directory is especially sensitive because queue journals may
contain complete environment values. Never copy it manually. Treat generated
submissions/logs as local evidence, not templates.
