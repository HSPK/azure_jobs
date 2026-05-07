#!/usr/bin/env bash
# Smoke test: SSH key + git over SSH inside an aj-submitted job.
#
# Verifies:
#   1. ~/.ssh/ was copied into the container by aj's code-upload step.
#   2. ssh-agent / known_hosts are wired up enough that `git@github.com`
#      accepts the key (no host-key prompt, no permission denied).
#   3. `git ls-remote` over SSH succeeds end-to-end.
#
# Submit with:
#   aj run -t <template> -n 1 -p 1 bash scripts/test_ssh_git.sh

set -uo pipefail

REPO="${SSH_TEST_REPO:-git@github.com:HSPK/azure_jobs.git}"

fail() { echo "[FAIL] $*" >&2; exit 1; }
ok()   { echo "[ OK ] $*"; }

echo "=== whoami ==="
id
echo

echo "=== ~/.ssh contents ==="
if [[ -d "$HOME/.ssh" ]]; then
    ls -la "$HOME/.ssh"
else
    fail "~/.ssh does not exist — code upload did not include the .ssh tree"
fi
echo

key_found=0
for k in id_ed25519 id_ecdsa id_rsa; do
    if [[ -f "$HOME/.ssh/$k" ]]; then
        ok "found private key: ~/.ssh/$k"
        key_found=1
    fi
done
[[ $key_found -eq 1 ]] || fail "no private key in ~/.ssh"

# Make sure permissions are tight enough for OpenSSH to use the keys.
chmod 700 "$HOME/.ssh" 2>/dev/null || true
chmod 600 "$HOME/.ssh"/id_* 2>/dev/null || true
[[ -f "$HOME/.ssh/known_hosts" ]] && chmod 644 "$HOME/.ssh/known_hosts" || true

echo "=== ssh -T git@github.com (key probe) ==="
# GitHub returns exit 1 with a "Hi <user>" greeting when the key works;
# permission errors come back on stderr.
ssh_out=$(ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes -T git@github.com 2>&1)
ssh_rc=$?
echo "$ssh_out"
if echo "$ssh_out" | grep -qiE "successfully authenticated|Hi .*!"; then
    ok "github accepted the SSH key (rc=$ssh_rc)"
else
    fail "github did NOT accept the SSH key (rc=$ssh_rc)"
fi
echo

# Some images export an empty GIT_SSH / GIT_SSH_COMMAND, which makes git
# try to exec "" and fail with "cannot run : No such file or directory".
# Force both to a real ssh invocation (env wins over -c core.sshCommand).
unset GIT_SSH
export GIT_SSH_COMMAND="ssh -o StrictHostKeyChecking=accept-new -o BatchMode=yes"

echo "=== git ls-remote $REPO HEAD ==="
echo "GIT_SSH_COMMAND=$GIT_SSH_COMMAND"
if git ls-remote "$REPO" HEAD; then
    ok "git over SSH works against $REPO"
else
    fail "git ls-remote failed against $REPO"
fi

echo
echo "=== ALL SSH+GIT CHECKS PASSED ==="
