#!/usr/bin/env bash
# install_azcopy.sh — idempotently install azcopy v10 into the pod's PATH.
#
# Loaded at submit time and injected into the Volcano pod's bash preamble
# by azure_jobs.backend.volcano.uploaders.blob:_build_pod_setup. Keep the
# script self-contained, dependency-free (curl OR wget), and re-entrant
# (no-op when azcopy is already on PATH).
#
# Placeholders (rendered by uploaders/_scripts.py::load_script):
#   {RETRIES}  — int, number of download attempts for the azcopy archive.

set -eo pipefail

if ! command -v tar >/dev/null 2>&1 ||
    ! command -v sha256sum >/dev/null 2>&1; then
    echo "[aj] installing tar/coreutils prerequisites"
    _aj_root=()
    if [ "$EUID" -ne 0 ]; then
        if ! command -v sudo >/dev/null 2>&1; then
            echo "[aj] tar/coreutils are required, but root/sudo is unavailable" >&2
            exit 1
        fi
        _aj_root=(sudo)
    fi
    if command -v apt-get >/dev/null 2>&1; then
        "${_aj_root[@]}" apt-get update &&
            "${_aj_root[@]}" apt-get install -y tar coreutils
    elif command -v apk >/dev/null 2>&1; then
        "${_aj_root[@]}" apk add --no-cache tar coreutils
    elif command -v dnf >/dev/null 2>&1; then
        "${_aj_root[@]}" dnf install -y tar coreutils
    elif command -v yum >/dev/null 2>&1; then
        "${_aj_root[@]}" yum install -y tar coreutils
    else
        echo "[aj] no supported package manager found for tar/coreutils" >&2
        exit 1
    fi
fi

if ! command -v tar >/dev/null 2>&1 ||
    ! command -v sha256sum >/dev/null 2>&1; then
    echo "[aj] tar/coreutils installation did not provide required binaries" >&2
    exit 1
fi

if command -v azcopy >/dev/null 2>&1; then
    echo "[aj] azcopy already installed: $(command -v azcopy)"
else

echo "[aj] azcopy not found — installing"
_aj_arch="$(uname -m)"
case "$_aj_arch" in
    x86_64|amd64)
        _aj_url="https://aka.ms/downloadazcopy-v10-linux"
        ;;
    aarch64|arm64)
        _aj_url="https://aka.ms/downloadazcopy-v10-linux-arm64"
        ;;
    *)
        echo "[aj] unsupported arch: $_aj_arch" >&2
        exit 1
        ;;
esac

_aj_az_tgz="$(mktemp /tmp/azcopy-XXXXXX.tgz)"
_aj_ok=0
for _aj_i in $(seq 1 {RETRIES}); do
    if command -v curl >/dev/null 2>&1; then
        if curl -fSL --retry 5 --retry-delay 3 -o "$_aj_az_tgz" "$_aj_url"; then
            _aj_ok=1
            break
        fi
    elif command -v wget >/dev/null 2>&1; then
        if wget -q -O "$_aj_az_tgz" "$_aj_url"; then
            _aj_ok=1
            break
        fi
    else
        echo "[aj] neither curl nor wget available — cannot bootstrap azcopy" >&2
        exit 1
    fi
    echo "[aj] azcopy download attempt $_aj_i failed; retrying in $((_aj_i * 3))s" >&2
    sleep "$((_aj_i * 3))"
done

if [ "$_aj_ok" != "1" ]; then
    echo "[aj] azcopy install failed: could not download archive" >&2
    exit 1
fi

_aj_az_extract="$(mktemp -d /tmp/azcopy-XXXXXX)"
if ! tar xzf "$_aj_az_tgz" -C "$_aj_az_extract"; then
    echo "[aj] azcopy install failed: tar extract" >&2
    exit 1
fi

_aj_az_bin="$(find "$_aj_az_extract" -maxdepth 3 -type f -name azcopy 2>/dev/null | head -n1)"
if [ -z "$_aj_az_bin" ]; then
    echo "[aj] azcopy install failed: binary not found in archive" >&2
    exit 1
fi

install -m 0755 "$_aj_az_bin" /usr/local/bin/azcopy 2>/dev/null \
    || install -m 0755 "$_aj_az_bin" "$HOME/.local/bin/azcopy" 2>/dev/null \
    || {
        mkdir -p /tmp/aj-bin
        install -m 0755 "$_aj_az_bin" /tmp/aj-bin/azcopy
        export PATH="/tmp/aj-bin:$PATH"
    }

rm -rf "$_aj_az_tgz" "$_aj_az_extract"

if ! command -v azcopy >/dev/null 2>&1; then
    echo "[aj] azcopy still not on PATH after install" >&2
    exit 1
fi

echo "[aj] azcopy installed:"
azcopy --version || true
fi
