#!/usr/bin/env bash
# blob_download.sh — fetch the code tarball from Azure Blob (SAS) and
# extract it into the pod-side code directory. Assumes azcopy is already
# on PATH (see install_azcopy.sh, which the uploader injects first).
#
# Placeholders (rendered by uploaders/_scripts.py::load_script):
#   {EXTRACT_DIR}  — absolute path inside the pod where code is unpacked.
#   {URL_WITH_SAS} — full https URL including '?sv=…&sig=…' SAS query.
#   {EXPECTED_SHA256} — daemon-computed SHA-256 for integrity verification.

set -eo pipefail

echo "[aj] preparing code from Azure Blob…"
_aj_code_dir='{EXTRACT_DIR}'
_aj_url_with_sas='{URL_WITH_SAS}'
_aj_expected_sha256='{EXPECTED_SHA256}'

mkdir -p "$_aj_code_dir"
_aj_tgz="$(mktemp /tmp/aj-code-XXXXXX.tgz)"

export AZCOPY_LOG_LOCATION=/tmp/aj-azcopy-logs
mkdir -p "$AZCOPY_LOG_LOCATION"

azcopy copy "$_aj_url_with_sas" "$_aj_tgz" --log-level=INFO

read -r _aj_actual_sha256 _ < <(sha256sum "$_aj_tgz")
if [ "$_aj_actual_sha256" != "$_aj_expected_sha256" ]; then
    echo "[aj] code archive SHA-256 mismatch: expected $_aj_expected_sha256, got $_aj_actual_sha256" >&2
    rm -f "$_aj_tgz"
    exit 1
fi

tar xzf "$_aj_tgz" -C "$_aj_code_dir"
rm -f "$_aj_tgz"

echo "[aj] code extracted to $_aj_code_dir"
