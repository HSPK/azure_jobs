#!/usr/bin/env bash
# blob_download.sh — fetch the code tarball from Azure Blob (SAS) and
# extract it into the pod-side code directory. Assumes azcopy is already
# on PATH (see install_azcopy.sh, which the uploader injects first).
#
# Placeholders (rendered by uploaders/_scripts.py::load_script):
#   {EXTRACT_DIR}  — absolute path inside the pod where code is unpacked.
#   {URL_WITH_SAS} — full https URL including '?sv=…&sig=…' SAS query.

set -eo pipefail

echo "[aj] preparing code from Azure Blob…"
_aj_code_dir='{EXTRACT_DIR}'
_aj_url_with_sas='{URL_WITH_SAS}'

mkdir -p "$_aj_code_dir"
_aj_tgz="$(mktemp /tmp/aj-code-XXXXXX.tgz)"

export AZCOPY_LOG_LOCATION=/tmp/aj-azcopy-logs
mkdir -p "$AZCOPY_LOG_LOCATION"

azcopy copy "$_aj_url_with_sas" "$_aj_tgz" --log-level=INFO

tar xzf "$_aj_tgz" -C "$_aj_code_dir"
rm -f "$_aj_tgz"

echo "[aj] code extracted to $_aj_code_dir"
