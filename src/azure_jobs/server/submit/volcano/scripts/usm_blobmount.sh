#!/usr/bin/env bash

_aj_install_usm_blobmount() {
    export PATH="$HOME/.local/bin:$PATH"
    if ! command -v python3 >/dev/null 2>&1; then
        if command -v apt-get >/dev/null 2>&1; then
            apt-get update -qq &&
                DEBIAN_FRONTEND=noninteractive apt-get install -yq \
                    python3 python3-pip ca-certificates curl
        elif command -v dnf >/dev/null 2>&1; then
            dnf install -y python3 python3-pip ca-certificates curl
        elif command -v yum >/dev/null 2>&1; then
            yum install -y python3 python3-pip ca-certificates curl
        else
            echo "[aj-blob] python3 is required to install usm" >&2
            return 1
        fi
    fi
    if ! command -v usm >/dev/null 2>&1; then
        python3 -m pip install --user --disable-pip-version-check \
            "usmo>=0.12.1" || return 1
    fi
    export PATH="$HOME/.local/bin:$PATH"
    usm update blobmount >/dev/null || return 1
    if ! usm blobmount mount --help 2>&1 | grep -q "fic"; then
        echo "[aj-blob] usm blobmount does not support --auth fic; require command version >=1.1.0" >&2
        return 1
    fi
    usm blobmount install >/dev/null || return 1
}

_aj_usm_blobmount() {
    _aj_id="$1"
    _aj_mount_dir="$2"
    _aj_account="$3"
    _aj_container="$4"
    _aj_auth="$5"
    _aj_credential_file="${6:-}"
    set -- usm blobmount mount \
        "$_aj_mount_dir" "$_aj_account" "$_aj_container" \
        --name "$_aj_id" --auth "$_aj_auth" --force
    if [ "$_aj_auth" = "file" ]; then
        set -- "$@" --sas-file "$_aj_credential_file"
    fi
    "$@"
}
