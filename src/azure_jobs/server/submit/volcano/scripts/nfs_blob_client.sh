#!/usr/bin/env bash

_aj_install_nfs_client() {
    command -v mount.nfs4 >/dev/null 2>&1 && return 0
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq &&
            DEBIAN_FRONTEND=noninteractive apt-get install -yq nfs-common
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y nfs-utils
    elif command -v yum >/dev/null 2>&1; then
        yum install -y nfs-utils
    else
        echo "[aj-blob] an NFS client is required for sandboxed Blob mounts" >&2
        return 1
    fi
}

_aj_wait_blob_sidecar() {
    _aj_waited=0
    while [ ! -f /mnt/aj-blob-ready/ready ]; do
        if [ -f /mnt/aj-blob-ready/error ]; then
            cat /mnt/aj-blob-ready/error >&2
            return 1
        fi
        [ "$_aj_waited" -lt 600 ] || {
            echo "[aj-blob] timed out waiting for Blob NFS sidecar" >&2
            return 1
        }
        sleep 2
        _aj_waited=$((_aj_waited + 2))
    done
}

_aj_mount_blob_nfs() {
    _aj_name="$1"
    _aj_dir="$2"
    mkdir -p "$_aj_dir"
    if command -v mountpoint >/dev/null 2>&1 &&
        mountpoint -q "$_aj_dir"; then
        return 0
    fi
    _aj_attempt=1
    while [ "$_aj_attempt" -le 10 ]; do
        mount -t nfs4 \
            -o vers=4.0,nolock,soft,noatime,rsize=1048576,wsize=1048576 \
            "127.0.0.1:/$_aj_name" "$_aj_dir" && return 0
        sleep 3
        _aj_attempt=$((_aj_attempt + 1))
    done
    echo "[aj-blob] failed to mount NFS export $_aj_name at $_aj_dir" >&2
    return 1
}
