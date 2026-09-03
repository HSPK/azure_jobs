#!/usr/bin/env bash

_aj_blob_sidecar_fail() {
    _aj_status=$?
    printf 'blob sidecar failed with exit %s\n' "$_aj_status" \
        > /mnt/aj-blob-ready/error
    exit "$_aj_status"
}

_aj_install_ganesha() {
    command -v ganesha.nfsd >/dev/null 2>&1 && return 0
    if command -v apt-get >/dev/null 2>&1; then
        apt-get update -qq &&
            DEBIAN_FRONTEND=noninteractive apt-get install -yq \
                nfs-ganesha nfs-ganesha-vfs rpcbind
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y nfs-ganesha nfs-ganesha-vfs
    elif command -v yum >/dev/null 2>&1; then
        yum install -y nfs-ganesha nfs-ganesha-vfs
    else
        echo "[aj-blob] nfs-ganesha is required in the sidecar" >&2
        return 1
    fi
}

_aj_start_ganesha() {
    _aj_exports="$1"
    _aj_conf=/etc/ganesha/ganesha.conf
    mkdir -p /etc/ganesha /var/run/ganesha
    ln -sf /proc/mounts /etc/mtab 2>/dev/null || true
    cat >"$_aj_conf" <<'AJ_NFS_BASE'
NFS_Core_Param {
    Protocols = 4;
    NFS_Port = 2049;
    Rquota_Port = 875;
}
LOG {
    Default_Log_Level = EVENT;
    Components {
        FSAL = WARN;
        NFS_V4 = WARN;
    }
}
AJ_NFS_BASE
    _aj_export_id=1
    _aj_old_ifs="$IFS"
    IFS=';'
    for _aj_export in $_aj_exports; do
        [ -n "$_aj_export" ] || continue
        _aj_name="${_aj_export%%:*}"
        _aj_path="${_aj_export#*:}"
        cat >>"$_aj_conf" <<AJ_NFS_EXPORT
EXPORT {
    Export_Id = $_aj_export_id;
    Path = $_aj_path;
    Pseudo = /$_aj_name;
    Access_Type = RW;
    Squash = No_Root_Squash;
    Protocols = 4;
    Transports = TCP;
    FSAL { Name = VFS; }
    CLIENT {
        Clients = 127.0.0.1;
        Access_Type = RW;
    }
}
AJ_NFS_EXPORT
        _aj_export_id=$((_aj_export_id + 1))
    done
    IFS="$_aj_old_ifs"
    rpcbind -w || true
    ganesha.nfsd -F -L /dev/stdout -f "$_aj_conf" \
        -p /var/run/ganesha/ganesha.pid &
    _aj_ganesha_pid=$!
    _aj_waited=0
    until rpcinfo -p 127.0.0.1 2>/dev/null | grep -qw nfs; do
        kill -0 "$_aj_ganesha_pid" 2>/dev/null || {
            echo "[aj-blob] NFS-Ganesha exited during startup" >&2
            return 1
        }
        [ "$_aj_waited" -lt 30 ] || {
            echo "[aj-blob] NFS-Ganesha was not ready after 30s" >&2
            return 1
        }
        sleep 1
        _aj_waited=$((_aj_waited + 1))
    done
    : > /mnt/aj-blob-ready/ready
    trap 'kill -TERM "$_aj_ganesha_pid" 2>/dev/null || true; wait "$_aj_ganesha_pid"' TERM INT
    wait "$_aj_ganesha_pid"
}

trap _aj_blob_sidecar_fail ERR INT TERM
