#!/usr/bin/env bash
# mount_blobfuse.sh — install blobfuse2 and mount Azure Blob containers.
#
# Injected into the Volcano pod's container script by
# azure_jobs.backend.volcano.storage. Images range from a bare distro to a
# vendor CUDA image, so nothing is assumed beyond a POSIX shell: the download
# helper tries every common client, and the installer covers dpkg, rpm and
# direct extraction when no package manager exists.
#
# Credentials are read from files, not environment variables, because a
# Kubernetes Secret exposed through the environment is fixed when the container
# starts. A user-delegation SAS expires within seven days, so a long run has to
# pick up a replacement without restarting: the mount loop re-reads the file and
# rewrites the blobfuse2 config, which blobfuse2 reloads in place.
#
# Placeholders (rendered by storage.py):
#   {REFRESH_SECONDS} — seconds between credential re-reads; 0 disables.
#   {BLOBFUSE_VERSION} — blobfuse2 release to install.

_aj_log() { echo "[aj-blob] $*"; }
_aj_warn() { echo "[aj-blob] $*" >&2; }

# Download to $2 from $1 using whichever client the image happens to ship.
_aj_fetch() {
    _aj_url="$1"
    _aj_dest="$2"
    if command -v curl >/dev/null 2>&1 &&
        curl -fsSL --retry 3 --retry-delay 2 -o "$_aj_dest" "$_aj_url"; then
        return 0
    fi
    if command -v wget >/dev/null 2>&1 &&
        wget -q -O "$_aj_dest" "$_aj_url"; then
        return 0
    fi
    for _aj_py in python3 python; do
        if command -v "$_aj_py" >/dev/null 2>&1 && "$_aj_py" - "$_aj_url" "$_aj_dest" <<'AJ_PY'
import shutil, sys, urllib.request
with urllib.request.urlopen(sys.argv[1], timeout=120) as response:
    with open(sys.argv[2], "wb") as handle:
        shutil.copyfileobj(response, handle)
AJ_PY
        then
            return 0
        fi
    done
    if command -v perl >/dev/null 2>&1 &&
        perl -MLWP::Simple -e 'exit(getstore($ARGV[0], $ARGV[1]) == 200 ? 0 : 1)' \
            "$_aj_url" "$_aj_dest" 2>/dev/null; then
        return 0
    fi
    if command -v busybox >/dev/null 2>&1 &&
        busybox wget -q -O "$_aj_dest" "$_aj_url"; then
        return 0
    fi
    _aj_warn "no usable download tool (curl, wget, python, perl or busybox)"
    return 1
}

# Echo the release asset matching this distribution, or nothing when unknown.
_aj_blobfuse_asset() {
    _aj_arch="$(uname -m)"
    case "$_aj_arch" in
        x86_64 | amd64) _aj_arch=x86_64 ;;
        aarch64 | arm64) _aj_arch=arm64 ;;
        *) return 1 ;;
    esac

    _aj_id=""
    _aj_ver=""
    _aj_like=""
    if [ -r /etc/os-release ]; then
        # shellcheck disable=SC1091
        . /etc/os-release
        _aj_id="${ID:-}"
        _aj_ver="${VERSION_ID:-}"
        _aj_like="${ID_LIKE:-}"
    fi
    _aj_major="${_aj_ver%%.*}"

    case "$_aj_id" in
        ubuntu)
            case "$_aj_ver" in
                18.04) echo "blobfuse2-{BLOBFUSE_VERSION}-Ubuntu-18.04.${_aj_arch}.deb" ;;
                20.04) echo "blobfuse2-{BLOBFUSE_VERSION}-Ubuntu-20.04.${_aj_arch}.deb" ;;
                # 24.04 and newer ship libfuse3 too, so the 22.04 build applies.
                *) echo "blobfuse2-{BLOBFUSE_VERSION}-Ubuntu-22.04.${_aj_arch}.deb" ;;
            esac
            ;;
        debian)
            case "$_aj_major" in
                9 | 10 | 11 | 12) echo "blobfuse2-{BLOBFUSE_VERSION}-Debian-${_aj_major}.0.${_aj_arch}.deb" ;;
                *) echo "blobfuse2-{BLOBFUSE_VERSION}-Debian-13.0.${_aj_arch}.deb" ;;
            esac
            ;;
        rhel | centos | rocky | almalinux | ol | oracle | fedora | amzn)
            case "$_aj_major" in
                7) echo "blobfuse2-{BLOBFUSE_VERSION}-RHEL-7.8.${_aj_arch}.rpm" ;;
                8) echo "blobfuse2-{BLOBFUSE_VERSION}-RHEL-8.6.${_aj_arch}.rpm" ;;
                *) echo "blobfuse2-{BLOBFUSE_VERSION}-RHEL-9.0.${_aj_arch}.rpm" ;;
            esac
            ;;
        sles | sled | opensuse* | suse)
            echo "blobfuse2-{BLOBFUSE_VERSION}-SUSE-15Gen2.${_aj_arch}.rpm"
            ;;
        *)
            case "$_aj_like" in
                *debian* | *ubuntu*) echo "blobfuse2-{BLOBFUSE_VERSION}-Debian-12.0.${_aj_arch}.deb" ;;
                *rhel* | *fedora* | *centos*) echo "blobfuse2-{BLOBFUSE_VERSION}-RHEL-9.0.${_aj_arch}.rpm" ;;
                *suse*) echo "blobfuse2-{BLOBFUSE_VERSION}-SUSE-15Gen2.${_aj_arch}.rpm" ;;
                *) return 1 ;;
            esac
            ;;
    esac
}

# Install what the rest of this script needs. Minimal images commonly ship
# neither a download client nor libfuse3: base Debian and Ubuntu images have no
# curl, wget or python, and their perl lacks LWP. The package manager is the one
# component that can reliably fetch over the network, so use it to bootstrap a
# client as well as the shared library blobfuse2 links against.
_aj_install_prereqs() {
    if command -v apt-get >/dev/null 2>&1; then
        DEBIAN_FRONTEND=noninteractive apt-get -qq update >/dev/null 2>&1 || true
        DEBIAN_FRONTEND=noninteractive apt-get -qq install -y --no-install-recommends \
            ca-certificates curl fuse3 libfuse3-3 >/dev/null 2>&1 ||
            DEBIAN_FRONTEND=noninteractive apt-get -qq install -y --no-install-recommends \
                ca-certificates wget fuse libfuse2 >/dev/null 2>&1 || true
    elif command -v dnf >/dev/null 2>&1; then
        dnf install -y -q ca-certificates curl fuse3 fuse3-libs >/dev/null 2>&1 || true
    elif command -v yum >/dev/null 2>&1; then
        yum install -y -q ca-certificates curl fuse3 fuse3-libs >/dev/null 2>&1 || true
    elif command -v zypper >/dev/null 2>&1; then
        zypper --non-interactive --quiet install ca-certificates curl fuse3 libfuse3-3 \
            >/dev/null 2>&1 || true
    elif command -v apk >/dev/null 2>&1; then
        apk add --no-cache ca-certificates curl fuse3 >/dev/null 2>&1 || true
    fi
}

# Last resort when no package manager can install the archive: unpack the
# payload by hand. Only the blobfuse2 binary is needed, and the shared library
# it links against was handled above.
_aj_extract_package() {
    _aj_pkg="$1"
    _aj_dir="$(mktemp -d)"
    case "$_aj_pkg" in
        *.deb)
            # ar may be absent, or present but unable to read the archive, so
            # treat both as a reason to try the next extractor rather than fail.
            _aj_ok=0
            if command -v ar >/dev/null 2>&1 &&
                (cd "$_aj_dir" && ar x "$_aj_pkg") >/dev/null 2>&1; then
                _aj_ok=1
            fi
            if [ "$_aj_ok" -eq 0 ]; then
                for _aj_py in python3 python; do
                    command -v "$_aj_py" >/dev/null 2>&1 || continue
                    if "$_aj_py" - "$_aj_pkg" "$_aj_dir" <<'AJ_PY'
import os, sys

# A .deb is an ar archive: a magic line, then 60-byte headers before each member.
archive, out = sys.argv[1], sys.argv[2]
with open(archive, "rb") as handle:
    if handle.read(8) != b"!<arch>\n":
        sys.exit(1)
    while True:
        header = handle.read(60)
        if len(header) < 60:
            break
        name = header[:16].decode().strip().rstrip("/")
        size = int(header[48:58].decode().strip())
        payload = handle.read(size)
        with open(os.path.join(out, name), "wb") as member:
            member.write(payload)
        if size % 2:
            handle.read(1)
AJ_PY
                    then
                        _aj_ok=1
                        break
                    fi
                done
            fi
            [ "$_aj_ok" -eq 1 ] || return 1
            _aj_data="$(find "$_aj_dir" -maxdepth 1 -name 'data.tar*' -print -quit)"
            [ -n "$_aj_data" ] || return 1
            tar xf "$_aj_data" -C "$_aj_dir" 2>/dev/null || return 1
            ;;
        *.rpm)
            command -v rpm2cpio >/dev/null 2>&1 || return 1
            command -v cpio >/dev/null 2>&1 || return 1
            (cd "$_aj_dir" && rpm2cpio "$_aj_pkg" | cpio -idm --quiet) || return 1
            ;;
        *) return 1 ;;
    esac

    _aj_bin="$(find "$_aj_dir" -type f -name blobfuse2 -perm -u+x -print -quit)"
    [ -n "$_aj_bin" ] || return 1
    for _aj_target in /usr/local/bin /usr/bin "$HOME/.local/bin" /tmp/aj-bin; do
        mkdir -p "$_aj_target" 2>/dev/null || continue
        if cp "$_aj_bin" "$_aj_target/blobfuse2" 2>/dev/null; then
            chmod 0755 "$_aj_target/blobfuse2"
            case ":$PATH:" in
                *":$_aj_target:"*) ;;
                *) PATH="$_aj_target:$PATH" && export PATH ;;
            esac
            rm -rf "$_aj_dir"
            return 0
        fi
    done
    rm -rf "$_aj_dir"
    return 1
}

_aj_install_blobfuse2() {
    command -v blobfuse2 >/dev/null 2>&1 && return 0

    _aj_asset="$(_aj_blobfuse_asset)" || {
        _aj_warn "unsupported distribution or architecture for blobfuse2"
        return 1
    }
    _aj_log "installing blobfuse2 {BLOBFUSE_VERSION} ($_aj_asset)"
    _aj_install_prereqs

    _aj_pkg="/tmp/${_aj_asset}"
    _aj_url="https://github.com/Azure/azure-storage-fuse/releases/download/blobfuse2-{BLOBFUSE_VERSION}/${_aj_asset}"
    _aj_fetch "$_aj_url" "$_aj_pkg" || return 1

    case "$_aj_pkg" in
        *.deb)
            if command -v apt-get >/dev/null 2>&1; then
                DEBIAN_FRONTEND=noninteractive apt-get -qq install -y "$_aj_pkg" >/dev/null 2>&1 ||
                    { command -v dpkg >/dev/null 2>&1 && dpkg -i "$_aj_pkg" >/dev/null 2>&1; } || true
            elif command -v dpkg >/dev/null 2>&1; then
                dpkg -i "$_aj_pkg" >/dev/null 2>&1 || true
            fi
            ;;
        *.rpm)
            if command -v dnf >/dev/null 2>&1; then
                dnf install -y -q "$_aj_pkg" >/dev/null 2>&1 || true
            elif command -v yum >/dev/null 2>&1; then
                yum install -y -q "$_aj_pkg" >/dev/null 2>&1 || true
            elif command -v zypper >/dev/null 2>&1; then
                zypper --non-interactive --quiet install "$_aj_pkg" >/dev/null 2>&1 || true
            elif command -v rpm >/dev/null 2>&1; then
                rpm -i --nodeps "$_aj_pkg" >/dev/null 2>&1 || true
            fi
            ;;
    esac

    command -v blobfuse2 >/dev/null 2>&1 || _aj_extract_package "$_aj_pkg" || true
    rm -f "$_aj_pkg"

    if command -v blobfuse2 >/dev/null 2>&1; then
        _aj_log "blobfuse2 ready: $(blobfuse2 --version 2>/dev/null | head -n1)"
        return 0
    fi
    _aj_warn "blobfuse2 installation failed"
    return 1
}

# Write the config blobfuse2 reads. Called again on every refresh so a replaced
# credential reaches the running mount.
_aj_write_config() {
    # $1 config path, $2 cache dir, $3 account, $4 container, $5 sas file
    _aj_sas="$(tr -d ' \t\r\n' <"$5" 2>/dev/null)"
    _aj_sas="${_aj_sas#\?}"
    [ -n "$_aj_sas" ] || return 1
    _aj_umask="$(umask)"
    umask 077
    cat >"$1" <<AJ_BLOBFUSE_CFG
logging:
  type: base
  file-path: /tmp/aj-blobfuse-$3-$4.log
  level: log_warning
components: [libfuse, file_cache, attr_cache, azstorage]
file_cache:
  path: $2
  timeout-sec: 120
  allow-non-empty-temp: true
  sync-to-flush: true
libfuse:
  attribute-expiration-sec: 120
  entry-expiration-sec: 120
  negative-entry-expiration-sec: 240
attr_cache:
  timeout-sec: 7200
azstorage:
  type: block
  account-name: $3
  endpoint: https://$3.blob.core.windows.net/
  container: $4
  mode: sas
  sas: $_aj_sas
AJ_BLOBFUSE_CFG
    umask "$_aj_umask"
}

# Report whether $1 is a live mount point. `mountpoint` lives in util-linux,
# which base images may omit, so fall back to /proc/mounts rather than letting
# a missing binary (exit 127) read as "not mounted".
_aj_is_mounted() {
    if command -v mountpoint >/dev/null 2>&1; then
        mountpoint -q "$1" 2>/dev/null
        return $?
    fi
    grep -qs " $1 " /proc/mounts
}

_aj_blob_mount() {
    # $1 identifier, $2 mount dir, $3 account, $4 container, $5 sas file
    _aj_id="$1"
    _aj_dir="$2"
    _aj_account="$3"
    _aj_container="$4"
    _aj_sas_file="$5"

    if _aj_is_mounted "$_aj_dir"; then
        _aj_log "$_aj_dir is already mounted"
        return 0
    fi
    if [ ! -r "$_aj_sas_file" ]; then
        _aj_warn "credential file is missing: $_aj_sas_file"
        return 1
    fi

    _aj_cache="/tmp/aj-blobfuse-cache/$_aj_id"
    _aj_cfg="/tmp/aj-blobfuse-$_aj_id.yaml"
    mkdir -p "$_aj_dir" "$_aj_cache"
    # NFS-Ganesha and blobfuse2 both consult /etc/mtab to detect FUSE mounts.
    [ -e /etc/mtab ] || ln -sf /proc/mounts /etc/mtab 2>/dev/null || true

    _aj_write_config "$_aj_cfg" "$_aj_cache" "$_aj_account" "$_aj_container" "$_aj_sas_file" || {
        _aj_warn "credential file is empty: $_aj_sas_file"
        return 1
    }

    if ! blobfuse2 mount "$_aj_dir" --config-file="$_aj_cfg" -o allow_other; then
        _aj_warn "failed to mount $_aj_account/$_aj_container at $_aj_dir"
        return 1
    fi
    # blobfuse2 daemonises, so a zero exit only means the child was spawned.
    # Confirm the mount actually appeared before letting the job proceed;
    # otherwise the command would read an empty directory and silently
    # train on no data.
    _aj_ready=0
    for _aj_try in 1 2 3 4 5 6 7 8 9 10; do
        if _aj_is_mounted "$_aj_dir"; then
            _aj_ready=1
            break
        fi
        sleep 1
    done
    if [ "$_aj_ready" -ne 1 ]; then
        _aj_warn "mount did not appear for $_aj_account/$_aj_container at $_aj_dir"
        [ -f "/tmp/aj-blobfuse-$_aj_account-$_aj_container.log" ] &&
            tail -n 50 "/tmp/aj-blobfuse-$_aj_account-$_aj_container.log" >&2
        return 1
    fi
    _aj_log "mounted $_aj_account/$_aj_container at $_aj_dir"

    if [ "{REFRESH_SECONDS}" -gt 0 ] 2>/dev/null; then
        # Rewriting the config is enough: blobfuse2 watches the file and applies
        # a replaced SAS to the live mount, so the run survives token rotation.
        (
            while true; do
                sleep {REFRESH_SECONDS}
                _aj_is_mounted "$_aj_dir" || exit 0
                _aj_write_config "$_aj_cfg" "$_aj_cache" "$_aj_account" \
                    "$_aj_container" "$_aj_sas_file" || true
            done
        ) &
    fi
}
