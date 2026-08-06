#!/usr/bin/env bash
set -euo pipefail

_aj_archive="${1:-${AZUREML_DATAREFERENCE_aj_code_archive:-}}"
_aj_expected_hash="${2:-${AJ_CODE_ARCHIVE_SHA256:-}}"
_aj_key="${AJ_ID:-job}"
_aj_key="${_aj_key//[^a-zA-Z0-9_.-]/_}"
_aj_code_dir="/tmp/aj-code-${_aj_key}"
_aj_lock="${_aj_code_dir}.lock"
_aj_ready="${_aj_code_dir}.ready"
_aj_failed="${_aj_code_dir}.failed"

if [ -z "$_aj_archive" ] || [ ! -f "$_aj_archive" ]; then
    echo "[aj] code archive input is missing or not a file: ${_aj_archive:-<empty>}" >&2
    echo "[aj] expected command input aj_code_archive or AZUREML_DATAREFERENCE_aj_code_archive" >&2
    exit 1
fi

if [ -z "$_aj_expected_hash" ]; then
    echo "[aj] expected code archive SHA-256 is missing" >&2
    exit 1
fi

_aj_install_tools() {
    if command -v tar >/dev/null 2>&1 &&
        command -v sha256sum >/dev/null 2>&1; then
        return 0
    fi

    echo "[aj] tar/coreutils are not installed; installing them before code extraction"
    _aj_root=()
    if [ "$EUID" -ne 0 ]; then
        if ! command -v sudo >/dev/null 2>&1; then
            echo "[aj] tar is required, but neither root access nor sudo is available" >&2
            return 1
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
        echo "[aj] tar is required, but no supported package manager was found (apt/apk/dnf/yum)" >&2
        return 1
    fi

    if ! command -v tar >/dev/null 2>&1 ||
        ! command -v sha256sum >/dev/null 2>&1; then
        echo "[aj] tool installation completed without making tar and sha256sum available on PATH" >&2
        return 1
    fi
}

_aj_prepare_code() {
    _aj_stage="${_aj_code_dir}.tmp.$$"
    printf '%s\n' "$_aj_stage" > "$_aj_lock/stage"
    _aj_install_tools || return 1
    read -r _aj_actual_hash _ < <(sha256sum "$_aj_archive")
    if [ "$_aj_actual_hash" != "$_aj_expected_hash" ]; then
        echo "[aj] code archive SHA-256 mismatch: expected $_aj_expected_hash, got $_aj_actual_hash" >&2
        return 1
    fi
    rm -rf "$_aj_stage" "$_aj_code_dir" || return 1
    mkdir -p "$_aj_stage" || return 1
    if ! tar -xzf "$_aj_archive" -C "$_aj_stage"; then
        echo "[aj] failed to extract code archive $_aj_archive into $_aj_stage" >&2
        rm -rf "$_aj_stage"
        return 1
    fi
    if [ ! -f "$_aj_stage/aj_runner.sh" ]; then
        echo "[aj] extracted code archive does not contain aj_runner.sh" >&2
        rm -rf "$_aj_stage"
        return 1
    fi
    mv "$_aj_stage" "$_aj_code_dir" || return 1
    _aj_stage=""
}

_aj_owner=0
_aj_stage=""
_aj_reclaim_guard="${_aj_lock}.reclaim"
_aj_reclaim_owner=0

_aj_release_reclaim_guard() {
    _aj_guard_pid=""
    if [ -f "$_aj_reclaim_guard/pid" ]; then
        read -r _aj_guard_pid < "$_aj_reclaim_guard/pid" || true
    fi
    if [ "$_aj_guard_pid" = "$$" ]; then
        rm -rf "$_aj_reclaim_guard"
    fi
    _aj_reclaim_owner=0
}

_aj_cleanup_lock() {
    if [ "$_aj_owner" = "1" ]; then
        _aj_lock_pid=""
        if [ -f "$_aj_lock/pid" ]; then
            read -r _aj_lock_pid < "$_aj_lock/pid" || true
        fi
        if [ "$_aj_lock_pid" = "$$" ]; then
            if [ -n "$_aj_stage" ]; then
                rm -rf "$_aj_stage"
            fi
            rm -rf "$_aj_lock"
        fi
        _aj_owner=0
    fi
    if [ "$_aj_reclaim_owner" = "1" ]; then
        _aj_release_reclaim_guard
    fi
}

_aj_try_reclaim_lock() {
    if ! mkdir "$_aj_reclaim_guard" 2>/dev/null; then
        return 1
    fi
    _aj_reclaim_owner=1
    printf '%s\n' "$$" > "$_aj_reclaim_guard/pid"
    _aj_current_pid=""
    _aj_current_stage=""
    if [ -f "$_aj_lock/pid" ]; then
        read -r _aj_current_pid < "$_aj_lock/pid" || true
    fi
    if [ -f "$_aj_lock/stage" ]; then
        read -r _aj_current_stage < "$_aj_lock/stage" || true
    fi
    _aj_reclaimed=1
    if [[ "$_aj_current_pid" =~ ^[0-9]+$ ]] &&
        ! kill -0 "$_aj_current_pid" 2>/dev/null; then
        echo "[aj] reclaiming abandoned code extraction lock from PID $_aj_current_pid" >&2
        _aj_expected_stage="${_aj_code_dir}.tmp.${_aj_current_pid}"
        if [ "$_aj_current_stage" = "$_aj_expected_stage" ]; then
            rm -rf "$_aj_current_stage"
        fi
        rm -rf "$_aj_lock"
        _aj_reclaimed=0
    elif ! [[ "$_aj_current_pid" =~ ^[0-9]+$ ]] &&
        [ "$_aj_waited" -ge 5 ]; then
        echo "[aj] reclaiming abandoned code extraction lock without owner metadata" >&2
        rm -rf "$_aj_lock"
        _aj_reclaimed=0
    fi
    _aj_release_reclaim_guard
    return "$_aj_reclaimed"
}

_aj_try_acquire_lock() {
    if ! mkdir "$_aj_reclaim_guard" 2>/dev/null; then
        return 1
    fi
    _aj_reclaim_owner=1
    printf '%s\n' "$$" > "$_aj_reclaim_guard/pid"
    _aj_acquired=1
    if mkdir "$_aj_lock" 2>/dev/null; then
        printf '%s\n' "$$" > "$_aj_lock/pid"
        if [ -f "$_aj_ready" ]; then
            rm -rf "$_aj_lock"
        else
            _aj_acquired=0
        fi
    fi
    _aj_release_reclaim_guard
    return "$_aj_acquired"
}

trap _aj_cleanup_lock EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

_aj_waited=0
while [ ! -f "$_aj_ready" ]; do
    if _aj_try_acquire_lock; then
        _aj_owner=1
        rm -f "$_aj_ready" "$_aj_failed"
        if _aj_prepare_code; then
            : > "$_aj_ready"
            rm -rf "$_aj_lock"
            _aj_owner=0
            break
        else
            : > "$_aj_failed"
            exit 1
        fi
    fi

    if [ -f "$_aj_failed" ]; then
        echo "[aj] another local process failed to prepare the code archive" >&2
        exit 1
    fi
    if _aj_try_reclaim_lock; then
        _aj_waited=0
        continue
    fi
    if [ "$_aj_waited" -ge 600 ]; then
        echo "[aj] timed out waiting 600s for local code archive extraction" >&2
        exit 1
    fi
    sleep 1
    _aj_waited=$((_aj_waited + 1))
done

trap - EXIT INT TERM

cd "$_aj_code_dir"
if [ ! -f aj_runner.sh ]; then
    echo "[aj] extracted code archive does not contain aj_runner.sh" >&2
    exit 1
fi
exec bash aj_runner.sh
