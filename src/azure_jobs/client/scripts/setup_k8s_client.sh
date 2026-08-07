#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

K8S_MINOR="${AJ_K8S_MINOR:-v1.32}"
FORCE_REPO="${AJ_K8S_FORCE_REPO:-0}"
INSTALL_OIDC="${AJ_K8S_INSTALL_OIDC:-1}"
KEYRING="/etc/apt/keyrings/kubernetes-${K8S_MINOR}-apt-keyring.gpg"
REPO_FILE="/etc/apt/sources.list.d/kubernetes.list"
REPO_URL="https://pkgs.k8s.io/core:/stable:/${K8S_MINOR}/deb/"
TMP_DIR=""

cleanup() {
    if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
        rm -rf -- "${TMP_DIR}"
    fi
}
trap cleanup EXIT

fail() {
    printf '[ERROR] %s\n' "$*" >&2
    exit 1
}

[[ "${K8S_MINOR}" =~ ^v[0-9]+\.[0-9]+$ ]] ||
    fail "AJ_K8S_MINOR must look like v1.32."
command -v apt-get >/dev/null 2>&1 ||
    fail "aj k8s setup currently supports Debian/Ubuntu/WSL hosts with apt."
command -v sudo >/dev/null 2>&1 ||
    fail "sudo is required to install kubectl and repository metadata."

EXPECTED_REPO="deb [signed-by=${KEYRING}] ${REPO_URL} /"
if [[ -f "${REPO_FILE}" ]]; then
    if grep -Ev '^[[:space:]]*(#|$)|https://pkgs\.k8s\.io' "${REPO_FILE}" |
        grep -q .; then
        fail "Mixed repository file ${REPO_FILE} contains non-Kubernetes entries; edit it manually."
    fi
    CURRENT_REPO="$(tr -d '\r' <"${REPO_FILE}")"
    if [[ "${CURRENT_REPO}" != "${EXPECTED_REPO}" &&
          "${FORCE_REPO}" != "1" ]]; then
        fail "Kubernetes repository ${REPO_FILE} differs from ${K8S_MINOR}; rerun with --force-repo."
    fi
    if [[ "${CURRENT_REPO}" != "${EXPECTED_REPO}" || ! -f "${KEYRING}" ]]; then
        sudo rm -f -- "${REPO_FILE}"
    fi
fi

if [[ -d /etc/apt/sources.list.d ]]; then
    while IFS= read -r candidate; do
        [[ "${candidate}" == "${REPO_FILE}" ]] && continue
        grep -qF "https://pkgs.k8s.io" "${candidate}" || continue
        if grep -Ev '^[[:space:]]*(#|$)|https://pkgs\.k8s\.io' "${candidate}" |
            grep -q .; then
            fail "Mixed repository file ${candidate} contains non-Kubernetes entries; edit it manually."
        fi
        if [[ "${FORCE_REPO}" != "1" ]]; then
            fail "Conflicting Kubernetes repository ${candidate}; rerun with --force-repo to remove it."
        fi
        printf '[WARNING] Removing conflicting Kubernetes repository %s\n' "${candidate}"
        sudo rm -f -- "${candidate}"
    done < <(find /etc/apt/sources.list.d -maxdepth 1 -type f -print)
fi

printf '[INFO] Installing Kubernetes client prerequisites...\n'
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg tar
sudo install -d -m 0755 /etc/apt/keyrings
sudo install -d -m 0755 /etc/apt/sources.list.d
TMP_DIR="$(mktemp -d)"

if [[ ! -f "${KEYRING}" ]]; then
    curl --fail --silent --show-error --location \
        --proto '=https' --tlsv1.2 \
        "${REPO_URL}Release.key" \
        --output "${TMP_DIR}/Release.key"
    gpg --dearmor <"${TMP_DIR}/Release.key" >"${TMP_DIR}/kubernetes.gpg"
    sudo install -m 0644 "${TMP_DIR}/kubernetes.gpg" "${KEYRING}"
fi

CURRENT_REPO=""
if [[ -f "${REPO_FILE}" ]]; then
    CURRENT_REPO="$(tr -d '\r' <"${REPO_FILE}")"
fi
if [[ "${CURRENT_REPO}" != "${EXPECTED_REPO}" ]]; then
    printf '%s\n' "${EXPECTED_REPO}" |
        sudo tee "${REPO_FILE}" >/dev/null
fi

sudo apt-get update
sudo apt-get install -y kubectl

if [[ "${INSTALL_OIDC}" != "1" ]]; then
    exit 0
fi

export PATH="${KREW_ROOT:-${HOME}/.krew}/bin:${PATH}"
if ! command -v kubectl-krew >/dev/null 2>&1; then
    OS="$(uname | tr '[:upper:]' '[:lower:]')"
    case "$(uname -m)" in
        x86_64) ARCH="amd64" ;;
        aarch64 | arm64) ARCH="arm64" ;;
        armv7l) ARCH="arm" ;;
        *) fail "Unsupported architecture for Krew: $(uname -m)" ;;
    esac
    KREW="krew-${OS}_${ARCH}"
    curl --fail --silent --show-error --location \
        --proto '=https' --tlsv1.2 \
        "https://github.com/kubernetes-sigs/krew/releases/latest/download/${KREW}.tar.gz" \
        --output "${TMP_DIR}/${KREW}.tar.gz"
    tar --extract --gzip --no-same-owner \
        --file "${TMP_DIR}/${KREW}.tar.gz" \
        --directory "${TMP_DIR}"
    "${TMP_DIR}/${KREW}" install krew
fi

if ! kubectl krew list 2>/dev/null | grep -qx 'oidc-login'; then
    kubectl krew install oidc-login
fi

printf '[INFO] kubectl and oidc-login are installed.\n'
