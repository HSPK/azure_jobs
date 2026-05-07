#!/usr/bin/env bash
# Install a CUDA-enabled torch into a uv-managed venv.
#
# Idempotent: if torch already imports successfully with CUDA, skip the
# install. Activates the venv on success so subsequent `python` /
# `torchrun` in the calling script use it.
#
# Assumes `uv` is already on PATH (Singularity images ship it).
#
# Override knobs:
#   TORCH_VERSION   — pin torch (default: latest)
#   TORCH_INDEX_URL — wheel index (default: cu124 wheel index)
#   AJ_VENV         — venv location (default: $HOME/.aj-venv)

set -euo pipefail

: "${TORCH_INDEX_URL:=https://download.pytorch.org/whl/cu124}"
: "${AJ_VENV:=$HOME/.aj-venv}"

command -v uv >/dev/null 2>&1 || {
    echo "[install_torch] FAIL: 'uv' not on PATH" >&2
    exit 1
}

if [[ ! -d "$AJ_VENV" ]]; then
    echo "[install_torch] creating venv at $AJ_VENV …"
    uv venv --seed "$AJ_VENV"
fi

# shellcheck disable=SC1091
source "$AJ_VENV/bin/activate"

if python -c "import torch; assert torch.cuda.is_available()" 2>/dev/null; then
    echo "[install_torch] torch already installed (cuda OK), skipping"
else
    echo "[install_torch] installing torch from $TORCH_INDEX_URL …"
    if [[ -n "${TORCH_VERSION:-}" ]]; then
        uv pip install --index-url "$TORCH_INDEX_URL" "torch==${TORCH_VERSION}"
    else
        uv pip install --index-url "$TORCH_INDEX_URL" torch
    fi
fi
uv pip install numpy

python - <<'PY'
import torch
print(f"[install_torch] torch={torch.__version__} cuda={torch.version.cuda} "
      f"cuda_available={torch.cuda.is_available()} "
      f"device_count={torch.cuda.device_count()}")
PY
