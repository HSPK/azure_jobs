"""Convenience accessors for the defaults and experiment sections."""

from __future__ import annotations

import secrets

from . import prompts
from .models import AJDefaults
from .store import read_config, write_config

def get_defaults() -> AJDefaults:
    """Return the defaults section as an :class:AJDefaults dataclass."""
    return read_config().defaults

def get_experiment() -> str:
    """Return the configured experiment name, or empty string if unset."""
    return read_config().experiment

def ensure_experiment() -> str:
    """Return experiment name, prompting the user if not yet configured."""
    name = get_experiment()
    if name:
        return name

    suggestion = f"experiment-{secrets.token_hex(4)}"

    prompts._echo()
    prompts._echo("  No experiment configured yet.")
    name = prompts._prompt("  Experiment name", default=suggestion).strip() or suggestion

    cfg = read_config()
    cfg.experiment = name
    write_config(cfg)
    prompts._echo()
    prompts._echo(f"  ✓ Experiment set to: {name}")
    prompts._echo("    Change anytime with: aj config experiment <name>")
    prompts._echo()
    return name
