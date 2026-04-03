# Roadmap

## Completed

- Rich terminal output — panels, spinners, styled messages
- Unified `aj_config.json` — defaults, workspace, repo_id in one file
- Template defaults — last-used template saved automatically
- `aj pull` shorthand — `user/repo` expands to SSH URL
- Codebase restructured into `cli/`, `core/`, `utils/`
- Type hints and error handling throughout
- Circular template inheritance detection
- 106 tests

## Next: Job Lifecycle

Direct Azure ML SDK integration for job management, bypassing `amlt` for speed.

- `aj jobs list` — show recent jobs from local records
- `aj jobs show <id>` — fetch live status from Azure
- `aj jobs cancel <id>` — cancel a running job
- `aj jobs logs <id>` — stream or download job logs

This requires a `core/backend.py` module with lazy Azure SDK imports to keep startup fast. See [amlt-integration.md](amlt-integration.md) for the design.

## Next: Template Management

- `aj template list` — replace current `aj list`
- `aj template show <name>` — display resolved config after inheritance
- `aj template validate <name>` — check for errors without submitting

## Future

- Pre-submission validation (check amlt installed, validate SKU names)
- Resubmit support (`aj run --resubmit <id>`)
- Job metrics — queue time tracking, cost estimation
- Shell completions
- Optional `[azure]` dependency group in pyproject.toml
