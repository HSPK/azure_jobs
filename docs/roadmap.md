# Roadmap

## Done

- Three submission backends: native (REST), volcano (kubectl), amlt (shim).
- Content-addressed code upload — re-submits skip the upload.
- Job lifecycle: `aj job list/show/cancel/logs/stats`.
- Templates: inheritance, validate, diff, pull, push.
- `aj quota` / `aj sku` / `aj sku check` (auto-toggles `-NvLink`).
- `aj code stats` — preview the upload payload.
- TUI dashboard (`aj dash`).

## Next

- Log streaming (`aj job logs -f`).
- `aj run --resubmit <id>`.
- Job diff between two submissions.
- Shell completions.
- Direct results download.
