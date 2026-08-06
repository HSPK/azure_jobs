from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[1]
SKILL = ROOT / "skills" / "azure-jobs"
SCRIPTS = SKILL / "scripts"
sys.path.insert(0, str(SCRIPTS))

from redaction import bounded_tail, redact_text  # noqa: E402
from summary import summarize  # noqa: E402


@pytest.mark.parametrize(
    ("value", "secret"),
    [
        ("https://user:password@example.test/x", "password"),
        ("https://blob.test/x?sv=1&sig=url-secret", "url-secret"),
        ("Authorization: Bearer bearer-secret", "bearer-secret"),
        ("AZURE_STORAGE_ACCOUNT_KEY=assignment-secret", "assignment-secret"),
        ('{"access_token":"json-secret"}', "json-secret"),
        (r'{\"access_token\":\"escaped-secret\"}', "escaped-secret"),
        (r'{\"access_token\":\"abc\\\"def\"}', "abc"),
        (r'{\"access_token\":\"abc\\\"def\"}', "def"),
        ("sv=1&sig=sas-secret&sp=r", "sas-secret"),
        (
            "-----BEGIN PRIVATE KEY-----\npem-secret\n"
            "-----END PRIVATE KEY-----",
            "pem-secret",
        ),
        ("github_pat_abcdefghijklmnopqrstuvwxyz123456", "github_pat_"),
        (
            "eyJabcdefghijk.abcdefghijklmnop.abcdefghijklmnop",
            "eyJabcdefghijk",
        ),
    ],
)
def test_redaction_removes_common_secret_shapes(value: str, secret: str) -> None:
    assert secret in value
    assert secret not in redact_text(value)


def test_bounded_tail_redacts_after_selecting_last_lines() -> None:
    value = "old-secret\nkeep\nAPI_TOKEN=tail-secret"
    result = bounded_tail(value, 2)
    assert result.splitlines()[0] == "keep"
    assert "old-secret" not in result
    assert "tail-secret" not in result


def test_summary_whitelists_and_redacts_nested_values() -> None:
    value = {
        "kind": "submission_result",
        "status": "failed",
        "portal_url": "https://portal.test/job?sig=portal-secret",
        "error": r'{\"client_secret\":\"error-secret\"}',
        "request": {
            "template_name": "demo",
            "command": "echo command-secret",
        },
        "config": {
            "target": {"service": "sing", "name": "vc"},
            "environment": {"TOKEN": "config-secret"},
            "jobs": [{"name": "train", "sku": "1x40G1-A100"}],
        },
    }
    result = summarize(value)
    rendered = json.dumps(result)
    assert "portal-secret" not in rendered
    assert "error-secret" not in rendered
    assert "command-secret" not in rendered
    assert "config-secret" not in rendered
    assert result["request"] == {"template_name": "demo"}


def test_summary_reports_diff_paths_without_diff_content() -> None:
    result = summarize(
        {
            "kind": "template_diff",
            "has_changes": True,
            "diff": (
                "diff --git a/template/a.yaml b/template/a.yaml\n"
                "+API_TOKEN=diff-secret\n"
            ),
        }
    )
    assert result["changed_file_count"] == 1
    assert result["changed_files"] == ["template/a.yaml"]
    assert "diff-secret" not in json.dumps(result)


def _markdown_targets(value: str) -> list[str]:
    import re

    targets = re.findall(r"\[[^]]+\]\(([^)#]+)(?:#[^)]+)?\)", value)
    return [
        target
        for target in targets
        if "://" not in target and not target.startswith("#")
    ]


def test_skill_frontmatter_and_local_links_are_valid() -> None:
    text = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    _, frontmatter, _ = text.split("---\n", 2)
    metadata = yaml.safe_load(frontmatter)
    assert metadata["name"] == SKILL.name
    assert metadata["description"]

    for document in SKILL.rglob("*.md"):
        for target in _markdown_targets(document.read_text(encoding="utf-8")):
            assert (document.parent / target).resolve().exists(), (
                f"{document} has broken link {target}"
            )


def _fake_aj(path: Path, source: str) -> Path:
    executable = path / "aj"
    executable.write_text(f"#!/usr/bin/env python3\n{source}", encoding="utf-8")
    executable.chmod(0o755)
    return executable


def test_run_aj_json_redacts_stdout_and_stderr(tmp_path: Path) -> None:
    executable = _fake_aj(
        tmp_path,
        "import json, sys\n"
        "print(json.dumps({"
        "'kind': 'job_logs', 'status': 'Failed', "
        "'content': 'API_TOKEN=log-secret\\nlast line'}))\n"
        "print('CLIENT_SECRET=stderr-secret', file=sys.stderr)\n",
    )
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "run-aj-json.py"),
            "--log-tail",
            "20",
            "--",
            str(executable),
            "--json",
            "job",
            "logs",
            "demo",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "last line" in result.stdout
    assert "log-secret" not in result.stdout
    assert "stderr-secret" not in result.stderr


def test_run_aj_json_redacts_non_json_failure(tmp_path: Path) -> None:
    executable = _fake_aj(
        tmp_path,
        "import sys\n"
        "print('API_TOKEN=stdout-secret')\n"
        "print('sig=stderr-secret', file=sys.stderr)\n"
        "raise SystemExit(7)\n",
    )
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "run-aj-json.py"),
            "--",
            str(executable),
            "--json",
            "job",
            "logs",
            "demo",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 7
    assert "stdout-secret" not in result.stderr
    assert "stderr-secret" not in result.stderr


def test_redact_log_can_delete_private_input(tmp_path: Path) -> None:
    source = tmp_path / "raw.log"
    source.write_text("API_TOKEN=file-secret\nsafe line\n", encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "redact-log.py"),
            "--delete",
            str(source),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "file-secret" not in result.stdout
    assert "safe line" in result.stdout
    assert not source.exists()


@pytest.mark.parametrize(
    ("option", "value"),
    [("--timeout", "0"), ("--backoff", "-1"), ("--retries", "0")],
)
def test_kubectl_retry_rejects_invalid_limits(option: str, value: str) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPTS / "kubectl-exec-retry.py"),
            option,
            value,
            "pod",
            "--",
            "echo",
            "ok",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
