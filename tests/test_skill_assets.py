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


def test_bounded_tail_redacts_before_character_and_line_limits() -> None:
    long_secret = "secret-start-" + "x" * 2100 + "-secret-end"
    json_log = f'{{"api_key":"{long_secret}"}}'
    pem_log = (
        "before\n"
        "-----BEGIN PRIVATE KEY-----\n"
        "pem-secret-body\n"
        "-----END PRIVATE KEY-----"
    )

    json_result = bounded_tail(json_log, 1)
    pem_result = bounded_tail(pem_log, 2)

    assert "secret-start" not in json_result
    assert "secret-end" not in json_result
    assert "pem-secret-body" not in pem_result
    assert "[REDACTED" in json_result
    assert "[REDACTED PEM BLOCK]" in pem_result


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


def test_skill_documents_job_naming_and_runtime_environment() -> None:
    main = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    operations = (SKILL / "references/job-operations.md").read_text(
        encoding="utf-8"
    )
    templates = (SKILL / "references/templates.md").read_text(encoding="utf-8")
    combined = "\n".join((main, operations, templates))

    assert "AJ_NAME=TRAIN_NAME" in main
    assert "AJ_NAME=pretrain aj run" in operations
    assert "_<8-character-AJ-ID>" in combined
    assert "submit_args.env.AJ_NAME" in combined
    assert "azure_name" in operations
    for variable in (
        "AJ_NAME",
        "AJ_ID",
        "AJ_TEMPLATE",
        "AJ_SUBMIT_TIMESTAMP_UTC",
        "AJ_NODES",
        "AJ_GPUS_PER_NODE",
        "AJ_PROCESSES",
        "AJ_PROCESSES_PER_NODE",
    ):
        assert f"`{variable}`" in operations


def test_skill_documents_nested_container_runtime_safely() -> None:
    volcano = (SKILL / "references/volcano.md").read_text(encoding="utf-8")
    templates = (SKILL / "references/templates.md").read_text(encoding="utf-8")
    combined = f"{volcano}\n{templates}"

    assert "scratch_mount_path: /var/lib/containers" in combined
    assert "scratch_size: 200Gi" in combined
    assert "capabilities: [SYS_ADMIN]" in combined
    assert "ephemeral-storage" in combined
    assert "independent ephemeral scratch" in volcano
    assert "`ALL` is rejected" in combined
    assert "does not mount host devices" in " ".join(volcano.split())


def test_skill_documents_k8s_setup_and_management_safely() -> None:
    main = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    volcano = (SKILL / "references/volcano.md").read_text(encoding="utf-8")
    analysis = (SKILL / "references/kubernetes-analysis.md").read_text(
        encoding="utf-8"
    )
    combined = f"{main}\n{volcano}\n{analysis}"

    assert "aj --json k setup --dry-run" in volcano
    assert "does not create a Kubernetes cluster" in volcano
    assert "Also confirm before:" in main
    assert "`aj k8s setup`" in main
    for command in (
        "aj k status",
        "aj k queues",
        "aj k jobs",
        "aj k pods",
        "aj k logs",
        "aj k events",
        "aj k delete",
    ):
        assert command in combined
    assert "--raw" in volcano
    assert "--force-repo" in volcano


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
