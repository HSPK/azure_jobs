from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).parents[1]
SKILL = ROOT / "skills" / "azure-jobs"
SCRIPTS = SKILL / "scripts"


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


def test_skill_only_bundles_the_kubectl_retry_helper() -> None:
    assert {path.name for path in SCRIPTS.iterdir() if path.is_file()} == {
        "kubectl-exec-retry.py"
    }


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


def test_skill_documents_k8s_install_login_and_management_safely() -> None:
    main = (SKILL / "SKILL.md").read_text(encoding="utf-8")
    volcano = (SKILL / "references/volcano.md").read_text(encoding="utf-8")
    analysis = (SKILL / "references/kubernetes-analysis.md").read_text(
        encoding="utf-8"
    )
    combined = f"{main}\n{volcano}\n{analysis}"

    assert "aj --json k install --dry-run" in volcano
    assert "aj --json k login --dry-run" in volcano
    assert "does not create a Kubernetes cluster" in volcano
    assert "Also confirm before:" in main
    assert "`aj k8s install`" in main
    for command in (
        "aj k install",
        "aj k login",
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
