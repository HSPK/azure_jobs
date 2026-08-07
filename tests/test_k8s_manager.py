from __future__ import annotations

import json
import os
import stat
import subprocess
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

import azure_jobs.client.k8s_manager as k8s_mod
from azure_jobs.client.k8s_manager import (
    K8sManager,
    MSR02_PROFILE,
    redact_k8s_output,
)
from azure_jobs.shared.errors import K8sError


def completed(
    command: list[str],
    *,
    code: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, code, stdout, stderr)


class Runner:
    def __init__(self, responder=None) -> None:
        self.calls: list[tuple[list[str], dict]] = []
        self.responder = responder or (
            lambda command, _kwargs: completed(command)
        )

    def __call__(self, command, **kwargs):
        command = list(command)
        self.calls.append((command, kwargs))
        return self.responder(command, kwargs)


def install_oidc_plugin(home: Path) -> Path:
    plugin = home / ".krew/bin/kubectl-oidc_login"
    plugin.parent.mkdir(parents=True, exist_ok=True)
    plugin.write_text("", encoding="utf-8")
    plugin.chmod(0o755)
    return plugin


@pytest.fixture
def manager(tmp_path: Path) -> K8sManager:
    home = tmp_path / "home"
    home.mkdir()
    install_oidc_plugin(home)
    return K8sManager(home=home, runner=Runner())


def test_profile_defaults_overrides_and_validation(manager: K8sManager) -> None:
    profile = manager.resolve_profile(
        "lambda-msr02",
        cluster="cluster-a",
        context="oidc@cluster-a",
        namespace="team-a",
    )
    assert profile.cluster == "cluster-a"
    assert profile.context == "oidc@cluster-a"
    assert profile.namespace == "team-a"
    assert profile.server == MSR02_PROFILE.server

    with pytest.raises(K8sError, match="Unknown Kubernetes profile"):
        manager.resolve_profile("unknown")


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("cluster", "bad name", "Invalid Kubernetes cluster"),
        ("context", "", "Invalid Kubernetes context"),
        ("user", "bad/name", "Invalid Kubernetes user"),
        ("namespace", "Upper", "Invalid Kubernetes namespace"),
        ("server", "http://cluster", "must be an HTTPS URL"),
        ("issuer_url", "issuer", "must be an HTTPS URL"),
        ("client_id", "bad\nid", "Invalid Kubernetes OIDC client ID"),
        ("extra_scopes", "", "Invalid Kubernetes OIDC scopes"),
    ],
)
def test_profile_rejects_invalid_values(
    manager: K8sManager,
    field: str,
    value: str,
    message: str,
) -> None:
    profile = replace(MSR02_PROFILE, **{field: value})
    with pytest.raises(K8sError, match=message):
        manager._validate_profile(profile)


def test_setup_merges_existing_kubeconfig_and_creates_private_backup(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    config = home / ".kube" / "config"
    config.parent.mkdir(parents=True)
    config.write_text(
        yaml.safe_dump(
            {
                "apiVersion": "v1",
                "kind": "Config",
                "clusters": [
                    {"name": "existing", "cluster": {"server": "https://old"}}
                ],
                "users": [{"name": "oidc", "user": {"token": "old"}}],
                "contexts": [
                    {
                        "name": "existing",
                        "context": {
                            "cluster": "existing",
                            "user": "oidc",
                        },
                    }
                ],
                "current-context": "existing",
            }
        ),
        encoding="utf-8",
    )
    config.chmod(0o644)
    install_oidc_plugin(home)
    manager = K8sManager(home=home, runner=Runner())

    result = manager.setup(
        MSR02_PROFILE,
        install_tools=False,
        verify=False,
    )

    written = yaml.safe_load(config.read_text(encoding="utf-8"))
    assert {item["name"] for item in written["clusters"]} == {
        "existing",
        "msr02",
    }
    assert written["current-context"] == "oidc@msr02"
    old_user = next(item for item in written["users"] if item["name"] == "oidc")
    assert old_user["user"]["token"] == "old"
    user = next(
        item for item in written["users"] if item["name"] == "oidc-msr02"
    )
    assert user["user"]["exec"]["command"].endswith("kubectl-oidc_login")
    assert "--grant-type=device-code" in user["user"]["exec"]["args"]
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
    backup = Path(result["backup"])
    assert backup.is_file()
    assert stat.S_IMODE(backup.stat().st_mode) == 0o600
    old = yaml.safe_load(backup.read_text(encoding="utf-8"))
    assert old["current-context"] == "existing"


def test_setup_runs_packaged_installer_and_detects_namespace_group(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()

    def respond(command: list[str], _kwargs: dict):
        if command[0] == "bash":
            return completed(command)
        assert command[-4:] == ["auth", "whoami", "-o", "json"]
        return completed(
            command,
            stdout=json.dumps(
                {
                    "username": "user@example.com",
                    "groups": ["system:authenticated", "external:bonete42esg"],
                }
            ),
        )

    runner = Runner(respond)
    install_oidc_plugin(home)
    manager = K8sManager(home=home, runner=runner)
    result = manager.setup(
        MSR02_PROFILE,
        kubernetes_minor="v1.33",
        force_repo=True,
    )

    setup_command, setup_kwargs = runner.calls[0]
    assert setup_command == ["bash", str(k8s_mod._SETUP_SCRIPT)]
    assert setup_kwargs["env"]["AJ_K8S_MINOR"] == "v1.33"
    assert setup_kwargs["env"]["AJ_K8S_FORCE_REPO"] == "1"
    assert setup_kwargs["timeout"] == 1800
    assert result["verified"] is True
    assert result["namespace"] == "bonete42"
    written = yaml.safe_load(manager.kubeconfig.read_text(encoding="utf-8"))
    context = next(
        item for item in written["contexts"] if item["name"] == "oidc@msr02"
    )
    assert context["context"]["namespace"] == "bonete42"


def test_setup_verify_keeps_default_namespace_without_matching_group(
    tmp_path: Path,
) -> None:
    runner = Runner(
        lambda command, _kwargs: completed(
            command,
            stdout=json.dumps(
                {"username": "user", "groups": ["system:authenticated"]}
            ),
        )
    )
    install_oidc_plugin(tmp_path)
    manager = K8sManager(home=tmp_path, runner=runner)
    result = manager.setup(
        MSR02_PROFILE,
        install_tools=False,
        verify=True,
    )
    assert result["namespace"] == "bonete01"
    assert result["verified"] is True


def test_setup_rejects_bad_minor_and_supports_no_verify(manager: K8sManager) -> None:
    with pytest.raises(K8sError, match="must look like v1.32"):
        manager.setup(MSR02_PROFILE, kubernetes_minor="latest")

    result = manager.setup(
        MSR02_PROFILE,
        install_tools=False,
        verify=False,
    )
    assert result["verified"] is False
    assert result["backup"] == ""


def test_setup_script_missing_timeout_error_and_exit_are_actionable(
    manager: K8sManager,
) -> None:
    with patch.object(k8s_mod, "_SETUP_SCRIPT", Path("/missing/script")):
        with pytest.raises(K8sError, match="script is missing"):
            manager.setup(MSR02_PROFILE)

    manager._runner = Runner(
        lambda command, _kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(command, 3)
        )
    )
    with pytest.raises(K8sError, match="TimeoutExpired"):
        manager.setup(MSR02_PROFILE)

    manager._runner = Runner(
        lambda command, _kwargs: completed(command, code=7)
    )
    with pytest.raises(K8sError, match="exited with 7"):
        manager.setup(MSR02_PROFILE)


def test_invalid_kubeconfig_is_never_overwritten(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.write_text("[invalid", encoding="utf-8")
    install_oidc_plugin(tmp_path)
    manager = K8sManager(kubeconfig=config, home=tmp_path, runner=Runner())

    with pytest.raises(K8sError, match="it was not modified"):
        manager.setup(MSR02_PROFILE, install_tools=False, verify=False)
    assert config.read_text(encoding="utf-8") == "[invalid"

    config.write_text("- item\n", encoding="utf-8")
    with pytest.raises(K8sError, match="must contain a YAML object"):
        manager.setup(MSR02_PROFILE, install_tools=False, verify=False)


def test_kubeconfig_backup_and_atomic_write_failures_are_actionable(
    tmp_path: Path,
) -> None:
    config = tmp_path / "config"
    config.write_text("{}\n", encoding="utf-8")
    install_oidc_plugin(tmp_path)
    manager = K8sManager(kubeconfig=config, home=tmp_path, runner=Runner())

    with patch.object(k8s_mod.shutil, "copy2", side_effect=OSError("backup denied")):
        with pytest.raises(K8sError, match="Cannot back up kubeconfig"):
            manager.setup(MSR02_PROFILE, install_tools=False, verify=False)

    with patch.object(k8s_mod.os, "replace", side_effect=OSError("replace denied")):
        with pytest.raises(K8sError, match="Cannot write kubeconfig"):
            manager.setup(MSR02_PROFILE, install_tools=False, verify=False)


def test_kubeconfig_merge_replaces_only_matching_named_entries() -> None:
    incoming = K8sManager(
        home=Path("/tmp"),
        runner=Runner(),
    )._profile_config(MSR02_PROFILE, Path("/plugin"))
    current = {
        "clusters": [
            {"name": "msr02", "cluster": {"server": "https://stale"}},
            {"name": "other", "cluster": {"server": "https://other"}},
        ],
        "users": [],
        "contexts": [],
        "extension": {"keep": True},
    }

    merged = K8sManager._merge_named_config(current, incoming)

    assert len([item for item in merged["clusters"] if item["name"] == "msr02"]) == 1
    assert merged["extension"] == {"keep": True}
    assert merged["current-context"] == "oidc@msr02"


def test_set_context_namespace_rejects_disappeared_context(
    manager: K8sManager,
) -> None:
    manager.kubeconfig.parent.mkdir(parents=True)
    manager.kubeconfig.write_text(
        yaml.safe_dump({"contexts": []}),
        encoding="utf-8",
    )
    with pytest.raises(K8sError, match="disappeared"):
        manager._set_context_namespace("missing", "team")


def test_status_reads_context_and_optional_reachability(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()
    runner = Runner(lambda command, _kwargs: completed(command, stdout="ok"))
    plugin = install_oidc_plugin(home)
    manager = K8sManager(home=home, runner=runner)
    manager.setup(MSR02_PROFILE, install_tools=False, verify=False)

    with patch.object(k8s_mod.shutil, "which", return_value="/usr/bin/kubectl"):
        result = manager.status(check_cluster=True)

    assert result["configured"] is True
    assert result["context"] == "oidc@msr02"
    assert result["namespace"] == "bonete01"
    assert result["server"] == MSR02_PROFILE.server
    assert result["reachable"] is True
    assert result["oidc_login"] == str(plugin)


def test_status_reports_cluster_error_as_data(tmp_path: Path) -> None:
    manager = K8sManager(
        home=tmp_path,
        runner=Runner(
            lambda command, _kwargs: completed(
                command,
                code=1,
                stderr="connection denied",
            )
        ),
    )
    result = manager.status(check_cluster=True)
    assert result["reachable"] is False
    assert "connection denied" in result["error"]
    assert manager.effective_namespace() == "default"


def test_resource_queries_use_narrow_fields_and_effective_namespace(
    tmp_path: Path,
) -> None:
    home = tmp_path / "home"
    home.mkdir()

    def respond(command: list[str], _kwargs: dict):
        joined = " ".join(command)
        if "delete jobs.batch.volcano.sh" in joined:
            return completed(command, stdout="job.batch.volcano.sh deleted\n")
        if "delete secret" in joined:
            return completed(command, stdout="secret deleted\n")
        if 'name==\"aj-blob-secrets\"' in joined:
            return completed(command, stdout="job-a-blob")
        if "jobs.batch.volcano.sh" in joined:
            return completed(command, stdout="job-a\tq\tRunning\t\n")
        if "queues.scheduling.volcano.sh" in joined:
            return completed(command, stdout="q\tOpen\t1\n")
        if "get pods" in joined:
            return completed(command, stdout="pod-a\tRunning\tnode-a\t0\tmaster\n")
        if "get events" in joined:
            return completed(
                command,
                stdout=json.dumps(
                    {
                        "items": [
                            {
                                "lastTimestamp": "now",
                                "type": "Warning",
                                "reason": "Failed",
                                "message": "url=https://blob/path?sig=secret-token",
                            }
                        ]
                    }
                ),
            )
        if "logs pod-a" in joined:
            return completed(
                command,
                stdout="API_TOKEN=secret-token\ntraining\n",
            )
        raise AssertionError(joined)

    runner = Runner(respond)
    install_oidc_plugin(home)
    manager = K8sManager(home=home, runner=runner)
    manager.setup(MSR02_PROFILE, install_tools=False, verify=False)
    manager.context = "oidc@msr02"

    assert manager.list_jobs() == [
        {"name": "job-a", "queue": "q", "phase": "Running", "reason": ""}
    ]
    assert manager.list_queues() == [{"name": "q", "state": "Open", "weight": "1"}]
    assert manager.list_pods(job="job-a")[0]["name"] == "pod-a"
    assert manager.logs("pod-a") == "API_TOKEN=[REDACTED]\ntraining\n"
    assert manager.logs("pod-a", raw=True).startswith("API_TOKEN=secret-token")
    assert "[REDACTED]" in manager.events("pod-a")[0]["message"]
    assert manager.delete_job("job-a") == (
        "job.batch.volcano.sh deleted\nsecret deleted"
    )

    commands = [" ".join(command) for command, _kwargs in runner.calls]
    resource_commands = [command for command in commands if " get " in command]
    assert resource_commands
    assert all("-n bonete01" in command or "queues." in command for command in resource_commands)
    assert not any("-o yaml" in command for command in commands)
    assert not any(
        "-o json " in command and "get events" not in command
        for command in commands
    )
    pod_command = next(command for command in commands if "get pods" in command)
    assert "volcano.sh/job-name=job-a" in pod_command
    secret_query = next(
        command for command in commands if 'name=="aj-blob-secrets"' in command
    )
    assert ".spec.tasks[0]" in secret_query


def test_logs_add_container_previous_and_tail_flags(tmp_path: Path) -> None:
    runner = Runner(lambda command, _kwargs: completed(command, stdout="ok"))
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        namespace="ns",
        runner=runner,
    )
    assert manager.logs(
        "pod",
        container="worker",
        tail=42,
        previous=True,
    ) == "ok"
    command = runner.calls[-1][0]
    assert ["-c", "worker"] == command[-3:-1]
    assert "--previous" == command[-1]
    assert "--tail=42" in command


@pytest.mark.parametrize(
    ("failure", "message"),
    [
        (FileNotFoundError("kubectl"), "not installed"),
        (subprocess.TimeoutExpired(["kubectl"], 9), "timed out after 9s"),
        (OSError("broken"), "OSError: broken"),
    ],
)
def test_kubectl_process_failures_are_actionable(
    tmp_path: Path,
    failure: BaseException,
    message: str,
) -> None:
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        runner=Runner(
            lambda _command, _kwargs: (_ for _ in ()).throw(failure)
        ),
    )
    with pytest.raises(K8sError, match=message):
        manager.list_jobs()


def test_kubectl_exit_error_redacts_credentials(tmp_path: Path) -> None:
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        runner=Runner(
            lambda command, _kwargs: completed(
                command,
                code=2,
                stderr="url=https://blob/path?sig=secret-token",
            )
        ),
    )
    with pytest.raises(K8sError) as error:
        manager.list_jobs()
    assert "secret-token" not in str(error.value)
    assert "[REDACTED]" in str(error.value)


def test_auth_whoami_rejects_invalid_json(tmp_path: Path) -> None:
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        runner=Runner(lambda command, _kwargs: completed(command, stdout="bad")),
    )
    with pytest.raises(K8sError, match="invalid JSON"):
        manager.auth_whoami()


def test_auth_whoami_non_object_returns_empty(tmp_path: Path) -> None:
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        runner=Runner(
            lambda command, _kwargs: completed(command, stdout="[]")
        ),
    )
    assert manager.auth_whoami() == {}


def test_pods_reject_job_without_app_label(tmp_path: Path) -> None:
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        namespace="ns",
        runner=Runner(lambda command, _kwargs: completed(command, stdout="")),
    )
    assert manager.list_pods(job="job-a") == []


def test_resource_names_reject_broad_flags_and_selectors(tmp_path: Path) -> None:
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        namespace="ns",
        runner=Runner(),
    )
    for operation in (
        lambda: manager.delete_job("--all"),
        lambda: manager.list_jobs(name="-l"),
        lambda: manager.list_pods(job="name=other"),
        lambda: manager.logs("../pod"),
        lambda: manager.events("pod/name"),
    ):
        with pytest.raises(K8sError, match="broad flags and selectors"):
            operation()


def test_events_reject_invalid_json_shapes(tmp_path: Path) -> None:
    responses = iter(("bad", "[]"))
    manager = K8sManager(
        kubeconfig=tmp_path / "config",
        namespace="ns",
        runner=Runner(
            lambda command, _kwargs: completed(
                command,
                stdout=next(responses),
            )
        ),
    )
    with pytest.raises(K8sError, match="invalid JSON"):
        manager.events("pod-a")
    with pytest.raises(K8sError, match="root must be an object"):
        manager.events("pod-a")


def test_skip_tools_resolves_plugin_from_path_or_fails(
    tmp_path: Path,
) -> None:
    discovered = tmp_path / "bin/kubectl-oidc_login"
    discovered.parent.mkdir()
    discovered.write_text("", encoding="utf-8")
    manager = K8sManager(home=tmp_path, runner=Runner())

    with patch.object(k8s_mod.shutil, "which", return_value=str(discovered)):
        result = manager.setup(
            MSR02_PROFILE,
            install_tools=False,
            verify=False,
        )
    config = yaml.safe_load(manager.kubeconfig.read_text(encoding="utf-8"))
    user = config["users"][0]
    assert user["user"]["exec"]["command"] == str(discovered.resolve())
    assert result["plugin"] == str(discovered.resolve())

    manager.kubeconfig.unlink()
    with patch.object(k8s_mod.shutil, "which", return_value=None):
        with pytest.raises(K8sError, match="oidc-login is not installed"):
            manager.setup(
                MSR02_PROFILE,
                install_tools=False,
                verify=False,
            )


def test_named_job_and_unmatched_context_branches(tmp_path: Path) -> None:
    config = tmp_path / "config"
    config.write_text(
        yaml.safe_dump(
            {
                "current-context": "wanted",
                "contexts": ["invalid", {"name": "other", "context": {}}],
                "clusters": ["invalid"],
            }
        ),
        encoding="utf-8",
    )
    runner = Runner(
        lambda command, _kwargs: completed(
            command,
            stdout="job-a\tq\tCompleted\t\n",
        )
    )
    manager = K8sManager(kubeconfig=config, runner=runner)

    assert manager.effective_namespace() == "default"
    assert manager.status()["server"] == ""
    assert manager.list_jobs(name="job-a")[0]["name"] == "job-a"
    assert "metadata.name=job-a" in runner.calls[-1][0]


def test_rows_and_group_namespace_helpers() -> None:
    assert K8sManager._rows("", ("a",)) == []
    assert K8sManager._rows("one\ttwo\n", ("a", "b", "c")) == [
        {"a": "one", "b": "two", "c": ""}
    ]
    assert K8sManager._rows("\nvalue\n", ("a",)) == [{"a": "value"}]
    assert K8sManager._namespace_from_groups(
        ["system:authenticated", "external:bonete09-scalt-esg"]
    ) == "bonete09"
    assert K8sManager._namespace_from_groups(["other"]) == ""


def test_redaction_covers_urls_bearer_and_assignments() -> None:
    value = (
        "https://blob/path?sig=url-secret "
        "Authorization: Bearer bearer-secret "
        "API_TOKEN=assignment-secret"
    )
    result = redact_k8s_output(value)
    assert "url-secret" not in result
    assert "bearer-secret" not in result
    assert "assignment-secret" not in result


def test_setup_script_is_strict_and_syntax_valid() -> None:
    result = subprocess.run(
        ["bash", "-n", str(k8s_mod._SETUP_SCRIPT)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    source = k8s_mod._SETUP_SCRIPT.read_text(encoding="utf-8")
    assert "set -Eeuo pipefail" in source
    assert "sudo rm -f --" in source
    assert "kubectl krew list" in source
    assert "read -p" not in source
    assert "cat <<'EOF' > \"$HOME/.kube/config\"" not in source
    assert source.index("EXPECTED_REPO=") < source.index("sudo apt-get update")
    assert "Mixed repository file" in source
