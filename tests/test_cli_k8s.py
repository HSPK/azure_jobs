from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from azure_jobs.client.cli import main
from azure_jobs.client.cli import k8s as k8s_cli
from azure_jobs.client.k8s_manager import MSR02_PROFILE
from azure_jobs.shared.errors import K8sError


class FakeK8sManager:
    def __init__(self) -> None:
        self.kubeconfig = Path("/tmp/kubeconfig")
        self.calls: list[tuple[str, object]] = []
        self.fail: tuple[str, str] | None = None

    def _maybe_fail(self, name: str) -> None:
        if self.fail is not None and self.fail[0] == name:
            raise K8sError(self.fail[1])

    def resolve_profile(self, name: str, **overrides):
        self._maybe_fail("resolve_profile")
        self.calls.append(("resolve_profile", (name, overrides)))
        return MSR02_PROFILE

    def setup(self, profile, **options):
        self._maybe_fail("setup")
        self.calls.append(("setup", (profile, options)))
        return {
            "context": profile.context,
            "namespace": profile.namespace,
            "kubeconfig": str(self.kubeconfig),
        }

    def status(self, *, check_cluster: bool = False):
        self._maybe_fail("status")
        self.calls.append(("status", check_cluster))
        return {
            "kubectl": "/usr/bin/kubectl",
            "oidc_login": "/home/u/.krew/bin/kubectl-oidc_login",
            "kubeconfig": str(self.kubeconfig),
            "context": "oidc@msr02",
            "namespace": "bonete01",
            "server": "https://cluster",
            "reachable": True,
        }

    def list_jobs(self, *, name: str = ""):
        self._maybe_fail("jobs")
        self.calls.append(("jobs", name))
        return [{"name": name or "job-a", "queue": "q", "phase": "Running", "reason": ""}]

    def list_queues(self):
        self._maybe_fail("queues")
        return [{"name": "q", "state": "Open", "weight": "1"}]

    def list_pods(self, *, job: str = ""):
        self._maybe_fail("pods")
        self.calls.append(("pods", job))
        return [{"name": "pod-a", "phase": "Running", "node": "n", "restarts": "0", "role": "master"}]

    def logs(self, pod: str, **options):
        self._maybe_fail("logs")
        self.calls.append(("logs", (pod, options)))
        return "safe log\n"

    def events(self, pod: str):
        self._maybe_fail("events")
        self.calls.append(("events", pod))
        return [{"time": "now", "type": "Warning", "reason": "Failed", "message": "safe"}]

    def delete_job(self, job: str):
        self._maybe_fail("delete")
        self.calls.append(("delete", job))
        return f"job.batch.volcano.sh/{job} deleted"


def _patch_manager(monkeypatch, fake: FakeK8sManager) -> None:
    monkeypatch.setattr(k8s_cli, "K8sManager", lambda **_kwargs: fake)


def test_k8s_and_k_alias_help_render() -> None:
    runner = CliRunner()
    for group in ("k8s", "k"):
        result = runner.invoke(main, [group, "--help"])
        assert result.exit_code == 0
        assert "setup" in result.output
        assert "jobs" in result.output
        assert "delete" in result.output


def test_setup_dry_run_supports_json() -> None:
    result = CliRunner().invoke(
        main,
        ["--json", "k", "setup", "--dry-run"],
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["metadata"]["kind"] == "k8s_setup_plan"
    assert payload["data"]["profile"] == "lambda-msr02"


def test_setup_requires_non_json_and_confirmation(
    monkeypatch,
) -> None:
    fake = FakeK8sManager()
    _patch_manager(monkeypatch, fake)
    runner = CliRunner()

    rejected = runner.invoke(main, ["--json", "k8s", "setup", "--yes"])
    assert rejected.exit_code == 1
    assert json.loads(rejected.output)["status"] == "failed"
    assert not any(call[0] == "setup" for call in fake.calls)

    accepted = runner.invoke(
        main,
        [
            "k8s",
            "setup",
            "--yes",
            "--skip-tools",
            "--no-verify",
            "--force-repo",
            "--kubernetes-minor",
            "v1.33",
        ],
    )
    assert accepted.exit_code == 0, accepted.output
    setup = next(value for name, value in fake.calls if name == "setup")
    _profile, options = setup
    assert options == {
        "kubernetes_minor": "v1.33",
        "install_tools": False,
        "force_repo": True,
        "verify": False,
    }
    assert "configured" in accepted.output


def test_setup_prompts_and_can_render_result_without_namespace(
    monkeypatch,
) -> None:
    fake = FakeK8sManager()
    original_setup = fake.setup

    def setup_without_namespace(profile, **options):
        result = original_setup(profile, **options)
        result["namespace"] = ""
        return result

    fake.setup = setup_without_namespace
    _patch_manager(monkeypatch, fake)

    result = CliRunner().invoke(
        main,
        ["k8s", "setup", "--skip-tools", "--no-verify"],
        input="y\n",
    )

    assert result.exit_code == 0
    assert "configured" in result.output
    assert "Namespace:" not in result.output

def test_status_jobs_queues_pods_and_events_emit_json(
    monkeypatch,
) -> None:
    fake = FakeK8sManager()
    _patch_manager(monkeypatch, fake)
    runner = CliRunner()

    status = json.loads(
        runner.invoke(
            main,
            ["--json", "k", "status", "--check-cluster"],
        ).output
    )
    assert status["metadata"]["kind"] == "k8s_status"
    assert status["data"]["reachable"] is True

    jobs = json.loads(
        runner.invoke(main, ["--json", "k8s", "jobs", "job-x"]).output
    )
    assert jobs["rows"][0]["name"] == "job-x"
    queues = json.loads(
        runner.invoke(main, ["--json", "k", "queues"]).output
    )
    assert queues["rows"][0]["state"] == "Open"
    pods = json.loads(
        runner.invoke(
            main,
            ["--json", "k8s", "pods", "--job", "job-x"],
        ).output
    )
    assert pods["rows"][0]["name"] == "pod-a"
    events = json.loads(
        runner.invoke(main, ["--json", "k", "events", "pod-a"]).output
    )
    assert events["rows"][0]["reason"] == "Failed"


def test_logs_json_forwards_bounded_options(monkeypatch) -> None:
    fake = FakeK8sManager()
    _patch_manager(monkeypatch, fake)

    result = CliRunner().invoke(
        main,
        [
            "--json",
            "k",
            "logs",
            "pod-a",
            "-c",
            "worker",
            "--tail",
            "42",
            "--previous",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["kind"] == "k8s_logs"
    assert payload["redacted"] is True
    assert payload["content"] == "safe log\n"
    assert fake.calls[-1] == (
        "logs",
        (
            "pod-a",
            {
                "container": "worker",
                "tail": 42,
                "previous": True,
                "raw": False,
            },
        ),
    )


def test_logs_and_delete_render_rich_output(monkeypatch) -> None:
    fake = FakeK8sManager()
    _patch_manager(monkeypatch, fake)
    runner = CliRunner()

    logs = runner.invoke(main, ["k8s", "logs", "pod-a"])
    deleted = runner.invoke(
        main,
        ["k", "delete", "job-a"],
        input="y\n",
    )

    assert logs.exit_code == 0
    assert "safe log" in logs.output
    assert deleted.exit_code == 0
    assert "deleted" in deleted.output


def test_delete_requires_confirmation_and_json_yes(monkeypatch) -> None:
    fake = FakeK8sManager()
    _patch_manager(monkeypatch, fake)
    runner = CliRunner()

    rejected = runner.invoke(main, ["--json", "k", "delete", "job-a"])
    assert rejected.exit_code == 1
    assert json.loads(rejected.output)["status"] == "failed"
    assert not any(call[0] == "delete" for call in fake.calls)

    deleted = runner.invoke(
        main,
        ["--json", "k8s", "delete", "job-a", "--yes"],
    )
    assert deleted.exit_code == 0
    payload = json.loads(deleted.output)
    assert payload["status"] == "ok"
    assert payload["job"] == "job-a"
    assert fake.calls[-1] == ("delete", "job-a")


def test_manager_failures_are_structured_in_json(monkeypatch) -> None:
    fake = FakeK8sManager()
    fake.fail = ("jobs", "cluster unavailable")
    _patch_manager(monkeypatch, fake)

    result = CliRunner().invoke(main, ["--json", "k8s", "jobs"])

    assert result.exit_code == 1
    assert json.loads(result.output) == {
        "kind": "command_result",
        "action": "k8s.jobs",
        "status": "failed",
        "message": "cluster unavailable",
    }


def test_manager_failures_use_click_error_in_rich_mode(monkeypatch) -> None:
    fake = FakeK8sManager()
    fake.fail = ("jobs", "cluster unavailable")
    _patch_manager(monkeypatch, fake)

    result = CliRunner().invoke(main, ["k8s", "jobs"])

    assert result.exit_code == 1
    assert "Error: cluster unavailable" in result.output
