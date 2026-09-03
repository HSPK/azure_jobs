"""Heterogeneous Volcano Task schema, CLI, wire, and manifest tests."""

from __future__ import annotations

import copy
import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from azure_jobs.client.cli import main
from azure_jobs.server.resources import _spec_from_payload
from azure_jobs.server.submit.volcano._scripts import load_script
from azure_jobs.server.submit.volcano.config import (
    build_volcano_config_from_request,
    build_volcano_job,
)
from azure_jobs.server.submit.volcano.storage import build_blob_mount_plan
from azure_jobs.shared.errors import ConfigError
from azure_jobs.shared.job import StorageMount, build_job_spec
from azure_jobs.shared.opts import (
    VolcanoBlobMountOpts,
    VolcanoOpts,
    VolcanoTaskEnvironment,
    VolcanoTaskOpts,
)
from azure_jobs.shared.opts.volcano_tasks import load_tasks
from azure_jobs.shared.spec import RunShapeRequest, get_spec_hooks
from azure_jobs.shared.template import Template

from .helpers import write_template


def _heterogeneous_config() -> dict:
    return {
        "target": {
            "service": "volcano",
            "namespace": "training",
            "queue": "default",
            "context": "cluster-a",
        },
        "environment": {
            "image": "common-runtime:latest",
            "setup": ["python -m pip install -e ."],
        },
        "jobs": [
            {
                "name": "train",
                "sku": "heterogeneous",
                "command": ["echo template-default"],
                "submit_args": {
                    "env": {"LOG_LEVEL": "INFO"},
                    "container_args": {"shm_size": "64Gi"},
                },
            }
        ],
        "_extra": {
            "volcano": {
                # Deliberately not rank order.
                "tasks": {
                    "h100-worker": {
                        "replicas": 4,
                        "cpus_per_node": 96,
                        "memory": "1Ti",
                        "gpus_per_node": 8,
                        "rdma": True,
                        "processes_per_node": 8,
                        "node_selector": {
                            "nvidia.com/gpu.product": "H100-80GB-HBM3"
                        },
                        "environment": {
                            "image": "h100-runtime:latest",
                            "setup": [
                                "python -m pip install -e .",
                                "python -m pip install transformer-engine",
                            ],
                        },
                        "command": ["python h100_train.py"],
                        "env": {"WORKER_KIND": "h100"},
                        "container_args": {
                            "shm_size": "96Gi",
                            "capabilities": ["sys_admin"],
                            "scratch_mount_path": "/var/lib/containers",
                            "scratch_size": "200Gi",
                        },
                    },
                    "master": {
                        "replicas": 1,
                        "cpus_per_node": 16,
                        "memory": "64Gi",
                        "gpus_per_node": 0,
                        "rdma": False,
                        "processes_per_node": 1,
                        "node_selector": {"node-type": "cpu"},
                        "environment": {
                            "image": "coordinator-runtime:latest",
                            "setup": ["python -m pip install coordinator"],
                        },
                        "command": ["python coordinator.py"],
                        "env": {"SERVICE_PORT": "9000", "RANK": "17"},
                    },
                    "a100-worker": {
                        "replicas": 2,
                        "cpus_per_node": 96,
                        "memory": "512Gi",
                        "gpus_per_node": 8,
                        "rdma": True,
                        "processes_per_node": 8,
                        "node_selector": {
                            "nvidia.com/gpu.product": "A100-SXM4-80GB"
                        },
                        "env": {"WORKER_KIND": "a100"},
                    },
                }
            }
        },
    }


def _build_request(tmp_path: Path):
    template = Template.from_dict(_heterogeneous_config())
    return build_job_spec(
        template,
        name="heterogeneous_job",
        sid="abc12345",
        sku="heterogeneous",
        user_command="python",
        user_args=("train.py",),
        template_name="heterogeneous",
        nodes=99,
        gpus_per_node=99,
        processes_per_node=99,
        code_dir=str(tmp_path),
    )


def _task(job: dict, name: str) -> dict:
    return next(task for task in job["spec"]["tasks"] if task["name"] == name)


def _container(job: dict, name: str) -> dict:
    return _task(job, name)["template"]["spec"]["containers"][0]


def _env(container: dict) -> dict[str, str]:
    return {item["name"]: item["value"] for item in container.get("env", [])}


class _BlobPlan:
    enabled = True
    requires_secret = True
    uses_fic = False
    strategy = "direct"

    def setup_lines(self) -> list[str]:
        return ["echo mount-blob"]

    def main_sidecar_setup_lines(self) -> list[str]:
        return []

    def sidecar_volumes(self) -> list[dict]:
        return []

    def main_sidecar_volume_mounts(self) -> list[dict]:
        return []

    def sidecar_container(self):
        return None

    def occupied_main_mount_paths(self) -> list[str]:
        return ["/mnt/secret"]

    def volume(self) -> dict:
        return {"name": "blob-secret", "secret": {"secretName": "job-blob"}}

    def volume_mount(self) -> dict:
        return {
            "name": "blob-secret",
            "mountPath": "/mnt/secret",
            "readOnly": True,
        }


def test_task_schema_and_run_shape_are_typed(tmp_path: Path) -> None:
    request = _build_request(tmp_path)
    opts = request.backend_spec

    assert isinstance(opts, VolcanoOpts)
    assert isinstance(opts.tasks["master"], VolcanoTaskOpts)
    assert isinstance(
        opts.tasks["master"].environment,
        VolcanoTaskEnvironment,
    )
    assert opts.tasks["a100-worker"].shm_size == "64Gi"
    assert opts.tasks["h100-worker"].shm_size == "96Gi"
    assert opts.tasks["h100-worker"].capabilities == ["SYS_ADMIN"]
    assert opts.tasks["h100-worker"].scratch_mount_path == "/var/lib/containers"

    assert request.nodes == 7
    assert request.gpus_per_node == 0
    assert request.processes_per_node == 1
    assert request.env_vars["AJ_NODES"] == "7"
    assert request.env_vars["AJ_GPU_NODES"] == "6"
    assert request.env_vars["AJ_TOTAL_GPUS"] == "48"
    assert request.env_vars["AJ_PROCESSES"] == "48"
    assert request.extra == _heterogeneous_config()["_extra"]


def test_task_options_survive_wire_roundtrip(tmp_path: Path) -> None:
    rebuilt = _spec_from_payload(_build_request(tmp_path).to_dict())

    assert isinstance(rebuilt.backend_spec.tasks["master"], VolcanoTaskOpts)
    h100 = rebuilt.backend_spec.tasks["h100-worker"]
    assert h100.environment.image == "h100-runtime:latest"
    assert h100.command == ["python h100_train.py"]
    assert h100.capabilities == ["SYS_ADMIN"]
    assert h100.scratch_size == "200Gi"


def test_task_optional_sections_and_absent_topology_defaults() -> None:
    homogeneous = Template.from_dict(
        {
            "target": {"service": "volcano"},
            "jobs": [{"sku": "literal"}],
        }
    )
    unrelated_extra = Template.from_dict(
        {
            "target": {"service": "volcano"},
            "jobs": [{"sku": "literal"}],
            "_extra": {"volcano": {"future": True}},
        }
    )
    assert VolcanoOpts.from_template(homogeneous).tasks == {}
    assert VolcanoOpts.from_template(unrelated_extra).tasks == {}

    config = _heterogeneous_config()
    a100 = config["_extra"]["volcano"]["tasks"]["a100-worker"]
    a100.pop("env")
    a100.pop("node_selector")
    parsed = VolcanoOpts.from_template(Template.from_dict(config))
    assert parsed.tasks["a100-worker"].env == {}
    assert parsed.tasks["a100-worker"].node_selector == {}


def test_wire_task_loader_rejects_invalid_shapes_and_accepts_typed_tasks() -> None:
    assert load_tasks(None) == {}
    with pytest.raises(ConfigError, match="backend_spec.tasks must be a mapping"):
        load_tasks("invalid")
    with pytest.raises(ConfigError, match="Task 'master' must be a mapping"):
        load_tasks({"master": "invalid"})

    parsed = VolcanoOpts.from_template(Template.from_dict(_heterogeneous_config()))
    loaded = load_tasks(parsed.tasks)
    assert isinstance(loaded["master"], VolcanoTaskOpts)
    assert loaded["h100-worker"].capabilities == ["SYS_ADMIN"]


def test_node_selector_rejects_invalid_qualified_prefix() -> None:
    config = _heterogeneous_config()
    config["_extra"]["volcano"]["tasks"]["master"]["node_selector"] = {
        "/node-type": "cpu"
    }
    with pytest.raises(ConfigError, match="invalid node_selector key"):
        VolcanoOpts.from_template(Template.from_dict(config))


def test_config_resolves_inheritance_commands_env_and_rank_bases(
    tmp_path: Path,
) -> None:
    cfg = build_volcano_config_from_request(_build_request(tmp_path))

    assert cfg.nodes == 7
    assert [task.name for task in cfg.tasks] == [
        "master",
        "a100-worker",
        "h100-worker",
    ]
    master, a100, h100 = cfg.tasks
    assert [master.rank_base, a100.rank_base, h100.rank_base] == [0, 1, 3]

    assert master.image == "coordinator-runtime:latest"
    assert master.setup_commands == ["python -m pip install coordinator"]
    assert master.command[-1] == "python coordinator.py"
    assert "python train.py" not in master.command

    assert a100.image == "common-runtime:latest"
    assert a100.setup_commands == ["python -m pip install -e ."]
    assert a100.command[-2:] == ["echo template-default", "python train.py"]

    assert h100.image == "h100-runtime:latest"
    assert h100.command[-1] == "python h100_train.py"
    assert "python train.py" not in h100.command

    assert master.env_vars["LOG_LEVEL"] == "INFO"
    assert master.env_vars["SERVICE_PORT"] == "9000"
    assert master.env_vars["RANK"] == "17"
    assert a100.env_vars["WORKER_KIND"] == "a100"
    for task in cfg.tasks:
        assert task.env_vars["AJ_NODES"] == "7"
        assert task.env_vars["AJ_GPU_NODES"] == "6"
        assert task.env_vars["AJ_TOTAL_GPUS"] == "48"
        assert task.env_vars["AJ_TASK_NAME"] == task.name
        assert task.env_vars["AJ_TASK_REPLICAS"] == str(task.replicas)
        assert task.env_vars["AJ_GPUS_PER_NODE"] == str(task.gpus_per_node)


def test_manifest_expands_independent_pod_specs(tmp_path: Path) -> None:
    cfg = build_volcano_config_from_request(_build_request(tmp_path))
    job = build_volcano_job(cfg, namespace="training")

    assert job["spec"]["minAvailable"] == 7
    assert [task["name"] for task in job["spec"]["tasks"]] == [
        "master",
        "a100-worker",
        "h100-worker",
    ]
    assert [task["replicas"] for task in job["spec"]["tasks"]] == [1, 2, 4]

    master_pod = _task(job, "master")["template"]["spec"]
    a100_pod = _task(job, "a100-worker")["template"]["spec"]
    h100_pod = _task(job, "h100-worker")["template"]["spec"]
    master = _container(job, "master")
    a100 = _container(job, "a100-worker")
    h100 = _container(job, "h100-worker")

    assert master["image"] == "coordinator-runtime:latest"
    assert master["resources"]["requests"] == {"cpu": "16", "memory": "64Gi"}
    assert master_pod["tolerations"] == []
    assert master_pod["nodeSelector"] == {"node-type": "cpu"}

    assert a100["image"] == "common-runtime:latest"
    assert a100["resources"]["requests"]["nvidia.com/gpu"] == "8"
    assert a100["resources"]["requests"]["rdma/rdma_shared_device_a"] == "1"
    assert a100_pod["nodeSelector"] == {
        "nvidia.com/gpu.product": "A100-SXM4-80GB"
    }

    assert h100["image"] == "h100-runtime:latest"
    assert h100["securityContext"] == {
        "capabilities": {"add": ["SYS_ADMIN"]}
    }
    scratch = next(
        volume
        for volume in h100_pod["volumes"]
        if volume["name"] == "aj-scratch"
    )
    assert scratch["emptyDir"] == {"sizeLimit": "200Gi"}
    assert h100["resources"]["requests"]["ephemeral-storage"] == "200Gi"
    h100_shm = next(
        volume for volume in h100_pod["volumes"] if volume["name"] == "dshm"
    )
    a100_shm = next(
        volume for volume in a100_pod["volumes"] if volume["name"] == "dshm"
    )
    assert h100_shm["emptyDir"]["sizeLimit"] == "96Gi"
    assert a100_shm["emptyDir"]["sizeLimit"] == "64Gi"

    assert "python coordinator.py" in master["args"][0]
    assert "python train.py" in a100["args"][0]
    assert "python h100_train.py" in h100["args"][0]
    assert "AJ_NODE_RANK=$((0 + AJ_TASK_INDEX))" in master["args"][0]
    assert "AJ_NODE_RANK=$((1 + AJ_TASK_INDEX))" in a100["args"][0]
    assert "AJ_NODE_RANK=$((3 + AJ_TASK_INDEX))" in h100["args"][0]
    assert a100["args"][0].index("AJ_NODE_RANK=") < a100["args"][0].index(
        "python -m pip install -e ."
    )
    assert _env(a100)["AJ_TASK_NAME"] == "a100-worker"
    assert "AJ_TASK_INDEX" not in _env(a100)


def test_task_security_context_merges_with_blob_privilege(
    tmp_path: Path,
) -> None:
    cfg = build_volcano_config_from_request(_build_request(tmp_path))
    job = build_volcano_job(
        cfg,
        namespace="training",
        blob_plan=_BlobPlan(),
    )

    h100 = _container(job, "h100-worker")
    master = _container(job, "master")
    assert h100["securityContext"] == {
        "capabilities": {"add": ["SYS_ADMIN"]},
        "privileged": True,
    }
    assert master["securityContext"] == {"privileged": True}


def test_fic_sidecar_is_applied_to_every_heterogeneous_task(
    tmp_path: Path,
) -> None:
    request = _build_request(tmp_path)
    request.storage = {
        "data": StorageMount("acct", "cont", "/mnt/data")
    }
    request.backend_spec.blob_mount = VolcanoBlobMountOpts(
        auth="fic",
        strategy="sidecar",
        service_account="blob-workload",
    )
    cfg = build_volcano_config_from_request(request)
    plan = build_blob_mount_plan(
        request.storage,
        request.name,
        options=request.backend_spec.blob_mount,
    )

    job = build_volcano_job(cfg, namespace="training", blob_plan=plan)

    for task in job["spec"]["tasks"]:
        template = task["template"]
        assert (
            template["metadata"]["labels"]["azure.workload.identity/use"]
            == "true"
        )
        assert template["spec"]["serviceAccountName"] == "blob-workload"
        assert template["spec"]["initContainers"][0]["name"] == "aj-blob-nfs"
        main = template["spec"]["containers"][0]
        assert main["securityContext"]["capabilities"]["add"] == ["SYS_ADMIN"]
        assert "privileged" not in main["securityContext"]


def test_heterogeneous_preamble_uses_task_index_and_preserves_user_rank() -> None:
    script = "\n".join(load_script("heterogeneous_preamble.sh", RANK_BASE=3))
    script += (
        "\nprintf '%s|%s|%s|%s|%s' "
        '"$AJ_TASK_INDEX" "$AJ_NODE_RANK" "$NODE_RANK" "$RANK" "$MASTER_ADDR"'
    )
    env = {
        **os.environ,
        "HOSTNAME": "train-abcde-h100-worker-2",
        "AJ_TASK_NAME": "h100-worker",
        "AJ_NODES": "7",
        "VK_TASK_INDEX": "2",
    }

    result = subprocess.run(
        ["bash", "-c", script],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout == "2|5|5|5|train-abcde-master-0.train-abcde"

    env["RANK"] = "42"
    result = subprocess.run(
        ["bash", "-c", script],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.split("|")[3] == "42"


def test_task_scratch_conflict_fails_before_manifest(tmp_path: Path) -> None:
    config = _heterogeneous_config()
    config["_extra"]["volcano"]["tasks"]["h100-worker"]["container_args"][
        "scratch_mount_path"
    ] = "/dev/shm/runtime"
    config["_extra"]["volcano"]["tasks"]["h100-worker"]["container_args"].pop(
        "scratch_size"
    )
    request = build_job_spec(
        Template.from_dict(config),
        name="conflict",
        sid="abc12345",
        sku="heterogeneous",
        user_command="python",
        user_args=("train.py",),
        nodes=1,
        code_dir=str(tmp_path),
    )

    with pytest.raises(ConfigError, match="conflicts with another mount"):
        build_volcano_job(
            build_volcano_config_from_request(request),
            namespace="training",
        )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda config: config.update(_extra="invalid"),
            "Template _extra must be a mapping",
        ),
        (
            lambda config: config["_extra"].update(volcano="invalid"),
            "_extra.volcano must be a mapping",
        ),
        (
            lambda config: config["_extra"]["volcano"].update(tasks={}),
            "tasks must be a non-empty mapping",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"].update(
                master="invalid"
            ),
            "Task 'master' must be a mapping",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"].pop("master"),
            "require a 'master'",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                replicas=2
            ),
            "master.*exactly 1 replica",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"].update(
                {
                    "Bad_Name": config["_extra"]["volcano"]["tasks"].pop(
                        "a100-worker"
                    )
                }
            ),
            "Invalid Volcano Task name",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                unsupported=True
            ),
            "has unsupported fields: unsupported",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"][
                "a100-worker"
            ].pop("memory"),
            "missing required fields: memory",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                replicas=0
            ),
            "replicas must be an integer >= 1",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                cpus_per_node=0
            ),
            "cpus_per_node must be an integer >= 1",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                memory="zero"
            ),
            "memory must be a positive Kubernetes quantity",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                gpus_per_node=-1
            ),
            "gpus_per_node must be an integer >= 0",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                rdma="false"
            ),
            "rdma must be true or false",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                processes_per_node=0
            ),
            "processes_per_node must be an integer >= 1",
        ),
        (
            lambda config: config["target"].update(
                gpus_per_node=8,
                cpus_per_node=96,
                memory="512Gi",
                rdma=True,
            ),
            "require per-Task resources",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"][
                "a100-worker"
            ].update(node_selector="invalid"),
            "node_selector must be a mapping",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"][
                "a100-worker"
            ]["node_selector"].update({"bad key": "value"}),
            "invalid node_selector key",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"][
                "a100-worker"
            ]["node_selector"].update(
                {"nvidia.com/gpu.product": " invalid "}
            ),
            "invalid node_selector value",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"][
                "a100-worker"
            ].update(container_args="invalid"),
            "container_args must be a mapping",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"][
                "a100-worker"
            ].update(container_args={"securityContext": {}}),
            "container_args has unsupported fields",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"][
                "a100-worker"
            ].update(container_args={"shm_size": "invalid"}),
            "shm_size must be a positive Kubernetes quantity",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                environment="invalid"
            ),
            "environment must be a mapping",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                environment={"unknown": True}
            ),
            "environment has unsupported fields",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                environment={"image": 42}
            ),
            "environment.image must be a string",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                environment={"setup": [42]}
            ),
            "environment.setup must be a string or list of strings",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                command=[]
            ),
            "command must not contain empty commands",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                env="invalid"
            ),
            "env must be a mapping",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                env={"": "invalid"}
            ),
            "env keys must be non-empty strings",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"].update(
                env={"VALUE": ["invalid"]}
            ),
            "env value for 'VALUE' must be scalar",
        ),
        (
            lambda config: config["_extra"]["volcano"]["tasks"]["master"][
                "env"
            ].update({"AJ_NODE_RANK": "99"}),
            "is reserved by aj",
        ),
        (
            lambda config: config["jobs"][0].update(
                sku="heterogeneous-{nodes}"
            ),
            "cannot use \\{nodes\\}",
        ),
        (
            lambda config: config["jobs"][0].update(sku={"1": "invalid"}),
            "sku must be a literal string",
        ),
        (
            lambda config: config["jobs"][0].update(
                process_count_per_node=8
            ),
            "require per-Task processes_per_node",
        ),
        (
            lambda config: config.update(jobs=[]),
            "require jobs\\[0\\]",
        ),
    ],
)
def test_invalid_task_schema_fails_before_submission(mutate, message) -> None:
    config = copy.deepcopy(_heterogeneous_config())
    mutate(config)

    with pytest.raises(ConfigError, match=message):
        VolcanoOpts.from_template(Template.from_dict(config))


@pytest.mark.parametrize(
    "shape_request",
    [
        RunShapeRequest(nodes=7),
        RunShapeRequest(gpus_per_node=8),
        RunShapeRequest(processes_per_node=8),
    ],
)
def test_run_shape_rejects_explicit_topology(
    shape_request: RunShapeRequest,
) -> None:
    template = Template.from_dict(_heterogeneous_config())

    with pytest.raises(ConfigError, match="topology is defined in YAML"):
        get_spec_hooks("volcano").resolve_run_shape(template, shape_request)


@pytest.mark.parametrize(
    "flag",
    [
        ["-n", "7"],
        ["-p", "8"],
        ["--ppn", "8"],
    ],
)
def test_cli_rejects_shape_flags_for_heterogeneous_template(
    aj_env,
    flag: list[str],
) -> None:
    write_template(
        aj_env["template_home"],
        "heterogeneous",
        _heterogeneous_config(),
    )

    result = CliRunner().invoke(
        main,
        ["run", "-d", "-t", "heterogeneous", *flag, "python", "train.py"],
    )

    assert result.exit_code != 0
    assert "topology is defined in YAML" in result.output


def test_cli_dry_run_reports_tasks_without_mutating_config(aj_env) -> None:
    write_template(
        aj_env["template_home"],
        "heterogeneous",
        _heterogeneous_config(),
    )
    config = json.loads(aj_env["config_fp"].read_text())
    config["defaults"].update({"nodes": 9, "processes": 3})
    aj_env["config_fp"].write_text(json.dumps(config))

    result = CliRunner().invoke(
        main,
        [
            "--json",
            "run",
            "-d",
            "-t",
            "heterogeneous",
            "python",
            "train.py",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    request = payload["request"]
    assert request["nodes"] == 7
    assert request["gpu_nodes"] == 6
    assert request["total_gpus"] == 48
    assert request["total_processes"] == 49
    assert [task["name"] for task in request["tasks"]] == [
        "master",
        "a100-worker",
        "h100-worker",
    ]
    saved = json.loads(aj_env["config_fp"].read_text())["defaults"]
    assert saved == {"template": "default", "nodes": 9, "processes": 3}


def test_cli_rejects_amlt_for_heterogeneous_template(aj_env) -> None:
    write_template(
        aj_env["template_home"],
        "heterogeneous",
        _heterogeneous_config(),
    )

    result = CliRunner().invoke(
        main,
        [
            "run",
            "-d",
            "--amlt",
            "-t",
            "heterogeneous",
            "python",
            "train.py",
        ],
    )

    assert result.exit_code != 0
    assert "cannot be submitted with --amlt" in result.output


def test_template_validate_reports_heterogeneous_schema_errors(aj_env) -> None:
    config = _heterogeneous_config()
    config["_extra"]["volcano"]["tasks"]["master"]["replicas"] = 2
    path = write_template(
        aj_env["template_home"],
        "invalid-heterogeneous",
        config,
    )
    path.write_text(yaml.safe_dump({"base": None, "config": config}))

    result = CliRunner().invoke(
        main,
        ["--json", "template", "validate", "invalid-heterogeneous"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["invalid_count"] == 1
    assert "master' must have exactly 1 replica" in payload["results"][0][
        "issues"
    ][0]
