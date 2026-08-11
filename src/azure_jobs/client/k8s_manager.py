"""Local Kubernetes client setup and narrow Volcano resource operations."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import yaml

from azure_jobs.shared.errors import K8sError

log = logging.getLogger(__name__)

_SCRIPTS_DIR = Path(__file__).parent / "scripts"
_SETUP_SCRIPT = _SCRIPTS_DIR / "setup_k8s_client.sh"
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]*$")
_NAMESPACE_RE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$"
)
_RESOURCE_NAME_RE = re.compile(
    r"^[a-z0-9](?:[-a-z0-9.]*[a-z0-9])?$"
)
_K8S_MINOR_RE = re.compile(r"^v[0-9]+\.[0-9]+$")
_GROUP_NAMESPACE_RE = re.compile(
    r"^(?:external:)?(bonete[0-9]+)(?:-scalt-)?esg$",
    re.IGNORECASE,
)
_URL_QUERY = re.compile(r"(https?://[^?\s]+)\?[^\s]+", re.IGNORECASE)
_BEARER = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_SECRET_ASSIGNMENT = re.compile(
    r"\b([A-Za-z0-9_]*(?:TOKEN|PASSWORD|PASSWD|SECRET|SAS|SIG|KEY)"
    r"[A-Za-z0-9_]*)\s*([:=])\s*(\"[^\"]*\"|'[^']*'|[^\s,;]+)",
    re.IGNORECASE,
)

Runner = Callable[..., subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class K8sProfile:
    name: str
    cluster: str
    server: str
    context: str
    namespace: str
    user: str
    issuer_url: str
    client_id: str
    extra_scopes: str = "openid,profile,email,offline_access"


MSR02_PROFILE = K8sProfile(
    name="lambda-msr02",
    cluster="msr02",
    server="https://msr02.gw.lambda.ai",
    context="oidc@msr02",
    namespace="bonete01",
    user="oidc-msr02",
    issuer_url="https://auth.lambdalabs.com/",
    client_id="FkDN3B4toTrDElMTpzBbTpIdqGLiZHKp",
)

PROFILES: dict[str, K8sProfile] = {MSR02_PROFILE.name: MSR02_PROFILE}


def redact_k8s_output(value: str) -> str:
    text = _URL_QUERY.sub(r"\1?[REDACTED]", str(value))
    text = _BEARER.sub("Bearer [REDACTED]", text)
    return _SECRET_ASSIGNMENT.sub(r"\1\2[REDACTED]", text)


class K8sManager:
    def __init__(
        self,
        *,
        kubeconfig: Path | None = None,
        context: str = "",
        namespace: str = "",
        home: Path | None = None,
        runner: Runner | None = None,
    ) -> None:
        self.home = (home or Path.home()).expanduser().resolve()
        self.kubeconfig = (
            kubeconfig.expanduser().resolve()
            if kubeconfig is not None
            else self.home / ".kube" / "config"
        )
        self.context = context
        self.namespace = namespace
        self._runner = runner or subprocess.run

    def resolve_profile(
        self,
        profile_name: str,
        *,
        cluster: str = "",
        server: str = "",
        context: str = "",
        namespace: str = "",
        user: str = "",
        issuer_url: str = "",
        client_id: str = "",
        extra_scopes: str = "",
    ) -> K8sProfile:
        if profile_name not in PROFILES:
            raise K8sError(
                f"Unknown Kubernetes profile {profile_name!r}. "
                f"Available: {', '.join(sorted(PROFILES))}."
            )
        base = PROFILES[profile_name]
        profile = K8sProfile(
            name=profile_name,
            cluster=cluster or base.cluster,
            server=server or base.server,
            context=context or base.context,
            namespace=namespace or base.namespace,
            user=user or base.user,
            issuer_url=issuer_url or base.issuer_url,
            client_id=client_id or base.client_id,
            extra_scopes=extra_scopes or base.extra_scopes,
        )
        self._validate_profile(profile)
        return profile

    @staticmethod
    def _validate_profile(profile: K8sProfile) -> None:
        for label, value in (
            ("cluster", profile.cluster),
            ("context", profile.context),
            ("user", profile.user),
        ):
            if not _NAME_RE.fullmatch(value):
                raise K8sError(f"Invalid Kubernetes {label}: {value!r}.")
        if not _NAMESPACE_RE.fullmatch(profile.namespace):
            raise K8sError(
                f"Invalid Kubernetes namespace: {profile.namespace!r}."
            )
        for label, value in (
            ("server", profile.server),
            ("issuer URL", profile.issuer_url),
        ):
            parsed = urlparse(value)
            if parsed.scheme != "https" or not parsed.netloc:
                raise K8sError(f"Kubernetes {label} must be an HTTPS URL.")
        for label, value in (
            ("OIDC client ID", profile.client_id),
            ("OIDC scopes", profile.extra_scopes),
        ):
            if not value or any(ord(char) < 32 for char in value):
                raise K8sError(f"Invalid Kubernetes {label}.")

    def setup(
        self,
        profile: K8sProfile,
        *,
        kubernetes_minor: str = "v1.32",
        install_tools: bool = True,
        force_repo: bool = False,
        verify: bool = True,
    ) -> dict[str, Any]:
        """Compatibility composition of one-time install plus login."""
        if install_tools:
            self.install_tools(
                kubernetes_minor=kubernetes_minor,
                force_repo=force_repo,
                reinstall=True,
            )
        return self.login(profile, verify=verify)

    def install_tools(
        self,
        *,
        kubernetes_minor: str = "v1.32",
        force_repo: bool = False,
        reinstall: bool = False,
    ) -> dict[str, Any]:
        """Install kubectl, Krew, and oidc-login without changing kubeconfig."""
        if not _K8S_MINOR_RE.fullmatch(kubernetes_minor):
            raise K8sError("Kubernetes minor must look like v1.32.")
        kubectl = shutil.which("kubectl") or ""
        plugin = self._find_oidc_plugin()
        if kubectl and plugin is not None and not reinstall:
            return {
                "kubectl": kubectl,
                "plugin": str(plugin),
                "kubernetes_minor": kubernetes_minor,
                "changed": False,
            }
        env = {
            **os.environ,
            "HOME": str(self.home),
            "AJ_K8S_MINOR": kubernetes_minor,
            "AJ_K8S_FORCE_REPO": "1" if force_repo else "0",
            "AJ_K8S_INSTALL_OIDC": "1",
        }
        self._run_setup_script(env)
        plugin = self._resolve_oidc_plugin()
        return {
            "kubectl": shutil.which("kubectl") or "",
            "plugin": str(plugin),
            "kubernetes_minor": kubernetes_minor,
            "changed": True,
        }

    def login(
        self,
        profile: K8sProfile,
        *,
        verify: bool = True,
        fresh: bool = True,
        preserve_namespace: bool = True,
    ) -> dict[str, Any]:
        """Merge the OIDC profile and authenticate without installing tools."""
        plugin = self._resolve_oidc_plugin()
        existed_before = self.kubeconfig.exists()
        backup, changed = self._merge_kubeconfig(
            profile,
            plugin,
            preserve_namespace=preserve_namespace,
        )
        result: dict[str, Any] = {
            "profile": profile.name,
            "kubeconfig": str(self.kubeconfig),
            "backup": str(backup) if backup is not None else "",
            "context": profile.context,
            "namespace": profile.namespace,
            "plugin": str(plugin),
            "verified": False,
            "changed": changed,
        }
        if not verify:
            return result

        try:
            if fresh:
                self._clean_oidc_token(plugin)
            self.context = profile.context
            self.namespace = self.effective_namespace()
            auth = self.auth_whoami()
            detected = self._namespace_from_groups(auth.get("groups") or [])
            if detected and detected != self.namespace:
                self._set_context_namespace(profile.context, detected)
                self.namespace = detected
                result["namespace"] = detected
            else:
                result["namespace"] = self.namespace
            result["verified"] = True
            result["username"] = str(auth.get("username") or "")
            result["groups"] = list(auth.get("groups") or [])
            return result
        except BaseException:
            self._restore_failed_login(
                backup,
                changed=changed,
                existed_before=existed_before,
            )
            raise

    def _resolve_oidc_plugin(self) -> Path:
        plugin = self._find_oidc_plugin()
        if plugin is not None:
            return plugin
        raise K8sError(
            "kubectl oidc-login is not installed. Run `aj k8s install`, or "
            "put kubectl-oidc_login on PATH."
        )

    def _find_oidc_plugin(self) -> Path | None:
        expected = (
            Path(os.getenv("KREW_ROOT", str(self.home / ".krew")))
            / "bin"
            / "kubectl-oidc_login"
        )
        if expected.is_file():
            return expected
        discovered = shutil.which("kubectl-oidc_login")
        if discovered:
            return Path(discovered).resolve()
        return None

    def _clean_oidc_token(self, plugin: Path) -> None:
        try:
            result = self._runner(
                [str(plugin), "clean"],
                capture_output=True,
                text=True,
                check=False,
                timeout=60,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise K8sError(
                f"Cannot clear the OIDC token cache "
                f"({type(exc).__name__}: {exc})."
            ) from exc
        if result.returncode != 0:
            detail = redact_k8s_output(
                (result.stderr or result.stdout or "").strip()
            )
            raise K8sError(
                f"oidc-login cache cleanup exited with {result.returncode}: "
                f"{detail or '(no output)'}"
            )

    def _run_setup_script(self, env: dict[str, str]) -> None:
        if not _SETUP_SCRIPT.is_file():
            raise K8sError(
                f"Bundled Kubernetes setup script is missing: {_SETUP_SCRIPT}"
            )
        try:
            result = self._runner(
                ["bash", str(_SETUP_SCRIPT)],
                env=env,
                text=True,
                check=False,
                timeout=1800,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            log.exception("Kubernetes setup script failed")
            raise K8sError(
                f"Kubernetes client setup failed "
                f"({type(exc).__name__}: {exc}). Set AJ_DEBUG=1 for a traceback."
            ) from exc
        if result.returncode != 0:
            raise K8sError(
                f"Kubernetes client setup exited with {result.returncode}. "
                "Review the setup output above and rerun with AJ_DEBUG=1."
            )

    def _profile_config(self, profile: K8sProfile, plugin: Path) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Config",
            "preferences": {},
            "clusters": [
                {
                    "name": profile.cluster,
                    "cluster": {"server": profile.server},
                }
            ],
            "users": [
                {
                    "name": profile.user,
                    "user": {
                        "exec": {
                            "apiVersion": "client.authentication.k8s.io/v1",
                            "command": str(plugin),
                            "args": [
                                "get-token",
                                "--skip-open-browser",
                                "--grant-type=device-code",
                                f"--oidc-issuer-url={profile.issuer_url}",
                                f"--oidc-client-id={profile.client_id}",
                                f"--oidc-extra-scope={profile.extra_scopes}",
                            ],
                            "interactiveMode": "Never",
                            "provideClusterInfo": False,
                        }
                    },
                }
            ],
            "contexts": [
                {
                    "name": profile.context,
                    "context": {
                        "cluster": profile.cluster,
                        "namespace": profile.namespace,
                        "user": profile.user,
                    },
                }
            ],
            "current-context": profile.context,
        }

    def _merge_kubeconfig(
        self,
        profile: K8sProfile,
        plugin: Path,
        *,
        preserve_namespace: bool = False,
    ) -> tuple[Path | None, bool]:
        incoming = self._profile_config(profile, plugin)
        self.kubeconfig.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self.kubeconfig.with_name(f".{self.kubeconfig.name}.lock")
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl

            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            current = self._read_kubeconfig()
            merged = self._merge_named_config(
                current,
                incoming,
                preserve_namespace=preserve_namespace,
            )
            if merged == current:
                return None, False
            backup = self._backup_kubeconfig()
            self._write_kubeconfig(merged)
            return backup, True
        finally:
            os.close(lock_fd)

    def _read_kubeconfig(self) -> dict[str, Any]:
        if not self.kubeconfig.exists():
            return {}
        try:
            value = yaml.safe_load(self.kubeconfig.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError, UnicodeError) as exc:
            log.exception("Could not read kubeconfig %s", self.kubeconfig)
            raise K8sError(
                f"Cannot read kubeconfig {self.kubeconfig} "
                f"({type(exc).__name__}: {exc}); it was not modified."
            ) from exc
        if not isinstance(value, dict):
            raise K8sError(
                f"Kubeconfig {self.kubeconfig} must contain a YAML object."
            )
        return value

    def _backup_kubeconfig(self) -> Path | None:
        if not self.kubeconfig.exists():
            return None
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup = self.kubeconfig.with_name(
            f"{self.kubeconfig.name}.{timestamp}.bak"
        )
        try:
            shutil.copy2(self.kubeconfig, backup)
            backup.chmod(0o600)
        except OSError as exc:
            raise K8sError(
                f"Cannot back up kubeconfig to {backup} "
                f"({type(exc).__name__}: {exc}); setup was aborted."
            ) from exc
        return backup

    @staticmethod
    def _merge_named_config(
        current: dict[str, Any],
        incoming: dict[str, Any],
        *,
        preserve_namespace: bool = False,
    ) -> dict[str, Any]:
        merged = dict(current)
        merged["apiVersion"] = "v1"
        merged["kind"] = "Config"
        merged.setdefault("preferences", {})
        for section in ("clusters", "users", "contexts"):
            existing = list(merged.get(section) or [])
            replacement = incoming[section][0]
            if section == "contexts" and preserve_namespace:
                previous = next(
                    (
                        item
                        for item in existing
                        if isinstance(item, dict)
                        and item.get("name") == replacement["name"]
                    ),
                    None,
                )
                previous_namespace = str(
                    ((previous or {}).get("context") or {}).get("namespace")
                    or ""
                )
                if previous_namespace:
                    replacement = {
                        **replacement,
                        "context": {
                            **replacement["context"],
                            "namespace": previous_namespace,
                        },
                    }
            existing = [
                item
                for item in existing
                if not isinstance(item, dict)
                or item.get("name") != replacement["name"]
            ]
            existing.append(replacement)
            merged[section] = existing
        merged["current-context"] = incoming["current-context"]
        return merged

    def _restore_failed_login(
        self,
        backup: Path | None,
        *,
        changed: bool,
        existed_before: bool,
    ) -> None:
        if not changed:
            return
        try:
            if backup is not None:
                os.replace(backup, self.kubeconfig)
                self.kubeconfig.chmod(0o600)
            elif not existed_before:
                self.kubeconfig.unlink(missing_ok=True)
        except OSError as exc:
            log.exception("Could not restore kubeconfig after failed login")
            raise K8sError(
                f"Kubernetes login failed and kubeconfig rollback also failed "
                f"({type(exc).__name__}: {exc}). Backup: {backup or '(none)'}"
            ) from exc

    def _write_kubeconfig(self, value: dict[str, Any]) -> None:
        fd, raw_path = tempfile.mkstemp(
            prefix=f".{self.kubeconfig.name}.",
            suffix=".tmp",
            dir=self.kubeconfig.parent,
        )
        temporary = Path(raw_path)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                yaml.safe_dump(value, stream, sort_keys=False)
            temporary.chmod(0o600)
            os.replace(temporary, self.kubeconfig)
            self.kubeconfig.chmod(0o600)
        except OSError as exc:
            temporary.unlink(missing_ok=True)
            raise K8sError(
                f"Cannot write kubeconfig {self.kubeconfig} "
                f"({type(exc).__name__}: {exc})."
            ) from exc

    def _set_context_namespace(self, context: str, namespace: str) -> None:
        value = self._read_kubeconfig()
        found = False
        for entry in value.get("contexts") or []:
            if isinstance(entry, dict) and entry.get("name") == context:
                entry.setdefault("context", {})["namespace"] = namespace
                found = True
                break
        if not found:
            raise K8sError(f"Context {context!r} disappeared from kubeconfig.")
        self._write_kubeconfig(value)

    @staticmethod
    def _namespace_from_groups(groups: list[object]) -> str:
        for raw in groups:
            match = _GROUP_NAMESPACE_RE.fullmatch(str(raw))
            if match:
                return match.group(1).lower()
        return ""

    def _kubectl_base(self) -> list[str]:
        command = ["kubectl", "--kubeconfig", str(self.kubeconfig)]
        if self.context:
            command.extend(["--context", self.context])
        return command

    def effective_namespace(self) -> str:
        if self.namespace:
            return self.namespace
        config = self._read_kubeconfig()
        context = self.context or str(config.get("current-context") or "")
        for entry in config.get("contexts") or []:
            if isinstance(entry, dict) and entry.get("name") == context:
                return str(
                    (entry.get("context") or {}).get("namespace") or "default"
                )
        return "default"

    def _run_kubectl(
        self,
        args: list[str],
        *,
        timeout: float = 60,
    ) -> str:
        command = [*self._kubectl_base(), *args]
        try:
            result = self._runner(
                command,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        except FileNotFoundError as exc:
            raise K8sError(
                "kubectl is not installed. Run `aj k8s setup` first."
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise K8sError(
                f"Kubernetes command timed out after {exc.timeout}s: "
                f"{' '.join(command)}"
            ) from exc
        except OSError as exc:
            raise K8sError(
                f"Kubernetes command failed ({type(exc).__name__}: {exc}): "
                f"{' '.join(command)}"
            ) from exc
        if result.returncode != 0:
            detail = redact_k8s_output(
                (result.stderr or result.stdout or "").strip()
            )
            raise K8sError(
                f"Kubernetes command exited with {result.returncode}: "
                f"{' '.join(command)}\n{detail or '(no output)'}"
            )
        return result.stdout or ""

    def auth_whoami(self) -> dict[str, Any]:
        output = self._run_kubectl(["auth", "whoami", "-o", "json"], timeout=180)
        try:
            value = json.loads(output)
        except json.JSONDecodeError as exc:
            raise K8sError("kubectl auth whoami returned invalid JSON.") from exc
        return value if isinstance(value, dict) else {}

    def status(self, *, check_cluster: bool = False) -> dict[str, Any]:
        config = self._read_kubeconfig()
        context = self.context or str(config.get("current-context") or "")
        namespace = ""
        server = ""
        for entry in config.get("contexts") or []:
            if isinstance(entry, dict) and entry.get("name") == context:
                namespace = str(
                    (entry.get("context") or {}).get("namespace") or "default"
                )
                cluster = str((entry.get("context") or {}).get("cluster") or "")
                for candidate in config.get("clusters") or []:
                    if (
                        isinstance(candidate, dict)
                        and candidate.get("name") == cluster
                    ):
                        server = str(
                            (candidate.get("cluster") or {}).get("server") or ""
                        )
                        break
                break
        plugin = (
            Path(os.getenv("KREW_ROOT", str(self.home / ".krew")))
            / "bin"
            / "kubectl-oidc_login"
        )
        result = {
            "kubectl": shutil.which("kubectl") or "",
            "oidc_login": str(plugin) if plugin.is_file() else "",
            "kubeconfig": str(self.kubeconfig),
            "configured": self.kubeconfig.is_file(),
            "context": context,
            "namespace": self.namespace or namespace,
            "server": server,
            "reachable": None,
        }
        if check_cluster:
            try:
                self._run_kubectl(["get", "--raw=/readyz"], timeout=20)
                result["reachable"] = True
            except K8sError as exc:
                result["reachable"] = False
                result["error"] = str(exc)
        return result

    @staticmethod
    def _rows(output: str, columns: tuple[str, ...]) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        for line in output.splitlines():
            if not line.strip():
                continue
            values = line.split("\t", len(columns) - 1)
            values.extend([""] * (len(columns) - len(values)))
            rows.append(dict(zip(columns, values)))
        return rows

    def list_jobs(self, *, name: str = "") -> list[dict[str, str]]:
        selector: list[str] = []
        if name:
            self._validate_resource_name(name, kind="Volcano Job")
            selector = ["--field-selector", f"metadata.name={name}"]
        output = self._run_kubectl(
            [
                "get",
                "jobs.batch.volcano.sh",
                "-n",
                self.effective_namespace(),
                *selector,
                "-o",
                'jsonpath={range .items[*]}{.metadata.name}{"\\t"}'
                '{.spec.queue}{"\\t"}{.status.state.phase}{"\\t"}'
                '{.status.state.reason}{"\\n"}{end}',
            ]
        )
        return self._rows(output, ("name", "queue", "phase", "reason"))

    def list_queues(self) -> list[dict[str, str]]:
        output = self._run_kubectl(
            [
                "get",
                "queues.scheduling.volcano.sh",
                "-o",
                'jsonpath={range .items[*]}{.metadata.name}{"\\t"}'
                '{.status.state}{"\\t"}{.spec.weight}{"\\n"}{end}',
            ]
        )
        return self._rows(output, ("name", "state", "weight"))

    def list_pods(self, *, job: str = "") -> list[dict[str, str]]:
        selector: list[str] = []
        if job:
            self._validate_resource_name(job, kind="Volcano Job")
            selector = ["-l", f"volcano.sh/job-name={job}"]
        output = self._run_kubectl(
            [
                "get",
                "pods",
                "-n",
                self.effective_namespace(),
                *selector,
                "-o",
                'jsonpath={range .items[*]}{.metadata.name}{"\\t"}'
                '{.status.phase}{"\\t"}{.spec.nodeName}{"\\t"}'
                '{.status.containerStatuses[*].restartCount}{"\\t"}'
                '{.metadata.labels.role}{"\\n"}{end}',
            ]
        )
        return self._rows(output, ("name", "phase", "node", "restarts", "role"))

    def logs(
        self,
        pod: str,
        *,
        container: str = "",
        tail: int = 200,
        previous: bool = False,
        raw: bool = False,
    ) -> str:
        self._validate_resource_name(pod, kind="Pod")
        args = [
            "logs",
            pod,
            "-n",
            self.effective_namespace(),
            f"--tail={tail}",
        ]
        if container:
            args.extend(["-c", container])
        if previous:
            args.append("--previous")
        output = self._run_kubectl(args, timeout=120)
        return output if raw else redact_k8s_output(output)

    def events(self, pod: str) -> list[dict[str, str]]:
        self._validate_resource_name(pod, kind="Pod")
        output = self._run_kubectl(
            [
                "get",
                "events",
                "-n",
                self.effective_namespace(),
                "--field-selector",
                f"involvedObject.name={pod}",
                "--sort-by=.lastTimestamp",
                "-o",
                "json",
            ]
        )
        try:
            value = json.loads(output)
        except json.JSONDecodeError as exc:
            raise K8sError("kubectl events returned invalid JSON.") from exc
        if not isinstance(value, dict):
            raise K8sError("kubectl events JSON root must be an object.")
        rows: list[dict[str, str]] = []
        for item in value.get("items") or []:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "time": str(item.get("lastTimestamp") or ""),
                    "type": str(item.get("type") or ""),
                    "reason": str(item.get("reason") or ""),
                    "message": redact_k8s_output(str(item.get("message") or "")),
                }
            )
        return rows

    def delete_job(self, name: str) -> str:
        self._validate_resource_name(name, kind="Volcano Job")
        namespace = self.effective_namespace()
        secret = self._run_kubectl(
            [
                "get",
                "jobs.batch.volcano.sh",
                name,
                "-n",
                namespace,
                "-o",
                'jsonpath={.spec.tasks[0].template.spec.volumes['
                '?(@.name=="aj-blob-secrets")].secret.secretName}',
            ]
        ).strip()
        if secret:
            self._validate_resource_name(secret, kind="Kubernetes Secret")
        output = self._run_kubectl(
            [
                "delete",
                "jobs.batch.volcano.sh",
                name,
                "-n",
                namespace,
            ],
            timeout=120,
        ).strip()
        if secret:
            secret_output = self._run_kubectl(
                [
                    "delete",
                    "secret",
                    secret,
                    "-n",
                    namespace,
                    "--ignore-not-found",
                ],
                timeout=120,
            ).strip()
            if secret_output:
                output = f"{output}\n{secret_output}".strip()
        return output

    @staticmethod
    def _validate_resource_name(name: str, *, kind: str) -> None:
        if (
            len(name) > 253
            or not _RESOURCE_NAME_RE.fullmatch(name)
        ):
            raise K8sError(
                f"Invalid {kind} name {name!r}; broad flags and selectors are "
                "not accepted."
            )


__all__ = [
    "K8sManager",
    "K8sProfile",
    "MSR02_PROFILE",
    "PROFILES",
    "redact_k8s_output",
]
