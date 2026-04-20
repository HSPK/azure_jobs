"""Job CRUD, listing, pagination, and Run History mixin."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ._auth import _API_VERSION, _ML_SCOPE, _RE_TOP_SEARCH, _RE_TOP_SUB
from ._extract import _extract_rest_job


class _JobsMixin:
    """Job management methods for ``AzureMLJobsClient``.

    Assumes ``self._base``, ``self._session``, ``self._scope_path``,
    ``self._ensure_token()``, ``self._ensure_data_token()``,
    ``self._get_location()``, and ``self._data_plane_base`` are available.
    """

    # ---- job creation -------------------------------------------------------

    def create_or_update_job(
        self, name: str, body: dict[str, Any],
    ) -> dict[str, Any]:
        """Create or update a job via REST PUT."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = (
            f"{self._base}/jobs/{quote(name, safe='')}"  # type: ignore[attr-defined]
            f"?api-version={_API_VERSION}"
        )
        resp = self._session.put(url, json=body, timeout=60)  # type: ignore[attr-defined]
        if resp.status_code >= 400:
            try:
                detail = resp.json().get("error", {}).get("message", "")
                inner = resp.json().get("error", {}).get("details", [])
                if inner:
                    detail += " | " + str([d.get("message", "") for d in inner])
            except Exception:
                detail = resp.text[:500]
            from requests.exceptions import HTTPError
            raise HTTPError(
                f"{resp.status_code}: {detail}",
                response=resp,
            )
        return resp.json()

    # ---- list jobs ----------------------------------------------------------

    def list_jobs_page(
        self,
        next_link: str | None = None,
        *,
        list_view_type: str = "ActiveOnly",
        top: int = 30,
        job_type: str = "",
        tag: str = "",
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Fetch one server page of jobs.

        Returns ``(jobs, next_link)`` where *next_link* is ``None`` when
        there are no more pages.
        """
        self._ensure_token()  # type: ignore[attr-defined]
        if next_link:
            url = self._patch_top(next_link, top)
        else:
            url = self._build_list_url(
                list_view_type=list_view_type,
                top=top,
                job_type=job_type,
                tag=tag,
            )
        resp = self._session.get(url, timeout=30)  # type: ignore[attr-defined]
        resp.raise_for_status()
        data = resp.json()
        jobs = [_extract_rest_job(j) for j in data.get("value", [])]
        return jobs, data.get("nextLink")

    def _build_list_url(
        self,
        *,
        list_view_type: str,
        top: int,
        job_type: str,
        tag: str,
    ) -> str:
        """Build the initial list URL with server-side query parameters."""
        params: list[tuple[str, str]] = [
            ("api-version", _API_VERSION),
            ("listViewType", list_view_type),
            ("$top", str(top)),
        ]
        if job_type:
            params.append(("jobType", job_type))
        if tag:
            params.append(("tag", tag))
        qs = "&".join(f"{k}={quote(v, safe='')}" for k, v in params)
        return f"{self._base}/jobs?{qs}"  # type: ignore[attr-defined]

    @staticmethod
    def _patch_top(url: str, top: int) -> str:
        """Ensure ``$top=<top>`` in a server-returned nextLink URL."""
        if _RE_TOP_SEARCH.search(url):
            return _RE_TOP_SUB.sub(rf'\g<1>{top}', url)
        sep = "&" if "?" in url else "?"
        return f"{url}{sep}$top={top}"

    # ---- single job ---------------------------------------------------------

    def get_job(self, name: str) -> dict[str, Any]:
        """Fetch a single job by name, enriched with error from Run History."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = f"{self._base}/jobs/{name}?api-version={_API_VERSION}"  # type: ignore[attr-defined]
        resp = self._session.get(url, timeout=30)  # type: ignore[attr-defined]
        resp.raise_for_status()
        job = _extract_rest_job(resp.json())

        # Management plane doesn't return error details — fetch from Run History
        if job.get("status") == "Failed" and not job.get("error"):
            try:
                error = self._get_run_error(name)
                if error:
                    job["error"] = error
            except Exception:
                pass  # best-effort; don't block info display
        return job

    # ---- cancel -------------------------------------------------------------

    def cancel_job(self, name: str) -> None:
        """Cancel a job via REST API (POST, returns 202 Accepted)."""
        self._ensure_token()  # type: ignore[attr-defined]
        url = f"{self._base}/jobs/{name}/cancel?api-version={_API_VERSION}"  # type: ignore[attr-defined]
        resp = self._session.post(url, timeout=30)  # type: ignore[attr-defined]
        resp.raise_for_status()

    # ---- data plane (Run History API) ---------------------------------------

    def _get_run_history(self, job_name: str) -> dict[str, Any]:
        """Fetch the full Run History record for a job (data plane)."""
        self._get_location()  # type: ignore[attr-defined]
        if not self._data_plane_base:  # type: ignore[attr-defined]
            return {}
        token = self._ensure_data_token()  # type: ignore[attr-defined]
        url = (
            f"{self._data_plane_base}/history/v1.0/"  # type: ignore[attr-defined]
            f"{self._scope_path}/runs/{job_name}"  # type: ignore[attr-defined]
        )
        resp = self._session.get(  # type: ignore[attr-defined]
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def _get_run_error(self, job_name: str) -> str:
        """Extract error message from Run History API response."""
        data = self._get_run_history(job_name)
        err = data.get("error")
        if not err:
            return ""
        if isinstance(err, dict):
            inner = err.get("error", err)
            if isinstance(inner, dict):
                code = inner.get("code", "")
                msg = inner.get("message", "")
                if code and msg:
                    return f"{code}: {msg}"
                return msg or code or str(inner)
            return str(inner)
        return str(err)

    def get_run_log_urls(self, job_name: str) -> dict[str, str]:
        """Return ``{log_path: signed_url}`` for a run via Run History API."""
        data = self._get_run_history(job_name)
        return data.get("logFiles", {}) or {}
