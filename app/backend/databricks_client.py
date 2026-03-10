"""
Async Databricks REST API client for the DOJ data migration pipeline.

Wraps the Databricks Jobs API, DBFS API, and SQL Statement Execution API
using aiohttp for non-blocking I/O.  All public methods are coroutines so
they can be awaited directly inside FastAPI route handlers and background
tasks.

Environment variables consumed:
    DATABRICKS_HOST   – workspace URL, e.g. https://adb-<id>.azuredatabricks.net
    DATABRICKS_TOKEN  – personal-access token or service-principal secret
    WAREHOUSE_ID      – SQL warehouse / compute ID for statement execution
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Typed exception hierarchy
# ---------------------------------------------------------------------------


class DatabricksError(Exception):
    """Base class for all Databricks client errors."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class DatabricksAuthError(DatabricksError):
    """Raised when the workspace returns 401 or 403."""


class DatabricksNotFoundError(DatabricksError):
    """Raised when a requested resource does not exist (404)."""


class DatabricksJobError(DatabricksError):
    """Raised when a job run ends in a terminal failure state."""


class DatabricksSQLError(DatabricksError):
    """Raised when a SQL statement execution fails."""


class DatabricksTimeoutError(DatabricksError):
    """Raised when polling a run or statement exceeds the allowed wait time."""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

_POLL_INTERVAL_SECONDS = 2.0
_SQL_POLL_TIMEOUT_SECONDS = 300


class DatabricksClient:
    """
    Async wrapper around the Databricks REST APIs required by the DOJ migration
    pipeline.

    Lifecycle
    ---------
    The underlying ``aiohttp.ClientSession`` is created lazily on the first
    request and closed by calling ``close()``.  Use the async context-manager
    protocol when possible::

        async with DatabricksClient(host, token) as client:
            run_id = await client.trigger_job(123, {"param": "value"})
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
    ) -> None:
        raw_host = host or os.environ.get("DATABRICKS_HOST", "")
        # Ensure the host always has an https:// scheme for URL construction.
        if raw_host and not raw_host.startswith("http"):
            raw_host = f"https://{raw_host}"
        self._host: str = raw_host.rstrip("/")
        # Token is read lazily at request time to support Apps runtime token rotation.
        # Store an initial value but _get_token() always re-reads from env.
        self._token: str = token or os.environ.get("DATABRICKS_TOKEN", "")
        self._warehouse_id: str = os.environ.get("WAREHOUSE_ID", "")
        self._session: Optional[aiohttp.ClientSession] = None

    # ------------------------------------------------------------------
    # Session management
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        """Return the current bearer token, always fetching fresh from the SDK.

        Priority:
        1. Databricks SDK credential chain — reads the rotating token file that
           the Apps runtime updates every ~45 minutes, so the token is always fresh.
        2. ``DATABRICKS_TOKEN`` env var — fallback for local dev / non-Apps envs.
        3. Previously cached token.
        """
        # Always try the SDK first: it reads the rotating credential file on every
        # call, ensuring we never use a stale env-var token for Jobs API calls.
        try:
            from databricks.sdk.config import Config  # type: ignore[import]

            cfg = Config(host=self._host or None)
            headers = cfg.authenticate()
            bearer = headers.get("Authorization", "")
            if bearer.startswith("Bearer "):
                self._token = bearer[7:]
                logger.debug("Obtained fresh token via Databricks SDK credential chain.")
                return self._token
        except Exception as sdk_exc:
            logger.debug("Databricks SDK credential chain failed: %s", sdk_exc)

        # Fallback: env var (valid for local dev; may be stale in long-running Apps).
        env_token = os.environ.get("DATABRICKS_TOKEN", "")
        if env_token:
            self._token = env_token
            return self._token

        return self._token

    def _get_session(self) -> aiohttp.ClientSession:
        """Return (and lazily create) the shared aiohttp session.

        Auth headers are NOT stored on the session; they are injected per-request
        by ``_request`` so that token rotation is handled transparently.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=60),
            )
        return self._session

    async def close(self) -> None:
        """Close the underlying HTTP session and release resources."""
        if self._session and not self._session.closed:
            await self._session.close()
            logger.debug("DatabricksClient session closed.")

    async def __aenter__(self) -> "DatabricksClient":
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Execute an HTTP request against the Databricks REST API.

        Parameters
        ----------
        method:
            HTTP verb (GET, POST, PUT, DELETE).
        path:
            API path relative to the workspace root, e.g. ``/api/2.1/jobs/runs/submit``.
        **kwargs:
            Forwarded verbatim to ``aiohttp.ClientSession.request``.

        Returns
        -------
        dict
            Parsed JSON response body.

        Raises
        ------
        DatabricksAuthError
            When the workspace returns 401 or 403.
        DatabricksNotFoundError
            When the workspace returns 404.
        DatabricksError
            For all other non-2xx responses.
        """
        url = f"{self._host}{path}"
        session = self._get_session()
        logger.debug("%s %s", method, url)

        # Inject a fresh auth header on every request (supports token rotation).
        token = self._get_token()
        extra_headers: dict[str, str] = {"Authorization": f"Bearer {token}"}
        if "headers" in kwargs:
            extra_headers.update(kwargs.pop("headers"))

        async with session.request(method, url, headers=extra_headers, **kwargs) as resp:
            body: dict[str, Any] = {}
            try:
                body = await resp.json(content_type=None)
            except Exception:
                body = {"raw": await resp.text()}

            if resp.status == 401 or resp.status == 403:
                raise DatabricksAuthError(
                    f"Authentication failed for {url}: {body}",
                    status_code=resp.status,
                )
            if resp.status == 404:
                raise DatabricksNotFoundError(
                    f"Resource not found: {url}",
                    status_code=resp.status,
                )
            if not resp.ok:
                raise DatabricksError(
                    f"Databricks API error {resp.status} for {url}: {body}",
                    status_code=resp.status,
                )

            return body

    # ------------------------------------------------------------------
    # Jobs API
    # ------------------------------------------------------------------

    async def trigger_job(self, job_id: int, params: dict[str, Any]) -> str:
        """
        Trigger a Databricks job run with the supplied parameters.

        Parameters
        ----------
        job_id:
            Numeric Databricks job ID to run.
        params:
            Key/value notebook or task parameters forwarded to the run.

        Returns
        -------
        str
            The run ID as a string.
        """
        payload: dict[str, Any] = {
            "job_id": job_id,
            "notebook_params": params,
        }
        response = await self._request(
            "POST", "/api/2.1/jobs/run-now", json=payload
        )
        run_id = str(response.get("run_id", ""))
        logger.info("Triggered job %s → run_id=%s", job_id, run_id)
        return run_id

    async def get_run_output(self, task_run_id: str) -> dict[str, Any]:
        """Fetch the notebook output of a completed task run."""
        return await self._request(
            "GET", "/api/2.1/jobs/runs/get-output",
            params={"run_id": task_run_id},
        )

    async def get_run_status(self, run_id: str) -> dict[str, Any]:
        """
        Poll the status of a job run.

        Parameters
        ----------
        run_id:
            Run ID returned by :meth:`trigger_job`.

        Returns
        -------
        dict
            Full run-state object from the Databricks API, including
            ``state.life_cycle_state`` and ``state.result_state``.
        """
        response = await self._request(
            "GET",
            "/api/2.1/jobs/runs/get",
            params={"run_id": run_id},
        )
        state = response.get("state", {})
        logger.debug(
            "run_id=%s lifecycle=%s result=%s",
            run_id,
            state.get("life_cycle_state"),
            state.get("result_state"),
        )
        return response

    async def get_job_runs(
        self, job_id: int, limit: int = 20
    ) -> list[dict[str, Any]]:
        """
        Retrieve recent runs for a specific job.

        Parameters
        ----------
        job_id:
            Numeric Databricks job ID.
        limit:
            Maximum number of runs to return (default 20, max 25 per API page).

        Returns
        -------
        list[dict]
            List of run objects ordered by start time descending.
        """
        response = await self._request(
            "GET",
            "/api/2.1/jobs/runs/list",
            params={"job_id": job_id, "limit": min(limit, 25)},
        )
        runs: list[dict[str, Any]] = response.get("runs", [])
        logger.debug("job_id=%s returned %d runs", job_id, len(runs))
        return runs

    async def wait_for_run(
        self,
        run_id: str,
        timeout_seconds: float = 3600.0,
    ) -> dict[str, Any]:
        """
        Block (async) until a run reaches a terminal state.

        Parameters
        ----------
        run_id:
            Run ID to poll.
        timeout_seconds:
            Maximum time to wait before raising :exc:`DatabricksTimeoutError`.

        Returns
        -------
        dict
            Final run-state object.

        Raises
        ------
        DatabricksJobError
            When the run terminates in a failure state.
        DatabricksTimeoutError
            When the run has not completed within ``timeout_seconds``.
        """
        terminal_states = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
        elapsed = 0.0

        while elapsed < timeout_seconds:
            status = await self.get_run_status(run_id)
            lifecycle = status.get("state", {}).get("life_cycle_state", "")
            if lifecycle in terminal_states:
                result = status.get("state", {}).get("result_state", "")
                if result != "SUCCESS":
                    raise DatabricksJobError(
                        f"Run {run_id} ended with result_state={result}",
                    )
                return status
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
            elapsed += _POLL_INTERVAL_SECONDS

        raise DatabricksTimeoutError(
            f"Run {run_id} did not complete within {timeout_seconds}s."
        )

    # ------------------------------------------------------------------
    # DBFS / ADLS API
    # ------------------------------------------------------------------

    async def upload_to_adls(
        self, file_content: bytes, adls_path: str
    ) -> None:
        """
        Write binary content to an ADLS Gen2 path via the Databricks DBFS API.

        The DBFS API is used here because the workspace mounts ADLS containers
        under ``/mnt/``.  Files larger than 1 MB are written in two steps:
        open → add-block → close.

        Parameters
        ----------
        file_content:
            Raw bytes to write.
        adls_path:
            Target DBFS/ADLS path, e.g. ``/mnt/doj-landing/uploads/file.xlsx``.
        """
        logger.info("Uploading %d bytes to %s", len(file_content), adls_path)

        # Use the streaming (open/addBlock/close) API to avoid base64 payload limits.
        open_payload = {"path": adls_path, "overwrite": True}
        open_response = await self._request(
            "POST", "/api/2.0/dbfs/create", json=open_payload
        )
        handle: int = open_response["handle"]

        chunk_size = 1 * 1024 * 1024  # 1 MB
        offset = 0
        try:
            while offset < len(file_content):
                chunk = file_content[offset : offset + chunk_size]
                encoded = base64.b64encode(chunk).decode("utf-8")
                await self._request(
                    "POST",
                    "/api/2.0/dbfs/add-block",
                    json={"handle": handle, "data": encoded},
                )
                offset += chunk_size
        finally:
            await self._request(
                "POST", "/api/2.0/dbfs/close", json={"handle": handle}
            )

        logger.info("Upload complete: %s", adls_path)

    # ------------------------------------------------------------------
    # SQL Statement Execution API
    # ------------------------------------------------------------------

    async def execute_sql(self, query: str) -> list[dict[str, Any]]:
        """
        Execute an arbitrary SQL statement on the configured SQL warehouse.

        Parameters
        ----------
        query:
            SQL query string.

        Returns
        -------
        list[dict]
            List of rows, each represented as a ``{column: value}`` dict.

        Raises
        ------
        DatabricksSQLError
            When the statement fails or is cancelled.
        DatabricksTimeoutError
            When the statement does not complete within the poll timeout.
        """
        if not self._warehouse_id:
            raise DatabricksError("WAREHOUSE_ID environment variable is not set.")

        payload = {
            "statement": query,
            "warehouse_id": self._warehouse_id,
            "wait_timeout": "0s",  # async mode
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        response = await self._request(
            "POST", "/api/2.0/sql/statements", json=payload
        )
        statement_id: str = response["statement_id"]
        return await self._poll_statement(statement_id)

    async def _poll_statement(
        self, statement_id: str
    ) -> list[dict[str, Any]]:
        """
        Poll a SQL statement until it reaches a terminal state.

        Parameters
        ----------
        statement_id:
            The statement ID returned by the execution API.

        Returns
        -------
        list[dict]
            Rows returned by the statement.
        """
        elapsed = 0.0
        while elapsed < _SQL_POLL_TIMEOUT_SECONDS:
            response = await self._request(
                "GET", f"/api/2.0/sql/statements/{statement_id}"
            )
            state = response.get("status", {}).get("state", "")

            if state == "SUCCEEDED":
                return self._parse_sql_result(response)
            if state in ("FAILED", "CANCELED", "CLOSED"):
                error = response.get("status", {}).get("error", {})
                raise DatabricksSQLError(
                    f"SQL statement {statement_id} ended with state={state}: {error}"
                )

            await asyncio.sleep(_POLL_INTERVAL_SECONDS)
            elapsed += _POLL_INTERVAL_SECONDS

        # Cancel the hung statement before raising.
        try:
            await self._request(
                "POST", f"/api/2.0/sql/statements/{statement_id}/cancel"
            )
        except DatabricksError:
            pass

        raise DatabricksTimeoutError(
            f"SQL statement {statement_id} timed out after {_SQL_POLL_TIMEOUT_SECONDS}s."
        )

    @staticmethod
    def _parse_sql_result(response: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Convert the Databricks JSON_ARRAY result format to a list of row dicts.

        Parameters
        ----------
        response:
            Raw API response from the statement execution endpoint.

        Returns
        -------
        list[dict]
            Each element is a ``{column_name: value}`` mapping.
        """
        manifest = response.get("manifest", {})
        schema = manifest.get("schema", {})
        columns: list[str] = [
            col["name"] for col in schema.get("columns", [])
        ]
        result = response.get("result", {})
        data_array: list[list[Any]] = result.get("data_array", []) or []
        return [dict(zip(columns, row)) for row in data_array]

    async def read_delta_table(
        self,
        full_table_name: str,
        filters: Optional[dict[str, Any]] = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Read rows from a Unity Catalog Delta table via SQL.

        Parameters
        ----------
        full_table_name:
            Three-part table name, e.g. ``doj_catalog.gold.reconciliation_issues``.
        filters:
            Optional equality filters expressed as ``{column: value}`` pairs.
            Multiple filters are combined with AND.
        limit:
            Maximum number of rows to return (default 1000).

        Returns
        -------
        list[dict]
            Rows matching the filters.
        """
        where_clause = ""
        if filters:
            conditions = " AND ".join(
                f"{col} = '{val}'" for col, val in filters.items()
            )
            where_clause = f" WHERE {conditions}"

        query = f"SELECT * FROM {full_table_name}{where_clause} LIMIT {limit}"
        logger.debug("read_delta_table: %s", query)
        return await self.execute_sql(query)
