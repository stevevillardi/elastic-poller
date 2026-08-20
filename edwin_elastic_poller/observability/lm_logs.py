"""Optional LogicMonitor Logs ingestion handler for edwin-elastic-poller."""

from __future__ import annotations

import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import requests

COMPONENT = "edwin-elastic-poller"
LM_LOGS_INGEST_PATH = "/rest/log/ingest"
# Operational summaries, errors, and startup always ship when LM Logs is enabled.
LM_OPERATIONAL_LEVEL = logging.INFO
# SDK modules that emit verbose mapping/delivery detail at DEBUG.
_THIRD_PARTY_LOGGERS = (
    "edwin_elastic_poller.sdk.common_event",
    "edwin_elastic_poller.sdk.edwin_request",
)


def env_bool(name: str, default: bool = False) -> bool:
    """Return True when an environment variable is set to a truthy string."""
    from edwin_elastic_poller.config import env_bool as _env_bool

    return _env_bool(name, default=default)


def verify_edwin_ssl() -> bool:
    """Return TLS verification for LM Logs HTTPS requests."""
    from edwin_elastic_poller import config

    return config.EDWIN_VERIFY_SSL


def sanitize_elastic_url(url: Optional[str]) -> str:
    """Return host:port for an Elasticsearch URL, stripping credentials."""
    if not url:
        return ""
    parsed = urlparse(url)
    if parsed.hostname:
        host = parsed.hostname
        if parsed.port:
            host = f"{host}:{parsed.port}"
        return host
    return url.split("@")[-1].rstrip("/")


def build_startup_context(
    *,
    edwin_org: Optional[str],
    elastic_url: Optional[str],
    elastic_index: Optional[str],
    elastic_query: str,
    elastic_batch_size: int,
    verify_ssl: bool,
    elastic_pit_keep_alive: str,
    poller_interval: str,
    bookmark_path: str,
    lm_logs_enabled: bool,
    elastic_overlap_ms: int = 0,
) -> Dict[str, Any]:
    """Build a non-sensitive configuration snapshot for startup logging."""
    return {
        "msg": "edwin-elastic-poller started",
        "component": COMPONENT,
        "edwin_org": edwin_org or "",
        "elastic_host": sanitize_elastic_url(elastic_url),
        "elastic_index": elastic_index or "",
        "elastic_query": elastic_query,
        "elastic_batch_size": elastic_batch_size,
        "verify_ssl": verify_ssl,
        "elastic_pit_keep_alive": elastic_pit_keep_alive,
        "poller_interval": poller_interval,
        "bookmark_path": bookmark_path,
        "lm_logs_enabled": lm_logs_enabled,
        "elastic_overlap_ms": elastic_overlap_ms,
        "event_type": "startup",
    }


class LmLogsHandler(logging.Handler):
    """Ship log records to the LogicMonitor Logs ingestion API."""

    def __init__(
        self,
        account: str,
        bearer_token: str,
        *,
        min_level: int = LM_OPERATIONAL_LEVEL,
        timeout: int = 10,
        resource_id: Optional[str] = None,
        queue_size: int = 1000,
    ) -> None:
        super().__init__()
        self.setLevel(min_level)
        self.account = account
        self.bearer_token = bearer_token
        self.resource_id = resource_id
        self.timeout = timeout
        self._queue: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=queue_size)
        self._last_failure_report = 0.0
        self._worker = threading.Thread(
            target=self._run, name="lm-logs-worker", daemon=True
        )
        self._worker.start()
        self._ingest_url = (
            f"https://{account}.logicmonitor.com{LM_LOGS_INGEST_PATH}"
        )

    def emit(self, record: logging.LogRecord) -> None:
        try:
            event = self._build_event(record)
            self._queue.put_nowait(event)
        except queue.Full:
            self._report_failure("LM Logs queue is full; dropping log record")
        except Exception as exc:
            self._report_failure(f"LM Logs ingestion failed: {exc}")

    def _run(self) -> None:
        while True:
            event = self._queue.get()
            try:
                response = requests.post(
                    self._ingest_url,
                    headers={
                        "Authorization": f"Bearer {self.bearer_token}",
                        "Content-Type": "application/json",
                    },
                    json=[event],
                    timeout=self.timeout,
                    verify=verify_edwin_ssl(),
                )
                if response.status_code == 207:
                    self._report_failure(
                        f"LM Logs partially accepted records: {response.text[:2000]}"
                    )
                elif response.status_code != 202:
                    self._report_failure(
                        f"LM Logs ingestion returned HTTP {response.status_code}: "
                        f"{response.text[:2000]}"
                    )
            except Exception as exc:  # pragma: no cover - network failures
                self._report_failure(f"LM Logs ingestion failed: {exc}")
            finally:
                self._queue.task_done()

    def _build_event(self, record: logging.LogRecord) -> Dict[str, Any]:
        event: Dict[str, Any] = {
            "msg": record.getMessage(),
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "component": COMPONENT,
            "log_level": record.levelname,
        }
        if self.resource_id:
            event["_lm.resourceId"] = {"system.deviceId": self.resource_id}

        extra = getattr(record, "lm_context", None)
        if isinstance(extra, dict):
            for key, value in extra.items():
                if key.startswith("_") or key in ("level", "log_level"):
                    continue
                if isinstance(value, bool):
                    event[key] = str(value).lower()
                elif value is None:
                    continue
                else:
                    event[key] = value
        return event

    @staticmethod
    def _report_failure(message: str) -> None:
        now = time.monotonic()
        if now - getattr(LmLogsHandler, "_last_report", 0.0) < 60:
            return
        LmLogsHandler._last_report = now
        sys.stderr.write(f"WARN - {message}\n")


def configure_third_party_loggers(*, debug: bool = False) -> None:
    """Tune SDK loggers so mapping fallbacks stay quiet unless DEBUG is on.

    ``common_event`` and ``edwin_request`` log recoverable mapping misses and
    per-batch HTTP detail at DEBUG. Real failures remain at ERROR.
    """
    sdk_level = logging.DEBUG if debug else logging.ERROR
    delivery_level = logging.DEBUG if debug else logging.WARNING
    levels = {
        "edwin_elastic_poller.sdk.common_event": sdk_level,
        "edwin_elastic_poller.sdk.edwin_request": delivery_level,
    }
    for name in _THIRD_PARTY_LOGGERS:
        logging.getLogger(name).setLevel(levels[name])


def configure_logging(
    *,
    debug: bool = False,
    log_enabled: bool = True,
    lm_logs_enabled: bool = False,
    lm_logs_account: Optional[str] = None,
    lm_logs_bearer_token: Optional[str] = None,
    lm_logs_resource_id: Optional[str] = None,
    lm_logs_verbose: bool = False,
    logger_name: str = "edwin_elastic_poller",
) -> logging.Logger:
    """Configure stderr logging and an optional LM Logs handler.

    ``debug`` controls stderr verbosity only. LM Logs always receives INFO+
    operational records; set ``lm_logs_verbose`` (or DEBUG=true) to also ship
    DEBUG detail to LM Logs.
    """
    # Recover from any prior global logging.disable() in third-party code.
    logging.disable(logging.NOTSET)

    logger = logging.getLogger(logger_name)
    logger.handlers.clear()
    logger.propagate = False

    if not log_enabled:
        logger.setLevel(logging.CRITICAL + 1)
        return logger

    stderr_level = logging.DEBUG if debug else logging.INFO
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(stderr_level)
    stderr_handler.setFormatter(
        logging.Formatter("%(levelname)s - %(message)s")
    )
    logger.addHandler(stderr_handler)
    logger.setLevel(logging.DEBUG)

    if lm_logs_enabled:
        account = lm_logs_account or ""
        token = lm_logs_bearer_token or ""
        if account and token:
            lm_min_level = (
                logging.DEBUG if (debug or lm_logs_verbose) else LM_OPERATIONAL_LEVEL
            )
            logger.addHandler(
                LmLogsHandler(
                    account=account,
                    bearer_token=token,
                    min_level=lm_min_level,
                    resource_id=lm_logs_resource_id or None,
                )
            )
        else:
            sys.stderr.write(
                "WARN - LM_LOGS_ENABLED is true but LM_LOGS_ACCOUNT or "
                "LM_LOGS_BEARER_TOKEN is missing; LM Logs shipping disabled\n"
            )

    configure_third_party_loggers(debug=debug)
    return logger


def operational_log_level() -> int:
    """Return INFO for per-cycle operational summaries (stderr and LM Logs)."""
    return LM_OPERATIONAL_LEVEL


def log_with_context(
    logger: logging.Logger,
    level: int,
    message: str,
    **context: Any,
) -> None:
    """Emit a log record with optional structured metadata for LM Logs."""
    if context:
        logger.log(level, message, extra={"lm_context": context})
    else:
        logger.log(level, message)
