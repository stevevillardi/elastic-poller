"""Environment configuration for edwin-elastic-poller."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

# Exit codes
OK = 0
ERROR_CODE_UNKNOWN = 1
ERROR_CODE_VALIDATION_FAILED = 2
ERROR_CODE_EVENT_MAPPING_FAILED = 3
ERROR_CODE_EVENT_DELIVERY_FAILED = 4
ERROR_CODE_HTTP = 5
ERROR_CODE_UNEXPECTED = 6

EDWIN_CREDENTIAL_VARS = (
    ("EDWIN_ORG", "DEXDA_ORG"),
    ("EDWIN_ID", "DEXDA_ID"),
    ("EDWIN_TOKEN", "DEXDA_TOKEN"),
)

_bootstrapped = False
logger = logging.getLogger("edwin_elastic_poller")

# Populated by reload_settings(); defaults keep type checkers and tests happy.
ELASTIC_USER: Optional[str] = None
ELASTIC_PASS: Optional[str] = None
ELASTIC_TOKEN: Optional[str] = None
ELASTIC_URL: Optional[str] = None
ELASTIC_BATCH_SIZE = 500
ELASTIC_INDEX: Optional[str] = None
ELASTIC_QUERY = "*"
ELASTIC_PIT_KEEP_ALIVE = "5m"
VERIFY_SSL = True
ELASTIC_VERIFY_SSL = True
EDWIN_VERIFY_SSL = True
ELASTIC_OVERLAP_MS = 300000
DEDUPE_MAX_RECORDS = 250000
DEDUPE_MAX_SIZE_MB = 256
PAUSE_INTERVAL = "240"
EDWIN_ORG: Optional[str] = None
EDWIN_ID: Optional[str] = None
EDWIN_TOKEN: Optional[str] = None
DEXDA_ORG: Optional[str] = None
DEXDA_ID: Optional[str] = None
DEXDA_TOKEN: Optional[str] = None
BOOKMARK_PATH: Optional[str] = None
DEBUG = False
LOG_ENABLED = True
LM_LOGS_ENABLED = False
LM_LOGS_VERBOSE = False
LM_LOGS_ACCOUNT: Optional[str] = None
LM_LOGS_BEARER_TOKEN: Optional[str] = None
LM_LOGS_RESOURCE_ID: Optional[str] = None
FAILED_PAYLOAD_PATH: Optional[str] = None
EVENT_MAPPING_FILE: Optional[str] = None


def env_bool(name: str, default: bool = False) -> bool:
    """Return True when an environment variable is set to a truthy string."""
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


def env_int(name: str, default: int) -> int:
    """Read an integer environment value without failing module import."""
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return -1


def getenv_alias(*names: str, default: Optional[str] = None) -> Optional[str]:
    """Return the first non-empty environment variable from *names."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def edwin_org() -> Optional[str]:
    return getenv_alias("EDWIN_ORG", "DEXDA_ORG")


def edwin_client_id() -> Optional[str]:
    return getenv_alias("EDWIN_ID", "DEXDA_ID")


def edwin_client_token() -> Optional[str]:
    return getenv_alias("EDWIN_TOKEN", "DEXDA_TOKEN")


def has_edwin_credentials() -> bool:
    """True when org, client id, and token are available from either naming scheme."""
    return all(
        getenv_alias(edwin_name, legacy_name)
        for edwin_name, legacy_name in EDWIN_CREDENTIAL_VARS
    )


def missing_edwin_credential_names() -> list[str]:
    """Return human-readable names for any missing Edwin credential pair."""
    missing: list[str] = []
    for edwin_name, legacy_name in EDWIN_CREDENTIAL_VARS:
        if not getenv_alias(edwin_name, legacy_name):
            missing.append(f"{edwin_name} or {legacy_name}")
    return missing


def resolve_verify_ssl(*legacy_names: str, default: bool = True) -> bool:
    """Return TLS verification for outbound HTTPS clients.

    ``VERIFY_SSL`` applies to Elasticsearch, Edwin, and LM Logs when set.
    Legacy ``ELASTIC_VERIFY_SSL`` / ``EDWIN_VERIFY_SSL`` are honored only when
    ``VERIFY_SSL`` is unset.
    """
    if os.getenv("VERIFY_SSL") is not None:
        return env_bool("VERIFY_SSL", default=default)
    for legacy_name in legacy_names:
        if os.getenv(legacy_name) is not None:
            return env_bool(legacy_name, default=default)
    return default


def suppress_insecure_request_warnings() -> None:
    """Silence urllib3 warnings when TLS verification is disabled."""
    if ELASTIC_VERIFY_SSL and EDWIN_VERIFY_SSL:
        return
    import requests

    requests.packages.urllib3.disable_warnings(
        requests.packages.urllib3.exceptions.InsecureRequestWarning
    )


def reload_settings() -> None:
    """Refresh module-level settings from the current process environment."""
    global ELASTIC_USER, ELASTIC_PASS, ELASTIC_TOKEN, ELASTIC_URL
    global ELASTIC_BATCH_SIZE, ELASTIC_INDEX, ELASTIC_QUERY, ELASTIC_PIT_KEEP_ALIVE
    global VERIFY_SSL, ELASTIC_VERIFY_SSL, EDWIN_VERIFY_SSL
    global ELASTIC_OVERLAP_MS, DEDUPE_MAX_RECORDS, DEDUPE_MAX_SIZE_MB
    global PAUSE_INTERVAL, EDWIN_ORG, EDWIN_ID, EDWIN_TOKEN
    global DEXDA_ORG, DEXDA_ID, DEXDA_TOKEN
    global BOOKMARK_PATH, DEBUG, LOG_ENABLED
    global LM_LOGS_ENABLED, LM_LOGS_VERBOSE, LM_LOGS_ACCOUNT
    global LM_LOGS_BEARER_TOKEN, LM_LOGS_RESOURCE_ID
    global FAILED_PAYLOAD_PATH, EVENT_MAPPING_FILE

    ELASTIC_USER = os.getenv("ELASTIC_USER")
    ELASTIC_PASS = os.getenv("ELASTIC_PASS")
    ELASTIC_TOKEN = os.getenv("ELASTIC_TOKEN")
    ELASTIC_URL = os.getenv("ELASTIC_URL")
    ELASTIC_BATCH_SIZE = env_int("ELASTIC_BATCH_SIZE", 500)
    ELASTIC_INDEX = os.getenv("ELASTIC_INDEXS")
    ELASTIC_QUERY = os.getenv("ELASTIC_QUERY", "*")
    ELASTIC_PIT_KEEP_ALIVE = os.getenv("ELASTIC_PIT_KEEP_ALIVE", "5m")
    VERIFY_SSL = resolve_verify_ssl("ELASTIC_VERIFY_SSL", "EDWIN_VERIFY_SSL")
    ELASTIC_VERIFY_SSL = resolve_verify_ssl("ELASTIC_VERIFY_SSL")
    EDWIN_VERIFY_SSL = resolve_verify_ssl("EDWIN_VERIFY_SSL")
    ELASTIC_OVERLAP_MS = env_int("ELASTIC_OVERLAP_MS", 300000)
    DEDUPE_MAX_RECORDS = env_int("DEDUPE_MAX_RECORDS", 250000)
    DEDUPE_MAX_SIZE_MB = env_int("DEDUPE_MAX_SIZE_MB", 256)
    PAUSE_INTERVAL = os.getenv("POLLER_INTERVAL", 240)
    EDWIN_ORG = edwin_org()
    EDWIN_ID = edwin_client_id()
    EDWIN_TOKEN = edwin_client_token()
    DEXDA_ORG = EDWIN_ORG
    DEXDA_ID = EDWIN_ID
    DEXDA_TOKEN = EDWIN_TOKEN
    BOOKMARK_PATH = os.getenv("BOOKMARK_PATH")
    DEBUG = env_bool("DEBUG", default=False)
    LOG_ENABLED = env_bool("LOG", default=True)
    LM_LOGS_ENABLED = env_bool("LM_LOGS_ENABLED", default=False)
    LM_LOGS_VERBOSE = env_bool("LM_LOGS_VERBOSE", default=False)
    LM_LOGS_ACCOUNT = os.getenv("LM_LOGS_ACCOUNT") or edwin_org()
    LM_LOGS_BEARER_TOKEN = os.getenv("LM_LOGS_BEARER_TOKEN")
    LM_LOGS_RESOURCE_ID = os.getenv("LM_LOGS_RESOURCE_ID")
    FAILED_PAYLOAD_PATH = os.getenv("FAILED_PAYLOAD_PATH")
    EVENT_MAPPING_FILE = os.getenv("EVENT_MAPPING_FILE")


class ConfigurationError(ValueError):
    """Raised when required runtime configuration is invalid."""


def load_environment(
    *,
    env_file: str | None = None,
    mapping_file: str | None = None,
) -> None:
    """Load dotenv files and refresh module settings.

    When ``env_file`` is set, only that file is loaded. Otherwise python-dotenv
    searches for ``.env`` from the current working directory upward.
    ``mapping_file`` sets ``EVENT_MAPPING_FILE`` after env loading.
    """
    if env_file:
        path = Path(env_file).expanduser()
        if not path.is_file():
            raise ConfigurationError(
                f"Env file does not exist or is not a file: {env_file}"
            )
        load_dotenv(path, override=True)
    else:
        load_dotenv()

    if mapping_file:
        path = Path(mapping_file).expanduser()
        if not path.is_file():
            raise ConfigurationError(
                f"Mapping file does not exist or is not a file: {mapping_file}"
            )
        os.environ["EVENT_MAPPING_FILE"] = str(path)

    reload_settings()


def validate_config() -> None:
    """Validate deployment configuration before entering the poll loop."""
    errors: list[str] = []
    if not ELASTIC_URL:
        errors.append("ELASTIC_URL is required")
    if not ELASTIC_INDEX:
        errors.append("ELASTIC_INDEXS is required")
    if ELASTIC_BATCH_SIZE <= 0:
        errors.append("ELASTIC_BATCH_SIZE must be greater than zero")
    try:
        interval = int(PAUSE_INTERVAL)
        if interval < 0:
            raise ValueError
    except (TypeError, ValueError):
        errors.append("POLLER_INTERVAL must be a non-negative integer")
    if ELASTIC_OVERLAP_MS < 0:
        errors.append("ELASTIC_OVERLAP_MS must be non-negative")
    if DEDUPE_MAX_RECORDS <= 0:
        errors.append("DEDUPE_MAX_RECORDS must be greater than zero")
    if DEDUPE_MAX_SIZE_MB <= 0:
        errors.append("DEDUPE_MAX_SIZE_MB must be greater than zero")
    if ELASTIC_TOKEN and (ELASTIC_USER or ELASTIC_PASS):
        errors.append("Use ELASTIC_TOKEN or ELASTIC_USER/ELASTIC_PASS, not both")
    if not ELASTIC_TOKEN and bool(ELASTIC_USER) != bool(ELASTIC_PASS):
        errors.append("ELASTIC_USER and ELASTIC_PASS must be provided together")
    if not has_edwin_credentials():
        errors.append(
            "Missing Edwin credentials: " + ", ".join(missing_edwin_credential_names())
        )
    if LM_LOGS_ENABLED and not (LM_LOGS_ACCOUNT and LM_LOGS_BEARER_TOKEN):
        errors.append(
            "LM_LOGS_ACCOUNT and LM_LOGS_BEARER_TOKEN are required when "
            "LM_LOGS_ENABLED is true"
        )
    if EVENT_MAPPING_FILE:
        mapping_path = Path(EVENT_MAPPING_FILE).expanduser()
        if not mapping_path.is_file():
            errors.append(
                f"EVENT_MAPPING_FILE does not exist or is not a file: "
                f"{EVENT_MAPPING_FILE}"
            )
    if errors:
        raise ConfigurationError("; ".join(errors))


def bootstrap() -> None:
    """Configure stderr and optional LM Logs handlers once at startup."""
    global _bootstrapped, logger
    if _bootstrapped:
        return

    from edwin_elastic_poller.observability import lm_logs

    suppress_insecure_request_warnings()
    logger = lm_logs.configure_logging(
        debug=DEBUG,
        log_enabled=LOG_ENABLED,
        lm_logs_enabled=LM_LOGS_ENABLED,
        lm_logs_account=LM_LOGS_ACCOUNT,
        lm_logs_bearer_token=LM_LOGS_BEARER_TOKEN,
        lm_logs_resource_id=LM_LOGS_RESOURCE_ID,
        lm_logs_verbose=LM_LOGS_VERBOSE,
    )
    _bootstrapped = True


load_environment()
