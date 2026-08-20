# SPDX-FileCopyrightText: 2025 LogicMonitor, Inc.
#
# SPDX-License-Identifier: LicenseRef-All-rights-reserved

"""HTTP client for Edwin OAuth and event ingestion."""
import json
import logging
import os
import pathlib
import time
import typing
from datetime import datetime
from itertools import islice
from typing import Any, Dict, Iterable, List, Union

import dotenv
import pydantic
import requests
import yaml

from edwin_elastic_poller import config

_logger = logging.getLogger("edwin_elastic_poller.sdk.edwin_request")


def _normalize_auth_dict(auth_dict: dict[str, str]) -> dict[str, str]:
    """Accept legacy ``dexda_org`` keys in auth payloads."""
    normalized = dict(auth_dict)
    if "edwin_org" not in normalized and "dexda_org" in normalized:
        normalized["edwin_org"] = normalized["dexda_org"]
    return normalized


class _EdwinAuth(pydantic.BaseModel, extra="forbid", strict=True):
    """Pydantic model for validating user-supplied Edwin auth config."""

    edwin_org: str
    client_id: str
    client_secret: str


class _EdwinAuthToken(typing.TypedDict):
    """Edwin OAuth token response."""

    access_token: str
    issued_token_type: str
    token_type: str
    expires_in: int
    expires_at: int
    now: int


class EdwinRequest:
    """Send CEF event batches to Edwin."""

    _FILE_DIR: str = "src/logicmonitor/edwin/common_event_integration_sdk/"

    _HEADERS: Dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    @classmethod
    def new_from_file(
        cls,
        auth_file_name: str,
        auth_file_path: typing.Optional[str] = None,
    ) -> "EdwinRequest":
        """Start a new EdwinRequest using YAML auth config files."""
        _file_path = (
            auth_file_path if auth_file_path is not None else cls._FILE_DIR
        )
        _afp = pathlib.Path(_file_path).joinpath(auth_file_name)
        try:
            _auth_yaml: dict = yaml.safe_load(_afp.read_text(encoding="utf-8"))
        except FileNotFoundError as e:
            raise FileNotFoundError(
                f"Unable to find auth config file\n"
                f'File name: "{auth_file_name}"\n'
                f'Path: "{_afp}"'
            ) from e
        _logger.debug(
            "Auth read from file: %s\nFull path: %s", auth_file_name, _afp
        )
        return cls.new_from_param(_auth_yaml, "FILE")

    @classmethod
    def new_from_env(cls) -> "EdwinRequest":
        """Start a new EdwinRequest using environment variables."""
        dotenv.load_dotenv()
        from edwin_elastic_poller.config import (
            edwin_client_id,
            edwin_client_token,
            edwin_org,
        )

        auth_dict: dict = {
            "edwin_org": edwin_org(),
            "client_id": edwin_client_id(),
            "client_secret": edwin_client_token(),
        }
        return cls.new_from_param(auth_dict, ".ENV")

    @classmethod
    def new_from_param(
        cls,
        auth_dict: dict[str, str],
        init_type: typing.Union[str, None] = None,
    ) -> "EdwinRequest":
        """Start a new EdwinRequest from an auth dict (org, client id, secret)."""
        auth_model: "_EdwinAuth" = _EdwinAuth.model_validate(
            obj=_normalize_auth_dict(auth_dict)
        )
        if init_type is None:
            init_type = "PARAM"
        return cls(auth_model, init_type)

    def __init__(self, auth_data: "_EdwinAuth", init_type: str) -> None:
        _logger.debug("init type: %s", init_type)
        self._client_data = {
            "client_id": auth_data.client_id,
            "client_secret": auth_data.client_secret,
        }
        self.portal_url = f"https://{auth_data.edwin_org}.dexda.ai"
        self._token_endpoint = f"{self.portal_url}/auth/token"
        self._data_endpoint = f"{self.portal_url}/integration/event/v1"
        self.access_token = self.retrieve_access_token()

    def retrieve_access_token(self) -> "_EdwinAuthToken":
        """Exchange client credentials for an Edwin access token."""
        try:
            response = requests.post(
                url=self._token_endpoint,
                data={"grant_type": "client_credentials", **self._client_data},
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                timeout=30,
                verify=config.EDWIN_VERIFY_SSL,
            )
            response.raise_for_status()
            if response.status_code != 200:
                raise requests.exceptions.RequestException
            return typing.cast(_EdwinAuthToken, response.json())
        except requests.exceptions.RequestException:
            _logger.exception(
                "Edwin OAuth request failed endpoint=%s", self._token_endpoint
            )
            raise

    def batched(self, iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
        """Yield successive `size`-item lists from *iterable*."""
        it = iter(iterable)
        while True:
            chunk = list(islice(it, size))
            if not chunk:
                break
            yield chunk

    def writePayload(self, data: str) -> None:
        """Persist a rejected payload when explicitly enabled."""
        directory = os.getenv("FAILED_PAYLOAD_PATH", "")
        if not directory:
            _logger.warning(
                "Edwin rejected a batch; failed payload persistence is disabled"
            )
            return
        os.makedirs(directory, mode=0o700, exist_ok=True)
        events = json.dumps(data, indent=4)
        timestamp = int(datetime.now().timestamp() * 1000)
        with open(
            os.path.join(directory, f"{timestamp}.json"), "w", encoding="utf-8"
        ) as fh:
            fh.write(events)

    def send(
        self,
        access_token: str,
        data: List[Dict[str, Union[str, int, Dict[str, str]]]],
    ) -> bool:
        """Send CEF event batches to Edwin."""
        batchcount = 100
        totalCount = 0
        all_succeeded = True
        for batch in self.batched(data, batchcount):
            totalCount = totalCount + len(batch)
            _logger.debug(
                "Sending Edwin batch batch_end=%s batch_size=%s",
                totalCount,
                len(batch),
            )

            auth_header = {
                "Authorization": f"Bearer {access_token.get('access_token')}"
            }
            headers = {**self._HEADERS, **auth_header}

            retry_max = 3
            retry_backoff = 5
            batch_succeeded = False
            response = None
            for attempt in range(retry_max):
                try:
                    response = requests.post(
                        url=self._data_endpoint,
                        data=json.dumps(batch),
                        headers=headers,
                        timeout=360,
                        verify=config.EDWIN_VERIFY_SSL,
                    )
                    response.raise_for_status()
                    _logger.info(
                        "Edwin batch accepted status=%s batch_size=%s",
                        response.status_code,
                        len(batch),
                    )
                    batch_succeeded = True
                    break
                except requests.exceptions.RequestException:
                    if response is not None and response.status_code == 422:
                        _logger.error(
                            "Edwin rejected batch status=422 response=%s",
                            response.text[:2000],
                        )
                        raise ValueError(response.text[:2000]) from None
                    if response is not None and 400 <= response.status_code < 500:
                        self.writePayload(batch)
                        _logger.error(
                            "Edwin rejected batch status=%s response=%s",
                            response.status_code,
                            response.text[:2000],
                        )
                        all_succeeded = False
                        break
                    _logger.warning(
                        "Edwin batch attempt failed attempt=%s/%s status=%s",
                        attempt + 1,
                        retry_max,
                        response.status_code if response is not None else None,
                    )
                    time.sleep(retry_backoff * (attempt + 1))

            if not batch_succeeded:
                all_succeeded = False
                if response is None or not (400 <= response.status_code < 500):
                    logging.error("Maximum retries exhausted for batch")
        return all_succeeded
