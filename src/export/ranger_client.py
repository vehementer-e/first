"""Client and DTOs for interacting with the Apache Ranger tag & policy API."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass, fields
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.utils.logging import DualLogger

logger = DualLogger(name=__name__)

# --- Exceptions / DTOs --------------------------------------------------------


class RangerClientException(Exception):
    """Raised when the RangerClient cannot complete a request successfully."""


@dataclass
class RangerTag:  # pylint: disable=too-few-public-methods
    """Tag object as returned by the Ranger tags API."""

    id: Optional[int]
    type: Optional[str]
    attributes: Optional[Dict[str, Any]]


@dataclass
class RangerResource:  # pylint: disable=too-few-public-methods
    """Resource object as returned by the Ranger tags API."""

    id: Optional[int]
    serviceName: Optional[str]  # pylint: disable=invalid-name
    resourceSignature: Optional[str]  # pylint: disable=invalid-name
    resourceElements: Optional[Dict[str, Any]]  # pylint: disable=invalid-name


@dataclass
class RangerTagResourceMap:  # pylint: disable=too-few-public-methods
    """Mapping between tags and resources."""

    tagId: Optional[int]  # pylint: disable=invalid-name
    resourceId: Optional[int]  # pylint: disable=invalid-name


@dataclass
class RangerPolicy:  # pylint: disable=too-few-public-methods
    """Policy object returned by the Ranger policy API."""

    id: Optional[int]
    name: Optional[str]
    service: Optional[str]
    resources: Optional[Dict[str, Any]]
    policyItems: Optional[List[Dict[str, Any]]]  # pylint: disable=invalid-name


# --- Client -------------------------------------------------------------------


class RangerClient:  # pylint: disable=too-many-instance-attributes
    """
    Python client for the Apache Ranger REST API.

    Features
    --------
    * Basic authentication (username/password).
    * Optional SSL verification bypass (verify_ssl=False) – not recommended in prod.
    * Automatic retries for idempotent GET requests.

    Endpoints
    ---------
    * /service/tags/tags
    * /service/tags/resources/service/{serviceName}
    * /service/tags/tagresourcemaps
    * /service/public/v2/api/policy
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        ranger_base_url: str,
        service_name: str,
        username: str,
        password: str,
        *,
        verify_ssl: bool = True,
        timeout_sec: float = 20.0,
        retries: int = 3,
        backoff_factor: float = 0.3,
    ) -> None:
        self.base = ranger_base_url.rstrip("/")
        self.service_name = service_name
        self.username = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.timeout = timeout_sec

        self._session = requests.Session()

        # Attach retry logic for idempotent GETs
        retry = Retry(
            total=retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
        )
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        # Pre-calc Authorization header like the Java version
        token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode(
            "ascii",
        )
        self._auth_header = {"Authorization": f"Basic {token}"}

    # ---- Public API ---------------------------------------------------------

    def get_tags(self) -> List[RangerTag]:
        """Retrieve all tags defined in Ranger."""
        url = f"{self.base}/service/tags/tags"
        logger.debug("Requesting tags from %s", url)
        data = self._get_json(url)
        return [
            RangerTag(**_coerce_keys(item, RangerTag))
            for item in data  # type: ignore[arg-type]
        ]

    def get_resources(self) -> List[RangerResource]:
        """Retrieve all resources for the configured service."""
        url = f"{self.base}/service/tags/resources/service/{self.service_name}"
        logger.debug("Requesting resources from %s", url)
        data = self._get_json(url)
        return [
            RangerResource(**_coerce_keys(item, RangerResource))
            for item in data  # type: ignore[arg-type]
        ]

    def get_tag_resource_maps(self) -> List[RangerTagResourceMap]:
        """Retrieve mappings between tags and resources."""
        url = f"{self.base}/service/tags/tagresourcemaps"
        logger.debug("Requesting tag-resource maps from %s", url)
        data = self._get_json(url)
        return [
            RangerTagResourceMap(**_coerce_keys(item, RangerTagResourceMap))
            for item in data  # type: ignore[arg-type]
        ]

    def get_policies(self) -> List[RangerPolicy]:
        """Retrieve all policies for the configured service."""
        url = f"{self.base}/service/public/v2/api/policy"
        logger.debug("Requesting policies from %s", url)
        data = self._get_json(url)
        return [
            RangerPolicy(**_coerce_keys(item, RangerPolicy))
            for item in data  # type: ignore[arg-type]
        ]

    # ---- Internals ---------------------------------------------------------

    def _get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """
        Perform a GET request and return the parsed JSON body.

        Raises
        ------
        RangerClientException
            If the request fails, returns a non-200 status code, or if
            the body cannot be parsed as JSON.
        """
        try:
            resp = self._session.get(
                url,
                headers={
                    **self._auth_header,
                    "Accept": "application/json",
                },
                timeout=self.timeout,
                verify=self.verify_ssl,
                params=params,
            )
        except requests.RequestException as exc:
            # Network / connection / timeout errors
            raise RangerClientException(f"Request to {url!r} failed") from exc

        # Non-200 response → raise
        if resp.status_code != 200:
            try:
                body_text = resp.text
            except UnicodeDecodeError:
                body_text = "<unreadable body>"

            # Optionally truncate to avoid logging megabytes
            snippet = body_text[:500]

            raise RangerClientException(
                f"Unexpected HTTP {resp.status_code} for GET {url!r}. "
                f"Body snippet: {snippet!r}",
            )

        try:
            return resp.json()
        except (json.JSONDecodeError, ValueError) as exc:
            try:
                body_text = resp.text
            except UnicodeDecodeError:
                body_text = "<unreadable body>"

            snippet = body_text[:500]

            raise RangerClientException(
                f"Failed to parse JSON from {url!r}. "
                f"Body snippet: {snippet!r}",
            ) from exc


# --- Helpers -----------------------------------------------------------------


def _coerce_keys(obj: Any, model_type: type) -> Dict[str, Any]:
    """
    Filter a dict down to the keys defined on the given dataclass type.

    This lets us safely do `Model(**_coerce_keys(api_obj, Model))` even if the
    API returns additional fields we don't model locally.
    """
    if not isinstance(obj, dict):
        return {}

    allowed = {f.name for f in fields(model_type)}
    return {key: value for key, value in obj.items() if key in allowed}
