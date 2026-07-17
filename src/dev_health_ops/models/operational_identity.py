"""Canonical provider coordinates for operational entity identities."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from dev_health_ops.models.operational import CanonicalOperationalEntity


@dataclass(frozen=True, slots=True)
class OperationalSourceCoordinates:
    """The immutable source coordinates used by canonical operational ids."""

    provider: str
    provider_instance_id: str
    entity_family: str
    external_id: str


def _normalized_instance(provider: str, provider_instance_id: str) -> str:
    """Normalize account or host identity without admitting repository scope."""
    raw_instance = provider_instance_id.strip()
    if provider not in {"github", "gitlab", "atlassian"}:
        return raw_instance.casefold()
    parsed = urlsplit(raw_instance if "://" in raw_instance else f"//{raw_instance}")
    host = parsed.hostname
    if host is None:
        return raw_instance.casefold().rstrip("/")
    port = f":{parsed.port}" if parsed.port is not None else ""
    return f"{host.casefold()}{port}"


def operational_source_coordinates(
    entity_type: type[CanonicalOperationalEntity],
    *,
    provider: str,
    provider_instance_id: str,
    external_id: str,
    repo_full_name: str | None = None,
    issue_number: str | None = None,
) -> OperationalSourceCoordinates:
    """Return the only allowed source coordinates for an operational entity.

    Issue-derived incidents are unique per repository, rather than per host-wide
    issue number. The entity family always comes from the canonical dataclass.
    """
    normalized_provider = provider.strip().casefold()
    normalized_external_id = external_id.strip()
    if entity_type.entity_family == "operational_incident" and repo_full_name:
        number = (issue_number or normalized_external_id).strip()
        normalized_external_id = f"{repo_full_name}#{number}"
    return OperationalSourceCoordinates(
        provider=normalized_provider,
        provider_instance_id=_normalized_instance(
            normalized_provider, provider_instance_id
        ),
        entity_family=entity_type.entity_family,
        external_id=normalized_external_id,
    )
