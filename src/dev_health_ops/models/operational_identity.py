"""Canonical provider coordinates for operational entity identities."""

from __future__ import annotations

from dataclasses import dataclass
from ipaddress import ip_address
from urllib.parse import urlsplit

from dev_health_ops.models.operational import CanonicalOperationalEntity


@dataclass(frozen=True, slots=True)
class OperationalSourceCoordinates:
    """The immutable source coordinates used by canonical operational ids."""

    provider: str
    provider_instance_id: str
    entity_family: str
    external_id: str


class InvalidOperationalProviderInstanceError(ValueError):
    pass


def normalized_operational_provider_instance(
    provider: str, provider_instance_id: str
) -> str | None:
    """Normalize an operational provider host without repository scope."""
    raw_instance = provider_instance_id.strip()
    if provider not in {"github", "gitlab"}:
        return raw_instance.casefold()
    try:
        has_scheme = "://" in raw_instance
        parsed = urlsplit(raw_instance if has_scheme else f"//{raw_instance}")
        port = parsed.port
    except ValueError:
        return None
    host = parsed.hostname
    if (
        host is None
        or host.casefold() in {"none", "null"}
        or (not has_scheme and parsed.path)
    ):
        return None
    try:
        ip_address(host)
    except ValueError:
        labels = host.split(".")
        if not all(
            label
            and label[0].isalnum()
            and label[-1].isalnum()
            and all(character.isalnum() or character == "-" for character in label)
            for label in labels
        ):
            return None
    normalized_host = host.casefold()
    if provider == "github" and normalized_host in {"api.github.com", "github.com"}:
        return "github.com"
    scheme = parsed.scheme.casefold() or "https"
    default_port = 443 if scheme == "https" else 80 if scheme == "http" else None
    port_suffix = f":{port}" if port is not None and port != default_port else ""
    return f"{normalized_host}{port_suffix}"


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

    Provider-global issue ids come from native producers and external push. The
    entity family always comes from the canonical dataclass.
    """
    normalized_provider = provider.strip().casefold()
    normalized_external_id = external_id.strip()
    normalized_instance = normalized_operational_provider_instance(
        normalized_provider, provider_instance_id
    )
    if normalized_instance is None:
        raise InvalidOperationalProviderInstanceError(
            f"Invalid {normalized_provider} provider instance"
        )
    return OperationalSourceCoordinates(
        provider=normalized_provider,
        provider_instance_id=normalized_instance,
        entity_family=entity_type.entity_family,
        external_id=normalized_external_id,
    )
