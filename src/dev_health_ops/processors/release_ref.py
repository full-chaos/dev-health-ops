from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ReleaseRefEnrichment:
    release_ref: str
    confidence: float
    source: str


def enrich_release_ref(
    deployment: Any,
    provider: str,
    *,
    releases: list[Any] | None = None,
) -> str:
    return get_release_ref_enrichment(
        deployment,
        provider,
        releases=releases,
    ).release_ref


def get_release_ref_enrichment(
    deployment: Any,
    provider: str,
    *,
    releases: list[Any] | None = None,
) -> ReleaseRefEnrichment:
    normalized_provider = provider.lower().strip()
    explicit_ref = _extract_explicit_release_ref(deployment)
    if explicit_ref:
        return ReleaseRefEnrichment(
            release_ref=explicit_ref,
            confidence=_extract_explicit_confidence(deployment),
            source="explicit",
        )

    if normalized_provider == "github":
        tag = _resolve_github_tag(deployment, releases or [])
        if tag:
            return ReleaseRefEnrichment(tag, 1.0, "github_release")
        fallback = _string_value(deployment, "deployment_id") or _string_value(
            deployment, "id"
        )
        return ReleaseRefEnrichment(fallback, 0.3, "deployment_id_fallback")

    if normalized_provider == "gitlab":
        tag = _resolve_gitlab_tag(deployment, releases or [])
        if tag:
            return ReleaseRefEnrichment(tag, 1.0, "gitlab_release")
        fallback = (
            _string_value(deployment, "deployment_iid")
            or _string_value(deployment, "iid")
            or _string_value(deployment, "deployment_id")
            or _string_value(deployment, "id")
        )
        return ReleaseRefEnrichment(fallback, 0.3, "deployment_iid_fallback")

    fallback = (
        _string_value(deployment, "deployment_id")
        or _string_value(deployment, "id")
        or ""
    )
    return ReleaseRefEnrichment(fallback, 0.3, "generic_fallback")


def _extract_explicit_release_ref(deployment: Any) -> str:
    for key in (
        "release_ref",
        "release",
        "release_tag",
        "tag_name",
        "tag",
        "version",
    ):
        value = _string_value(deployment, key)
        if value:
            return value

    payload = _mapping_value(deployment, "payload")
    if payload:
        for key in (
            "release_ref",
            "release",
            "release_tag",
            "tag_name",
            "tag",
            "version",
        ):
            value = _string_value(payload, key)
            if value:
                return value

    return ""


def _extract_explicit_confidence(deployment: Any) -> float:
    confidence = _raw_value(deployment, "release_ref_confidence")
    if confidence is None:
        payload = _mapping_value(deployment, "payload")
        confidence = _raw_value(payload, "release_ref_confidence")
    try:
        if confidence is not None:
            return max(0.0, min(1.0, float(confidence)))
    except (TypeError, ValueError):
        # Invalid confidence values are treated as unknown and fall back to the default below.
        confidence = None
    return 1.0


def _resolve_github_tag(deployment: Any, releases: list[Any]) -> str:
    candidates = {
        _string_value(deployment, "ref"),
        _string_value(deployment, "tag"),
        _string_value(deployment, "tag_name"),
    }
    payload = _mapping_value(deployment, "payload")
    candidates.update(
        {
            _string_value(payload, "ref"),
            _string_value(payload, "tag"),
            _string_value(payload, "tag_name"),
            _string_value(payload, "release_tag"),
        }
    )
    clean_candidates = {candidate for candidate in candidates if candidate}
    if not clean_candidates:
        return ""
    for release in releases:
        tag_name = _string_value(release, "tag_name")
        if tag_name and tag_name in clean_candidates:
            return tag_name
    return ""


def _resolve_gitlab_tag(deployment: Any, releases: list[Any]) -> str:
    candidates = {
        _string_value(deployment, "ref"),
        _string_value(deployment, "tag"),
        _string_value(deployment, "tag_name"),
    }
    clean_candidates = {candidate for candidate in candidates if candidate}
    if not clean_candidates:
        return ""
    for release in releases:
        tag_name = _string_value(release, "tag_name")
        if tag_name and tag_name in clean_candidates:
            return tag_name
    return ""


def _mapping_value(value: Any, key: str) -> Mapping[str, Any] | None:
    raw = _raw_value(value, key)
    return raw if isinstance(raw, Mapping) else None


def _string_value(value: Any, key: str) -> str:
    raw = _raw_value(value, key)
    if raw is None:
        return ""
    text = str(raw).strip()
    return text


def _raw_value(value: Any, key: str) -> Any:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return value.get(key)
    if hasattr(value, key):
        return getattr(value, key)
    raw_data = getattr(value, "raw_data", None)
    if isinstance(raw_data, Mapping):
        return raw_data.get(key)
    return None
