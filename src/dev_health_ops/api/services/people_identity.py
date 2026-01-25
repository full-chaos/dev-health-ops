from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import yaml


_ALIAS_CACHE: Optional[Dict[str, List[str]]] = None


def _norm_key(value: str) -> str:
    return " ".join((value or "").strip().lower().split())


def _norm_email(email: str) -> str:
    return (email or "").strip().lower()


def load_identity_aliases() -> Dict[str, List[str]]:
    global _ALIAS_CACHE
    if _ALIAS_CACHE is not None:
        return _ALIAS_CACHE

    raw_path = os.getenv("IDENTITY_MAPPING_PATH")
    path = (
        Path(raw_path)
        if raw_path
        else Path("src/dev_health_ops/config/identity_mapping.yaml")
    )

    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    except FileNotFoundError:
        payload = {}

    aliases: Dict[str, List[str]] = {}
    for entry in payload.get("identities") or []:
        canonical = entry.get("canonical")
        if not canonical:
            continue
        canonical_norm = _norm_email(str(canonical)) or str(canonical).strip()
        if not canonical_norm:
            continue
        aliases.setdefault(canonical_norm, [])
        for alias in entry.get("aliases") or []:
            alias_str = str(alias).strip()
            if alias_str:
                aliases[canonical_norm].append(alias_str)

    _ALIAS_CACHE = aliases
    return aliases


def person_id_for_identity(identity: str) -> str:
    digest = hashlib.md5(identity.encode("utf-8")).hexdigest()
    return digest.lower()


def display_name_for_identity(identity: str) -> str:
    if "@" in identity:
        local = identity.split("@", 1)[0]
        return local.replace(".", " ").replace("_", " ").title()
    if ":" in identity:
        return identity.split(":", 1)[1] or identity
    return identity


def parse_identity(identity: str) -> Tuple[str, str]:
    if "@" in identity:
        return "email", identity
    if ":" in identity:
        provider, handle = identity.split(":", 1)
        return provider or "identity", handle or identity
    return "identity", identity


def identities_for_person(
    identity: str, aliases: Iterable[str]
) -> List[Dict[str, str]]:
    seen = set()
    results: List[Dict[str, str]] = []
    for item in [identity, *aliases]:
        if not item:
            continue
        provider, handle = parse_identity(item)
        key = f"{provider}:{handle}"
        if key in seen:
            continue
        seen.add(key)
        results.append({"provider": provider, "handle": handle})
    return results


def identity_variants(identity: str, aliases: Iterable[str]) -> List[str]:
    variants = {identity}
    for alias in aliases:
        if alias:
            variants.add(alias)
    if ":" in identity:
        _, handle = identity.split(":", 1)
        if handle:
            variants.add(handle)
    if "@" in identity:
        local = identity.split("@", 1)[0]
        if local:
            variants.add(local)
        variants.add(_norm_email(identity))
    return [v for v in variants if v]
