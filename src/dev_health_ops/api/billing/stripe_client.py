"""Stripe client helpers for billing endpoints.

Provides a lazily-initialized ``StripeClient`` instance and helpers to map
Stripe price IDs to ``LicenseTier`` values.
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache

from stripe import StripeClient

from dev_health_ops.licensing.types import LicenseTier

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_stripe_client() -> StripeClient:
    """Return a lazily-initialized ``StripeClient``.

    Raises ``RuntimeError`` if ``STRIPE_SECRET_KEY`` is not set.
    """
    secret_key = os.getenv("STRIPE_SECRET_KEY")
    if not secret_key:
        raise RuntimeError("STRIPE_SECRET_KEY environment variable is not set")
    return StripeClient(secret_key)


def get_webhook_secret() -> str:
    """Return the Stripe webhook signing secret.

    Raises ``RuntimeError`` if ``STRIPE_WEBHOOK_SECRET`` is not set.
    """
    secret = os.getenv("STRIPE_WEBHOOK_SECRET")
    if not secret:
        raise RuntimeError("STRIPE_WEBHOOK_SECRET environment variable is not set")
    return secret


def get_private_key() -> str:
    """Return the Ed25519 private key for license signing.

    Raises ``RuntimeError`` if ``LICENSE_PRIVATE_KEY`` is not set.
    """
    key = os.getenv("LICENSE_PRIVATE_KEY")
    if not key:
        raise RuntimeError("LICENSE_PRIVATE_KEY environment variable is not set")
    return key


_PRICE_TIER_MAP: dict[str, LicenseTier] | None = None


def _build_price_tier_map() -> dict[str, LicenseTier]:
    """Build a mapping from Stripe price ID to LicenseTier.

    Reads ``STRIPE_PRICE_ID_TEAM`` and ``STRIPE_PRICE_ID_ENTERPRISE``
    from the environment.  Missing values are silently skipped.
    """
    global _PRICE_TIER_MAP
    if _PRICE_TIER_MAP is not None:
        return _PRICE_TIER_MAP

    mapping: dict[str, LicenseTier] = {}
    team_price = os.getenv("STRIPE_PRICE_ID_TEAM")
    enterprise_price = os.getenv("STRIPE_PRICE_ID_ENTERPRISE")

    if team_price:
        mapping[team_price] = LicenseTier.TEAM
    if enterprise_price:
        mapping[enterprise_price] = LicenseTier.ENTERPRISE

    _PRICE_TIER_MAP = mapping
    return mapping


def map_price_id_to_tier(price_id: str) -> LicenseTier | None:
    """Map a Stripe price ID to a ``LicenseTier``.

    Returns ``None`` if the price ID is not recognized.
    """
    return _build_price_tier_map().get(price_id)


def get_tier_from_line_items(line_items: list[dict]) -> LicenseTier:
    """Extract the ``LicenseTier`` from Stripe checkout line items.

    Iterates items looking for the first recognized price ID.
    Falls back to ``LicenseTier.TEAM`` if nothing matches.
    """
    for item in line_items:
        price = item.get("price", {})
        price_id = price.get("id") if isinstance(price, dict) else None
        if price_id:
            tier = map_price_id_to_tier(price_id)
            if tier is not None:
                return tier

    logger.warning("No recognized price ID in line items, defaulting to TEAM")
    return LicenseTier.TEAM


def get_tier_price_id(tier: LicenseTier) -> str | None:
    """Return the Stripe price ID for a given tier.

    Returns ``None`` if the tier has no configured price.
    """
    mapping = _build_price_tier_map()
    for price_id, mapped_tier in mapping.items():
        if mapped_tier == tier:
            return price_id
    return None


def reset_price_tier_map() -> None:
    """Reset the cached price-to-tier mapping (for testing)."""
    global _PRICE_TIER_MAP
    _PRICE_TIER_MAP = None
