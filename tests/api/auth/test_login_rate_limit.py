"""CHAOS-1772: backend slowapi must not rate-limit successful logins.

Before the fix, /api/v1/auth/login had two slowapi decorators:
  - @limiter.limit("20/15minutes")                   (per-IP, OK)
  - @limiter.limit("5/15minutes", key_func=ip:email) (per-email, the bug)

The per-email decorator counted EVERY request (success + failure) so 5
successful logins in 15 minutes returned 429 to legitimate users. The DB
`login_attempts` table already provides per-account failed-attempt protection.

This test ensures only the per-IP slowapi limit is applied, and that the
removed constants/imports stay removed.
"""

from __future__ import annotations


def test_auth_login_constant_removed() -> None:
    """`AUTH_LOGIN_LIMIT` was removed in CHAOS-1772; only the per-IP limit remains."""
    from dev_health_ops.api.middleware import rate_limit

    assert not hasattr(rate_limit, "AUTH_LOGIN_LIMIT"), (
        "AUTH_LOGIN_LIMIT should be removed — it counted successful logins. "
        "DB login_attempts table (LOCKOUT_FAILURE_THRESHOLD=5) handles per-account "
        "credential-stuffing protection."
    )

    # The per-IP limit must still exist as a coarse brute-force guard.
    assert rate_limit.AUTH_LOGIN_IP_LIMIT == "20/15minutes"


def test_get_auth_key_still_exported_for_other_routes() -> None:
    """`get_auth_key` is used by password_reset.py and verify.py — must remain exported."""
    from dev_health_ops.api.middleware.rate_limit import get_auth_key

    assert callable(get_auth_key)


def test_login_module_uses_only_ip_rate_limit() -> None:
    """Source-level check: only the per-IP @limiter.limit decorator remains.

    Inspecting decorators via runtime reflection is unreliable because slowapi
    wraps the handler. Source inspection is the deterministic alternative.
    """
    import inspect

    from dev_health_ops.api.auth.routers import login as login_module

    source = inspect.getsource(login_module)
    # The per-IP decorator must remain.
    assert "@limiter.limit(AUTH_LOGIN_IP_LIMIT)" in source, (
        "Per-IP rate limit (20/15minutes) must remain as a brute-force guard."
    )
    # The per-email decorator must NOT come back.
    assert "AUTH_LOGIN_LIMIT" not in source, (
        "AUTH_LOGIN_LIMIT decorator/import must not be reintroduced (CHAOS-1772)."
    )
    assert "key_func=get_auth_key" not in source, (
        "Per-email key function must not be applied to /login (CHAOS-1772)."
    )
