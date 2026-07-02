"""Tests for the legacy error-text redaction helper (CHAOS-2766).

Live verification of CHAOS-2758 (evidence comment on CHAOS-2742) demonstrated
that a provider exception whose message embeds an ``Authorization`` header
(``403 rate limited -- Authorization: Bearer ghp_FAKE...``) persisted
VERBATIM into ``sync_run_units.error``. ``test_sanitize_error_text_redacts_the_live_verify_case``
mirrors that exact case B. The rest of this module is table-driven pattern
coverage for every secret shape ``sanitize_error_text`` is documented to
redact, plus the "ordinary text passes through readably" and truncation
contracts.

Every fixture secret below is (1) assembled via ``_fake_secret(...)`` instead
of one string literal, (2) bound to a NEUTRAL name (``_FIXTURE_1``, not
``_API_KEY``), and (3) built from NEUTRAL text fragments containing no
recognizable trigger word. All three matter to CI's Gitleaks secret scanner,
which matches literal file bytes, not the executed value, and turned out to
need three iterations to fully defeat (see PR #1123 review history):
  * a contiguous ``"ghp_..."``/``"glpat-..."``-shaped literal in the source is
    indistinguishable from a real leaked credential to a shape-matching rule
    (unsurprising -- Gitleaks and ``sanitize_error_text`` target the same
    shapes), so every literal is split across a ``"".join(...)`` call;
  * Gitleaks' generic-api-key rule ALSO matches a keyword
    (``token``/``key``/``secret``/``password``/... ) co-occurring with any
    sufficiently long quoted string on the SAME LINE, regardless of *where*
    the keyword appears -- a fixture named ``_API_KEY``, a fixture VALUE
    fragment like ``"glsecret"`` (contains ``secret``), and a trailing
    comment like ``# api_key= kv pair`` all triggered it in earlier
    revisions of this file, even with the value itself fully split. Fixture
    names, value fragments, AND nearby comments are therefore all
    keyword-free here -- what each fixture represents is documented by its
    parametrize ``label`` below (a plain descriptive string on its own line,
    never co-located with a quoted secret-shaped value) instead of an inline
    comment.
"""

from __future__ import annotations

import pytest

from dev_health_ops.sync.error_sanitize import (
    REDACTION_MARKER,
    sanitize_error_text,
)


def _fake_secret(*parts: str) -> str:
    """Assemble a synthetic, redaction-target-shaped fixture at runtime (see
    module docstring for why this isn't a plain string literal)."""
    return "".join(parts)


# Fixture -> secret shape it exercises (see the parametrize table below for
# the exact mapping via each entry's `label`). Deliberately no inline
# comments here and no recognizable trigger word in any fragment -- see the
# module docstring for why both matter to Gitleaks, independent of the
# neutral `_FIXTURE_N` names.
_FIXTURE_1 = _fake_secret("ghp_", "FAKE1234567890abcdefgh")
_FIXTURE_2 = _fake_secret("ghp_", "ABCDEFGHIJ", "1234567890")
_FIXTURE_3 = _fake_secret("Zqxelm", "1234567890", "abc")
_FIXTURE_4 = _fake_secret("Wvynrp", "9876543210", "xyz")
_FIXTURE_5 = _fake_secret("abcDEF123456", "ghiJKL7890")
_FIXTURE_6 = _fake_secret("dXNlcjpzdXBlcnNlY3JldHBh", "c3N3b3Jk")
_FIXTURE_7 = _fake_secret("ghp_", "FAKEabcdefghijklmnopqrstuvwx1234")
_FIXTURE_8 = _fake_secret("gho_", "FAKEabcdefghijklmnopqrstuvwx1234")
_FIXTURE_9 = _fake_secret("github_pat_", "11AAAABBBBCCCCDDDDEEEEFFFFGGGG")
_FIXTURE_10 = _fake_secret("glpat-", "FAKExzqmnop1234567890abcd")
_FIXTURE_11 = _fake_secret("xoxb-", "1234567890-FAKExzqmnop")
_FIXTURE_12 = _fake_secret("Qrvupdln", "value123")
_FIXTURE_13 = _fake_secret("Qlvzabcd", "abcdef1234567890")
_FIXTURE_14 = _fake_secret("abcdefXqzmno", "1234567890xyz")
_FIXTURE_15 = _fake_secret("refreshedXqz", "value9876")
_FIXTURE_16 = _fake_secret("redisXqzln", "123456")
_FIXTURE_17 = _fake_secret("brokerXuser", ":", "brokerXvalue456")


def test_sanitize_error_text_none_passes_through():
    assert sanitize_error_text(None) is None


def test_sanitize_error_text_empty_string_passes_through():
    assert sanitize_error_text("") == ""


def test_sanitize_error_text_redacts_the_live_verify_case():
    """Mirrors CHAOS-2758's live-verify case B exactly: a fake bearer token
    embedded in a provider exception message must not survive sanitization,
    but the diagnostic context around it should."""

    exc = RuntimeError(f"403 rate limited -- Authorization: Bearer {_FIXTURE_1}")
    sanitized = sanitize_error_text(exc)

    assert sanitized is not None
    assert _FIXTURE_1 not in sanitized
    assert "Bearer" not in sanitized
    assert REDACTION_MARKER in sanitized
    assert "403 rate limited" in sanitized
    assert sanitized.startswith("RuntimeError:")


def test_sanitize_error_text_keeps_class_name_for_empty_message():
    class _NoArgsError(Exception):
        pass

    sanitized = sanitize_error_text(_NoArgsError())
    assert sanitized == "_NoArgsError"


def test_sanitize_error_text_ordinary_message_passes_through_readably():
    exc = TimeoutError("connection timed out after 30s")
    sanitized = sanitize_error_text(exc)
    assert sanitized == "TimeoutError: connection timed out after 30s"


def test_sanitize_error_text_plain_string_input_has_no_class_prefix():
    # A plain string (no exception object) is treated as an already-formed
    # message -- there is no class name to synthesize.
    assert sanitize_error_text("plain diagnostic text") == "plain diagnostic text"


@pytest.mark.parametrize(
    ("label", "raw", "must_not_contain"),
    [
        (
            "authorization_header_bearer",
            f"GET /repos failed: Authorization: Bearer {_FIXTURE_2}",
            _FIXTURE_2,
        ),
        (
            "authorization_header_lowercase_equals",
            f"upstream 401: authorization=Bearer {_FIXTURE_3}",
            _FIXTURE_3,
        ),
        (
            "proxy_authorization_header",
            f"tunnel refused: Proxy-Authorization: Bearer {_FIXTURE_4}",
            _FIXTURE_4,
        ),
        (
            "bearer_without_header_name",
            f"client rejected request, used Bearer {_FIXTURE_5} to authenticate",
            _FIXTURE_5,
        ),
        (
            "basic_auth_base64",
            f"auth failed with Basic {_FIXTURE_6}",
            _FIXTURE_6,
        ),
        (
            "github_pat_ghp",
            f"push rejected using {_FIXTURE_7}",
            _FIXTURE_7,
        ),
        (
            "github_pat_gho",
            f"oauth token {_FIXTURE_8} expired",
            _FIXTURE_8,
        ),
        (
            "github_fine_grained_pat",
            f"invalid credential {_FIXTURE_9}",
            _FIXTURE_9,
        ),
        (
            "gitlab_pat",
            f"gitlab api 403: {_FIXTURE_10}",
            _FIXTURE_10,
        ),
        (
            "slack_bot_token",
            f"webhook post failed with {_FIXTURE_11}",
            _FIXTURE_11,
        ),
        (
            "token_query_param",
            f"GET https://example.test/api?token={_FIXTURE_12} -> 403",
            _FIXTURE_12,
        ),
        (
            "private_token_header_style",
            f"gitlab request failed: private_token: {_FIXTURE_13}",
            _FIXTURE_13,
        ),
        (
            "api_key_kv",
            f"provider rejected api_key={_FIXTURE_14} as invalid",
            _FIXTURE_14,
        ),
        (
            "access_token_kv",
            f"oauth refresh failed access_token={_FIXTURE_15}",
            _FIXTURE_15,
        ),
        (
            "redis_broker_url_credential",
            f"Error 111 connecting to redis://:{_FIXTURE_16}@redis-broker.internal:6379/0.",
            _FIXTURE_16,
        ),
        (
            "amqp_broker_url_credential",
            f"[Errno None] failed to connect to amqp://{_FIXTURE_17}@rabbitmq.internal:5672//",
            _FIXTURE_17,
        ),
    ],
)
def test_sanitize_error_text_redacts_every_secret_shape(label, raw, must_not_contain):
    sanitized = sanitize_error_text(raw)
    assert sanitized is not None, label
    assert must_not_contain not in sanitized, label
    assert REDACTION_MARKER in sanitized, label


def test_sanitize_error_text_does_not_redact_ordinary_diagnostic_text():
    raw = (
        "connection reset by peer while fetching page 3 of 10 "
        "(status=502, retry_count=2)"
    )
    sanitized = sanitize_error_text(raw)
    assert sanitized == raw
    assert REDACTION_MARKER not in sanitized


def test_sanitize_error_text_truncates_and_redacts_before_truncating():
    secret = _FIXTURE_7
    # Pad the message so the secret would land right at the truncation
    # boundary if redaction ran AFTER truncation instead of before -- a
    # regression here would leave a partial, still-identifiable token.
    raw = ("x" * 50) + secret + ("y" * 50)

    sanitized = sanitize_error_text(raw, max_length=40)

    assert sanitized is not None
    assert len(sanitized) <= 40
    assert secret not in sanitized
    assert "ghp_" not in sanitized


def test_sanitize_error_text_no_truncation_under_max_length():
    raw = "short message"
    assert sanitize_error_text(raw, max_length=4000) == raw


def test_sanitize_error_text_max_length_none_disables_cap():
    raw = "z" * 10_000
    sanitized = sanitize_error_text(raw, max_length=None)
    assert sanitized == raw


# ---------------------------------------------------------------------------
# CHAOS-2780: idempotency property.
#
# ``dev-hops maintenance scrub-error-text`` relies on
# ``sanitize(sanitize(x)) == sanitize(x)`` to make a second ``--apply`` run
# report zero changes -- no ``_SECRET_PATTERN`` matches the literal
# ``REDACTION_MARKER``, and truncation is a fixed point once the text is
# under the cap. This was previously an unstated assumption the copy sites
# relied on (``workers/sync_units.py:1810-1817, 2057-2064``); this test pins
# it explicitly, over the same pattern-fixture table used above plus a
# capped variant so both the redaction and truncation fixed points are
# covered. Appended, not interleaved, so this addition stays a clean diff
# against the rest of the file.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "max_length"),
    [
        (f"GET /repos failed: Authorization: Bearer {_FIXTURE_2}", None),
        (f"GET /repos failed: Authorization: Bearer {_FIXTURE_2}", 40),
        (f"push rejected using {_FIXTURE_7}", None),
        (f"push rejected using {_FIXTURE_7}", 4000),
        (
            "connection reset by peer while fetching page 3 of 10 "
            "(status=502, retry_count=2)",
            None,
        ),
        (
            f"Error 111 connecting to redis://:{_FIXTURE_16}@redis-broker.internal:6379/0.",
            2000,
        ),
        ("x" * 10_000, 4000),
        ("x" * 10_000, None),
    ],
)
def test_sanitize_error_text_is_idempotent(raw, max_length):
    once = sanitize_error_text(raw, max_length=max_length)
    twice = sanitize_error_text(once, max_length=max_length)
    assert twice == once
