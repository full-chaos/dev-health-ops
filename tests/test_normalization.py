import pytest

from dev_health_ops.processors.normalization import canonicalize_environment


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("production", "production"),
        ("prod", "production"),
        ("PRODUCTION", "production"),
        ("Prod", "production"),
        ("staging", "staging"),
        ("stg", "staging"),
        ("stage", "staging"),
        ("development", "development"),
        ("dev", "development"),
        ("test", "test"),
        ("qa", "test"),
        ("testing", "test"),
        ("preview", "preview"),
        ("review/my-branch", "preview"),
        ("review-app-123", "preview"),
        ("", ""),
        ("custom-env", "custom-env"),  # unknown passes through lowercased
        ("  Production  ", "production"),  # whitespace stripped
    ],
)
def test_canonicalize_environment(raw, expected):
    assert canonicalize_environment(raw) == expected
