"""``providers/github/graphql.py`` query builder + response parser tests
(CHAOS-2773 CS7).

Pins byte-for-byte parity with the legacy ``connectors/utils/graphql.py::
GitHubGraphQLClient.get_blame`` / ``get_blob_texts`` response-shape handling
this module relocates, plus the GraphQL-level error contract that the
transport core's status-code classification never sees (a GraphQL error
rides on an HTTP 200).
"""

from __future__ import annotations

import pytest

from dev_health_ops.exceptions import APIException
from dev_health_ops.providers.github.graphql import (
    blame_variables,
    build_blob_texts_query,
    github_graphql_url,
    parse_blame_response,
    parse_blob_texts_response,
    raise_for_graphql_errors,
)


def test_blame_variables_carries_all_four_fields() -> None:
    assert blame_variables("acme", "widgets", "src/app.py", "main") == {
        "owner": "acme",
        "repo": "widgets",
        "path": "src/app.py",
        "ref": "main",
    }


def test_github_graphql_url_uses_public_github_endpoint() -> None:
    assert (
        github_graphql_url("https://api.github.com") == "https://api.github.com/graphql"
    )


def test_github_graphql_url_uses_enterprise_graphql_endpoint() -> None:
    assert (
        github_graphql_url("https://ghe.example.com/api/v3")
        == "https://ghe.example.com/api/graphql"
    )


def test_github_graphql_url_preserves_enterprise_prefix() -> None:
    assert (
        github_graphql_url("https://ghe.example.com/source/api/v3")
        == "https://ghe.example.com/source/api/graphql"
    )


def test_build_blob_texts_query_aliases_one_field_per_path() -> None:
    query = build_blob_texts_query("main", ["src/app.py", "README.md"])

    assert 'f0: object(expression: "main:src/app.py")' in query
    assert 'f1: object(expression: "main:README.md")' in query
    assert "isBinary" in query
    assert "isTruncated" in query


def test_build_blob_texts_query_json_escapes_path_special_characters() -> None:
    # A path containing a quote/backslash must not break out of the
    # GraphQL string literal -- json.dumps is the escaping mechanism.
    query = build_blob_texts_query("main", ['weird"path.py'])
    assert '\\"path.py' in query


def test_raise_for_graphql_errors_noop_when_no_errors_key() -> None:
    raise_for_graphql_errors({"data": {}}, operation="files:test")
    raise_for_graphql_errors({}, operation="files:test")


def test_raise_for_graphql_errors_raises_api_exception_with_joined_messages() -> None:
    with pytest.raises(APIException) as exc_info:
        raise_for_graphql_errors(
            {"errors": [{"message": "Field 'x' doesn't exist"}, {"message": "boom"}]},
            operation="blame:POST /graphql",
        )
    assert "Field 'x' doesn't exist" in str(exc_info.value)
    assert "boom" in str(exc_info.value)
    assert "blame:POST /graphql" in str(exc_info.value)


def test_parse_blob_texts_response_omits_binary_truncated_and_missing() -> None:
    payload = {
        "repository": {
            "f0": {"text": "hello\n", "isBinary": False, "isTruncated": False},
            "f1": {"text": None, "isBinary": True, "isTruncated": False},
            "f2": {"text": "big", "isBinary": False, "isTruncated": True},
            "f3": None,
        }
    }
    result = parse_blob_texts_response(payload, ["a.py", "b.bin", "c.py", "d.py"])

    assert result == {
        "a.py": "hello\n",
        "b.bin": None,
        "c.py": None,
        "d.py": None,
    }


def test_parse_blame_response_builds_ranges_and_computes_age() -> None:
    payload = {
        "repository": {
            "object": {
                "blame": {
                    "ranges": [
                        {
                            "startingLine": 1,
                            "endingLine": 3,
                            "commit": {
                                "oid": "abc123",
                                "authoredDate": "2020-01-01T00:00:00Z",
                                "author": {"name": "Ada", "email": "ada@example.com"},
                            },
                        }
                    ]
                }
            }
        }
    }

    blame = parse_blame_response(payload, file_path="src/app.py")

    assert blame.file_path == "src/app.py"
    assert len(blame.ranges) == 1
    rng = blame.ranges[0]
    assert rng.starting_line == 1
    assert rng.ending_line == 3
    assert rng.commit_sha == "abc123"
    assert rng.author == "Ada"
    assert rng.author_email == "ada@example.com"
    # Authored in 2020 -- age is a large positive number of seconds.
    assert rng.age_seconds > 0


def test_parse_blame_response_defaults_on_missing_shape() -> None:
    blame = parse_blame_response({}, file_path="src/app.py")

    assert blame.file_path == "src/app.py"
    assert blame.ranges == []


def test_parse_blame_response_unparseable_date_falls_back_to_zero_age() -> None:
    payload = {
        "repository": {
            "object": {
                "blame": {
                    "ranges": [
                        {
                            "startingLine": 1,
                            "endingLine": 1,
                            "commit": {
                                "oid": "abc",
                                "authoredDate": "not-a-date",
                                "author": {},
                            },
                        }
                    ]
                }
            }
        }
    }

    blame = parse_blame_response(payload, file_path="src/app.py")

    assert blame.ranges[0].age_seconds == 0
    assert blame.ranges[0].author == "Unknown"
    assert blame.ranges[0].author_email == ""
