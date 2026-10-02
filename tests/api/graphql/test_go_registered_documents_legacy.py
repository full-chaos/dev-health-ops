"""CHAOS-8000: a legacy registrydump entry never replaces an operation's current text."""

from __future__ import annotations

from _go_registered_documents import documents_by_operation


def test_legacy_entry_after_the_current_one_does_not_replace_it():
    docs = [
        {"operation": "foo", "document": "query Foo { new }", "digest": "d-new"},
        {
            "operation": "foo",
            "document": "query Foo { old }",
            "digest": "d-old",
            "legacy": True,
        },
        {"operation": "bar", "document": "query Bar { bar }", "digest": "d-bar"},
    ]
    assert documents_by_operation(docs) == {
        "foo": "query Foo { new }",
        "bar": "query Bar { bar }",
    }


def test_no_legacy_entries_is_the_plain_mapping():
    docs = [{"operation": "foo", "document": "query Foo { foo }", "digest": "d"}]
    assert documents_by_operation(docs) == {"foo": "query Foo { foo }"}
