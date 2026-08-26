from __future__ import annotations

import pytest

from dev_health_ops.workers.team_autoimport_categories import (
    import_categories_from_sync_options,
    resolve_import_categories,
)


@pytest.mark.parametrize(
    ("sync_options", "expected"),
    [
        # None: no sync_options context at all -- unrestricted (legacy callers).
        (None, {"teams": True, "projects": True, "members": True}),
        # {}: a real (empty) sync_options -- authoritative, every category off.
        ({}, {"teams": False, "projects": False, "members": False}),
        (
            {"auto_import_teams": True},
            {"teams": True, "projects": False, "members": False},
        ),
        (
            {"auto_import_projects": True},
            {"teams": False, "projects": True, "members": False},
        ),
        (
            {"auto_import_members": True},
            {"teams": False, "projects": False, "members": True},
        ),
        (
            {
                "auto_import_teams": True,
                "auto_import_projects": True,
                "auto_import_members": True,
            },
            {"teams": True, "projects": True, "members": True},
        ),
        # Unrelated keys are ignored.
        (
            {"owner": "acme", "auto_import_members": True},
            {"teams": False, "projects": False, "members": True},
        ),
    ],
)
def test_import_categories_from_sync_options(sync_options, expected):
    assert import_categories_from_sync_options(sync_options) == expected


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ({}, {"teams": True, "projects": True, "members": True}),
        (
            {"import_categories": {"teams": False, "projects": True, "members": False}},
            {"teams": False, "projects": True, "members": False},
        ),
        # A non-mapping import_categories is treated as absent.
        (
            {"import_categories": "nonsense"},
            {"teams": True, "projects": True, "members": True},
        ),
        # Partial dict: missing keys default True (this is the strict/backfill
        # default path -- resolve_import_categories is never fed a partial
        # dict by run_team_autoimport, which always sets all three).
        (
            {"import_categories": {"projects": False}},
            {"teams": True, "projects": False, "members": True},
        ),
    ],
)
def test_resolve_import_categories(scope, expected):
    assert resolve_import_categories(scope) == expected
