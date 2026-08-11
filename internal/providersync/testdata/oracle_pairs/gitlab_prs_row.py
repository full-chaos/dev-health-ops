"""Live Python GitLab merge-request row producer oracle."""

from __future__ import annotations

import pathlib

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import dict_literal_keys
from internal.providersync.testdata.oracle_pairs._gitlab_pr_family_common import (
    BASE_GIT_SOURCE,
    build_pr_row,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/prs/row",
        build_row=build_pr_row,
        reflected_fields=lambda: dict_literal_keys(
            pathlib.Path(BASE_GIT_SOURCE).read_text(),
            "build_git_pull_request",
            ("values", "optional_values"),
        ),
        excluded_fields={
            "additions": "GitLab MR sync does not provide GitHub diff-stat fields to build_git_pull_request",
            "deletions": "GitLab MR sync does not provide GitHub diff-stat fields to build_git_pull_request",
            "changed_files": "GitLab MR sync does not provide GitHub diff-stat fields to build_git_pull_request",
            "first_comment_at": "GitLab MR sync does not populate comment-event timestamps in the PR row",
        },
    )
)
