"""Live Python GitLab merge-request review row producer oracle."""

from __future__ import annotations

import pathlib

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import class_annotated_field_names
from internal.providersync.testdata.oracle_pairs._gitlab_pr_family_common import (
    MODEL_SOURCE,
    build_review_row,
)

oracle_registry.register(
    oracle_registry.PairSpec(
        id="gitlab/pr-reviews/row",
        build_row=build_review_row,
        reflected_fields=lambda: class_annotated_field_names(
            pathlib.Path(MODEL_SOURCE).read_text(), "GitPullRequestReview"
        ),
        excluded_fields={
            "last_synced": "stamped from the Go collection instant, not the Python mapper",
        },
    )
)

