"""Live Projects v2 PullRequest inclusion-decision oracle.

DOCUMENTED DIVERGENCE, not a parity pin, since CHAOS-4194. Python drops a
PullRequest board item outright (normalize.py:514); Go now emits a
project-membership row for it. `emitted` is therefore excluded below -- with
the reason and the rulings that produced it -- while `transition_count` stays
compared, because the other half of the decision (a PR board item contributes
no status transitions; its `changes` history is CHAOS-4221) is still genuine
parity worth pinning.

The pair is deliberately NOT deleted. Deleting it would discard the
`transition_count` pin to record one disagreement, and would leave nothing
asserting that Python's behaviour is what the divergence note claims it is.
"""

from __future__ import annotations

import contextlib
import io
import pathlib
from typing import Any

from internal.providersync.testdata import oracle_registry
from internal.providersync.testdata.field_reflection import (
    RETURN_LITERAL,
    dict_literal_keys,
)
from internal.providersync.testdata.oracle_pairs._github_work_items_helpers import (
    install_minimal_oracle_imports,
)

install_minimal_oracle_imports()

with (
    contextlib.redirect_stdout(io.StringIO()),
    contextlib.redirect_stderr(io.StringIO()),
):
    from dev_health_ops.providers.github.normalize import (
        github_project_v2_item_to_work_item,
    )
    from dev_health_ops.providers.identity import load_identity_resolver
    from dev_health_ops.providers.status_mapping import load_status_mapping

_THIS_FILE = pathlib.Path(__file__)


def _build_row(case: dict[str, Any]) -> dict[str, Any]:
    row, transitions = github_project_v2_item_to_work_item(
        item_node=case["item_node"],
        project_scope_id=case["project_scope_id"],
        status_mapping=load_status_mapping(),
        identity=load_identity_resolver(),
    )
    return {"emitted": row is not None, "transition_count": len(transitions)}


oracle_registry.register(
    oracle_registry.PairSpec(
        id="github/work-items-project-v2/pr-skip",
        build_row=_build_row,
        reflected_fields=lambda: dict_literal_keys(
            _THIS_FILE.read_text(), "_build_row", (RETURN_LITERAL,)
        ),
        excluded_fields={
            "emitted": (
                "documented divergence, not a gap: Go emits a "
                "project_membership_transition row for a PullRequest board "
                "item where Python drops it (normalize.py:514). chris ruling "
                "2026-08-23 ('if there's a project mapping it should be in the "
                "graph ... especially PRs'); shape ruled by Context Fabric "
                "2026-08-24; built under CHAOS-4194. Python is not wrong here, "
                "it is superseded -- prod is Go workers only."
            ),
        },
    )
)
