"""Generate the work-unit-label golden for CHAOS-4441.

Drives `materialize._resolve_work_unit_label` itself -- imported, never
imitated (plan.md section 5b's audit question). Same import cost as
generate_effort_golden.py / generate_repo_effort_allocation_golden.py and
excluded from the CI live-oracle closure for the identical httpx2 reason
(see excludedGenerators in
internal/jobs/workgraph/units/live_python_corpus_guard_test.go).

AXES VARIED
-----------
  * priority order: issue title beats PR title beats commit message, and
    within a tier the SMALLEST SORTED id wins, not the first id a caller
    happens to pass -- a case orders ids out of sort order to prove this.
  * the type-only fallback (no title/message ANYWHERE) uses the smallest
    sorted issue id's own type, defaulting to "issue" only when that
    specific field is empty.
  * whitespace that `.strip()` treats as strippable but a naive
    strings.TrimSpace-based port would not: 0x1c-0x1f control characters
    (pythonparity.IsSpace's own documented extension over unicode.IsSpace).
  * commit message line-boundary handling: str.splitlines() recognises
    \\r, \\r\\n, \\x0b, \\x0c, \\x1c-\\x1e, \\x85 (NEL), \\u2028, \\u2029 --
    not just \\n -- so a message using one of those as its only separator
    still yields a real first line, not the whole blob.

Usage:
    uv run python tests/fixtures/generate_work_unit_label_golden.py [--stdout]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from dev_health_ops.work_graph.investment.materialize import _resolve_work_unit_label

OUTPUT_PATH = Path(__file__).parent / "work_unit_label_python_golden.json"


def _scenarios() -> list[dict[str, Any]]:
    return [
        {"label": "nothing_at_all"},
        {
            "label": "single_issue_with_title",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "Fix the bug", "type": "bug"}},
        },
        {
            "label": "issue_title_empty_falls_to_pr",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "", "type": "bug"}},
            "pr_ids": ["p1"],
            "pr_map": {"p1": {"title": "Add the feature"}},
        },
        {
            "label": "issue_title_whitespace_only_falls_to_pr",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "   \t  ", "type": "bug"}},
            "pr_ids": ["p1"],
            "pr_map": {"p1": {"title": "Add the feature"}},
        },
        {
            "label": "issue_title_present_type_empty_defaults_issue",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "Fix the bug", "type": ""}},
        },
        {
            # Passed out of sort order (i2 first) -- the smaller sorted id
            # (i1) must still win, and it has no title, so this falls
            # through to i2's title, not i1's absence overriding it.
            "label": "sorted_order_within_tier_not_argument_order",
            "issue_ids": ["i2", "i1"],
            "work_item_map": {
                "i1": {"title": "", "type": "bug"},
                "i2": {"title": "Second issue title", "type": "feature"},
            },
        },
        {
            "label": "pr_title_used_when_no_issue_title",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "", "type": "bug"}},
            "pr_ids": ["p1"],
            "pr_map": {"p1": {"title": "PR title wins"}},
        },
        {
            "label": "commit_message_used_when_nothing_else",
            "commit_ids": ["c1"],
            "commit_map": {"c1": {"message": "Fix the login flow\n\nDetails here."}},
        },
        {
            "label": "commit_message_leading_blank_lines",
            "commit_ids": ["c1"],
            "commit_map": {"c1": {"message": "\n\n   \nReal first line\nmore"}},
        },
        {
            # \x1c (FS) is a line boundary for str.splitlines() but NOT for
            # a naive "\n"-only split -- this message has no "\n" at all.
            "label": "commit_message_fs_line_boundary",
            "commit_ids": ["c1"],
            "commit_map": {"c1": {"message": "first part\x1csecond part"}},
        },
        {
            # U+2029 (paragraph separator) is also a str.splitlines() boundary.
            "label": "commit_message_unicode_paragraph_separator",
            "commit_ids": ["c1"],
            "commit_map": {"c1": {"message": "para one para two"}},
        },
        {
            "label": "no_titles_anywhere_falls_to_type_only_from_smallest_issue",
            "issue_ids": ["i2", "i1"],
            "work_item_map": {
                "i1": {"title": "", "type": "epic"},
                "i2": {"title": "", "type": "bug"},
            },
        },
        {
            "label": "no_titles_type_empty_defaults_issue",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "", "type": ""}},
        },
        {
            "label": "no_issues_prs_present_no_titles_yields_pr_type_only",
            "pr_ids": ["p1"],
            "pr_map": {"p1": {"title": ""}},
        },
        {
            "label": "no_issues_or_prs_commits_present_no_messages_yields_commit_type_only",
            "commit_ids": ["c1"],
            "commit_map": {"c1": {"message": ""}},
        },
        {
            # 0x1c (FS) surrounding an otherwise-empty title -- pythonparity's
            # documented extension over unicode.IsSpace: `.strip()` treats
            # 0x1c-0x1f as whitespace, so this title cleans to "".
            "label": "control_char_whitespace_title_is_empty",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "\x1c\x1d  \x1e\x1f", "type": "bug"}},
            "pr_ids": ["p1"],
            "pr_map": {"p1": {"title": "PR fallback"}},
        },
        {
            "label": "title_with_surrounding_control_char_whitespace_is_stripped",
            "issue_ids": ["i1"],
            "work_item_map": {"i1": {"title": "\x1cReal Title\x1d", "type": "bug"}},
        },
        {
            "label": "missing_id_in_map_treated_as_absent",
            "issue_ids": ["i-not-in-map"],
            "work_item_map": {},
            "pr_ids": ["p1"],
            "pr_map": {"p1": {"title": "PR wins because issue map entry is missing"}},
        },
    ]


def main() -> None:
    cases: list[dict[str, Any]] = []
    for scenario in _scenarios():
        issue_ids = scenario.get("issue_ids", [])
        pr_ids = scenario.get("pr_ids", [])
        commit_ids = scenario.get("commit_ids", [])
        work_item_map = scenario.get("work_item_map", {})
        pr_map = scenario.get("pr_map", {})
        commit_map = scenario.get("commit_map", {})

        label_type, name = _resolve_work_unit_label(
            issue_ids=issue_ids,
            pr_ids=pr_ids,
            commit_ids=commit_ids,
            work_item_map=work_item_map,
            pr_map=pr_map,
            commit_map=commit_map,
        )
        cases.append(
            {
                "label": scenario["label"],
                "issue_ids": issue_ids,
                "pr_ids": pr_ids,
                "commit_ids": commit_ids,
                "work_item_map": work_item_map,
                "pr_map": pr_map,
                "commit_map": commit_map,
                "expected_type": label_type,
                "expected_name": name,
            }
        )

    payload = {
        "_comment": (
            "Generated by tests/fixtures/generate_work_unit_label_golden.py. "
            "Do not hand-edit."
        ),
        "_note": (
            "Priority: issue title, then PR title, then commit message; within "
            "a tier the SMALLEST SORTED id wins, not argument order. The "
            "type-only fallback (no title/message anywhere) uses the smallest "
            "sorted issue id's own type."
        ),
        "cases": cases,
    }
    rendered = json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    if "--stdout" in sys.argv[1:]:
        sys.stdout.write(rendered)
        return
    OUTPUT_PATH.write_text(rendered)
    print(f"wrote {OUTPUT_PATH}")
    print(f"  cases: {len(cases)}")


if __name__ == "__main__":
    main()
