"""Golden generator for the pure-logic work_units.py helpers this port's
attribution.go/workitemid.go port -- CHAOS-4977's build_work_unit_investments
step. Calls the REAL Python functions directly (module-private, imported via
attribute access) rather than hand-imitating them.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_workunit_attribution_golden.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from dev_health_ops.api.models.filters import MetricFilter
from dev_health_ops.api.services import work_units
from dev_health_ops.external_ingest.ids import derive_work_item_id

OUT_DIR = Path(__file__).parent


def majority_team_cases() -> dict[str, dict]:
    team_map = {
        "issue-a": {"team_id": "team-1", "team_name": "Platform"},
        "issue-b": {"team_id": "team-1", "team_name": "Platform"},
        "issue-c": {"team_id": "team-2", "team_name": "Growth"},
        "issue-d": {"team_id": "team-2", "team_name": "Growth Renamed"},
        "issue-tie-x": {"team_id": "team-zzz", "team_name": "Zed"},
        "issue-tie-y": {"team_id": "team-aaa", "team_name": "Alpha"},
        "issue-blank-team": {"team_id": "", "team_name": "Ignored"},
        "issue-whitespace-team": {"team_id": "   ", "team_name": "Ignored"},
    }
    cases = {
        "clear_majority": {
            "issue_ids": ["issue-a", "issue-b", "issue-c"],
            "team_map": team_map,
        },
        "tie_breaks_on_larger_team_id": {
            "issue_ids": ["issue-tie-x", "issue-tie-y"],
            "team_map": team_map,
        },
        "no_known_issues_falls_back_unassigned": {
            "issue_ids": ["issue-unknown-1", "issue-unknown-2"],
            "team_map": team_map,
        },
        "blank_team_id_ignored": {
            "issue_ids": ["issue-blank-team", "issue-c"],
            "team_map": team_map,
        },
        "whitespace_team_id_ignored": {
            "issue_ids": ["issue-whitespace-team", "issue-c"],
            "team_map": team_map,
        },
        "duplicate_refs_do_not_double_vote": {
            "issue_ids": ["issue-a", "issue-a", "issue-c"],
            "team_map": team_map,
        },
        "empty_issue_ids": {"issue_ids": [], "team_map": team_map},
        "label_takes_max_over_winning_id_refs": {
            "issue_ids": ["issue-c", "issue-d"],
            "team_map": team_map,
        },
    }
    return cases


def pr_ref_cases() -> dict[str, dict]:
    repo_identities = {
        "11111111-1111-1111-1111-111111111111": ["myorg/myrepo", "github"],
        "22222222-2222-2222-2222-222222222222": ["mygroup/myproject", "gitlab"],
    }
    cases = {
        "github_pr_ref": {
            "pr_ref": "11111111-1111-1111-1111-111111111111#pr42",
            "repo_identities": repo_identities,
        },
        "gitlab_mr_ref": {
            "pr_ref": "22222222-2222-2222-2222-222222222222#pr7",
            "repo_identities": repo_identities,
        },
        "unknown_repo_uuid": {
            "pr_ref": "33333333-3333-3333-3333-333333333333#pr1",
            "repo_identities": repo_identities,
        },
        "not_pr_shaped": {
            "pr_ref": "not-a-pr-ref",
            "repo_identities": repo_identities,
        },
        "malformed_uuid_length": {
            "pr_ref": "1111#pr1",
            "repo_identities": repo_identities,
        },
    }
    return cases


def structural_payload_cases() -> dict[str, dict]:
    return {
        "issues_and_prs": {
            "payload": json.dumps(
                {
                    "issues": ["issue-1", "issue-2", 0, "", None],
                    "prs": ["11111111-1111-1111-1111-111111111111#pr42", ""],
                }
            )
        },
        "missing_keys": {"payload": json.dumps({"other": "value"})},
        "not_a_dict": {"payload": json.dumps(["a", "b"])},
        "invalid_json": {"payload": "{not valid"},
        "empty_string": {"payload": ""},
    }


def distribution_cases() -> dict[str, dict]:
    return {
        "simple_dict_string": {
            "value": json.dumps({"velocity": 62.5, "quality": 37.5})
        },
        "zero_and_null_values": {"value": json.dumps({"velocity": 0, "quality": None})},
        "invalid_json": {"value": "{broken"},
        "not_a_dict": {"value": json.dumps([1, 2, 3])},
        "empty_string": {"value": ""},
    }


def category_filter_cases() -> dict[str, dict]:
    theme_dist = {"velocity": 62.5, "quality": 0.0}
    subcat_dist = {"velocity.feature": 40.0, "quality.bugfix": 0.0}
    return {
        "no_filters_matches_everything": {
            "theme_distribution": theme_dist,
            "subcategory_distribution": subcat_dist,
            "themes": [],
            "subcategories": [],
        },
        "subcategory_filter_matches": {
            "theme_distribution": theme_dist,
            "subcategory_distribution": subcat_dist,
            "themes": [],
            "subcategories": ["velocity.feature"],
        },
        "subcategory_filter_zero_value_does_not_match": {
            "theme_distribution": theme_dist,
            "subcategory_distribution": subcat_dist,
            "themes": [],
            "subcategories": ["quality.bugfix"],
        },
        "theme_filter_matches": {
            "theme_distribution": theme_dist,
            "subcategory_distribution": subcat_dist,
            "themes": ["velocity"],
            "subcategories": [],
        },
        "theme_filter_zero_value_does_not_match": {
            "theme_distribution": theme_dist,
            "subcategory_distribution": subcat_dist,
            "themes": ["quality"],
            "subcategories": [],
        },
    }


def split_category_filters_cases() -> dict[str, dict]:
    return {
        "mixed_themes_and_subcategories": {
            "work_category": ["velocity", "quality.bugfix", "velocity.feature", ""]
        },
        "duplicates_deduped": {
            "work_category": ["velocity", "velocity", "velocity.feature"]
        },
        "empty_list": {"work_category": []},
        "whitespace_entries_dropped": {"work_category": ["  ", "velocity"]},
    }


def work_item_id_cases() -> dict[str, dict]:
    return {
        "jira": {
            "system": "jira",
            "instance": "ignored",
            "external_key": "PROJ-1",
            "work_item_type": None,
        },
        "linear": {
            "system": "linear",
            "instance": "ignored",
            "external_key": "ENG-42",
            "work_item_type": None,
        },
        "github_issue": {
            "system": "github",
            "instance": "myorg/myrepo",
            "external_key": "5",
            "work_item_type": None,
        },
        "github_pr": {
            "system": "github",
            "instance": "myorg/myrepo",
            "external_key": "5",
            "work_item_type": "pr",
        },
        "gitlab_issue": {
            "system": "gitlab",
            "instance": "mygroup/myproject",
            "external_key": "9",
            "work_item_type": None,
        },
        "gitlab_mr": {
            "system": "gitlab",
            "instance": "mygroup/myproject",
            "external_key": "9",
            "work_item_type": "merge_request",
        },
        "custom_fallback": {
            "system": "custom",
            "instance": "some-instance",
            "external_key": "k1",
            "work_item_type": None,
        },
    }


def main() -> None:
    output: dict[str, dict[str, Any]] = {
        "majority_team_for_issues": {},
        "pr_ref_work_item_id": {},
        "extract_issue_ids": {},
        "extract_pr_refs": {},
        "parse_distribution": {},
        "matches_category_filter": {},
        "split_category_filters": {},
        "derive_work_item_id": {},
    }

    for name, case in majority_team_cases().items():
        team_id, team_name = work_units._majority_team_for_issues(
            case["issue_ids"], case["team_map"]
        )
        output["majority_team_for_issues"][name] = {
            "input": case,
            "team_id": team_id,
            "team_name": team_name,
        }

    for name, case in pr_ref_cases().items():
        repo_identities = {k: tuple(v) for k, v in case["repo_identities"].items()}
        result = work_units._pr_ref_work_item_id(case["pr_ref"], repo_identities)
        output["pr_ref_work_item_id"][name] = {"input": case, "result": result}

    for name, case in structural_payload_cases().items():
        output["extract_issue_ids"][name] = {
            "input": case,
            "result": work_units._extract_issue_ids(case["payload"]),
        }
        output["extract_pr_refs"][name] = {
            "input": case,
            "result": work_units._extract_pr_refs(case["payload"]),
        }

    for name, case in distribution_cases().items():
        output["parse_distribution"][name] = {
            "input": case,
            "result": work_units._parse_distribution(case["value"]),
        }

    for name, case in category_filter_cases().items():
        output["matches_category_filter"][name] = {
            "input": case,
            "result": work_units._matches_category_filter(
                case["theme_distribution"],
                case["subcategory_distribution"],
                case["themes"],
                case["subcategories"],
            ),
        }

    class _Why:
        def __init__(self, work_category):
            self.work_category = work_category

    class _Filters:
        def __init__(self, work_category):
            self.why = _Why(work_category)

    for name, case in split_category_filters_cases().items():
        # _split_category_filters takes a MetricFilter, not a raw list --
        # this stand-in carries only the ONE field it touches
        # (filters.why.work_category), matching the function's own real
        # access pattern rather than constructing a full MetricFilter.
        themes, subcategories = work_units._split_category_filters(
            cast(MetricFilter, _Filters(case["work_category"]))
        )
        output["split_category_filters"][name] = {
            "input": case,
            "themes": themes,
            "subcategories": subcategories,
        }

    for name, case in work_item_id_cases().items():
        output["derive_work_item_id"][name] = {
            "input": case,
            "result": derive_work_item_id(
                case["system"],
                case["instance"],
                case["external_key"],
                case["work_item_type"],
            ),
        }

    out_path = OUT_DIR / "workunit_attribution.json"
    out_path.write_text(
        json.dumps(output, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    )
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
