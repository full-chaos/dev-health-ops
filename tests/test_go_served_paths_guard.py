"""``ci/check_go_served_paths.py`` (CHAOS-6813): red/green against a real tree.

A Python route body may be reduced to the "served by Go" refusal stub only for a
path the deployed ingress routes to Go. The checker is loaded and run from
pytest (the precedent of ``tests/test_endpoint_profiles_contract.py``), which
puts it under a required context, and each of its three failure modes is proven
to fail: a gate that cannot be shown to fail is not a gate.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CHECKER_PATH = REPO_ROOT / "ci" / "check_go_served_paths.py"
MANIFEST_PATH = REPO_ROOT / "ci" / "go_served_paths.tsv"


def _load_checker():
    spec = importlib.util.spec_from_file_location(
        "_check_go_served_paths_under_test", CHECKER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve postponed annotations through sys.modules
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


checker = _load_checker()


@pytest.fixture(scope="module")
def real_routes() -> list[dict]:
    return checker.discover_routes(REPO_ROOT)


def test_the_real_tree_passes(real_routes):
    manifest, problems = checker.load_manifest(MANIFEST_PATH)
    assert problems == []
    assert checker.check(real_routes, manifest, REPO_ROOT) == []
    stubs = checker.stub_routes(real_routes, REPO_ROOT)
    # CHAOS-6241's 32 deleted bodies are recognised as stubs (a recogniser that
    # found none would make every other assertion here vacuous).
    # ... and the 14 sync-admin routes of CHAOS-6846 (go-api, rev187).
    assert len(stubs) == 46
    # a template row covers its static siblings (the ingress rule), so cover, not equality
    for route in stubs:
        path = checker.normalize(route["path"])
        assert any(checker.covers(template, path) for template in manifest), path


def test_a_stub_whose_path_leaves_the_manifest_fails(real_routes):
    manifest, _ = checker.load_manifest(MANIFEST_PATH)
    manifest = dict(manifest)
    del manifest["/api/v1/meta"]
    problems = checker.check(real_routes, manifest, REPO_ROOT)
    assert any(
        "/api/v1/meta" in problem and "not in the Go-served path manifest" in problem
        for problem in problems
    )


def test_a_manifest_row_with_no_python_route_fails(real_routes):
    manifest, _ = checker.load_manifest(MANIFEST_PATH)
    manifest = dict(manifest)
    manifest["/api/v1/admin/nothing-here"] = checker.ManifestRow(
        "rev999", "go-api", "/api/v1/admin/nothing-here"
    )
    problems = checker.check(real_routes, manifest, REPO_ROOT)
    assert any(
        "/api/v1/admin/nothing-here" in problem
        and "names no served Python route" in problem
        for problem in problems
    )


SOURCE = textwrap.dedent(
    '''
    def stubbed():
        _raise_served_by_go_api("/x")

    def stubbed_with_docstring():
        """Served by Go."""
        _raise_served_by_go_api("/x")

    def stubbed_return():
        return _raise_served_by_go_api("/x")

    def stubbed_attribute():
        module._raise_served_by_go_api("/x")

    def stubbed_public_name():
        raise_served_by_go_api("/x", "go-api")

    def stubbed_public_attribute():
        go_served.raise_served_by_go_api("/x", GO_API)

    def stubbed_keyword_plane():
        raise_served_by_go_api("/x", plane=GO_API)

    def stubbed_query_constant():
        raise_served_by_go_api("/x", QUERY_API)

    def stubbed_attribute_plane():
        raise_served_by_go_api("/x", go_served.GO_API)

    def stubbed_unknown_literal_plane():
        raise_served_by_go_api("/x", "python")

    def stubbed_computed_plane():
        raise_served_by_go_api("/x", pick_plane())

    def public_name_then_more():
        raise_served_by_go_api("/x", "go-api")
        return 1

    def logic_then_stub():
        value = 1
        _raise_served_by_go_api("/x")

    def stub_and_more():
        _raise_served_by_go_api("/x")
        return 1

    def calls_something_else():
        other_function("/x")

    def empty_body_docstring_only():
        """Nothing."""
        pass

    class Holder:
        @staticmethod
        def shared():
            return 1

    @decorator
    def shared():
        _raise_served_by_go_api("/x")
    '''
)


@pytest.fixture
def tree(tmp_path: Path):
    source = tmp_path / "routes.py"
    source.write_text(SOURCE)
    lines = {}
    for number, line in enumerate(SOURCE.splitlines(), start=1):
        if line.startswith("def "):
            lines[line[4 : line.index("(")]] = number
    lines["decorated_shared"] = lines["shared"] - 1
    return tmp_path, source, lines


def _route(
    source: Path, name: str, path: str, line: int | None = None, method: str = "GET"
) -> dict:
    return {
        "method": method,
        "methods": [method],
        "path": path,
        "file": str(source),
        "line": line,
        "endpoint_in_ops_source": True,
        "endpoint_name": name,
    }


def test_the_recogniser_counts_only_a_body_that_is_nothing_but_the_stub(tree):
    root, source, lines = tree
    stubs = {
        name
        for name in (
            "stubbed",
            "stubbed_with_docstring",
            "stubbed_return",
            "stubbed_attribute",
            "stubbed_public_name",
            "stubbed_public_attribute",
            "public_name_then_more",
            "logic_then_stub",
            "stub_and_more",
            "calls_something_else",
            "empty_body_docstring_only",
        )
        if checker.stub_routes([_route(source, name, f"/{name}")], root)
    }
    assert stubs == {
        "stubbed",
        "stubbed_with_docstring",
        "stubbed_return",
        "stubbed_attribute",
        "stubbed_public_name",
        "stubbed_public_attribute",
    }


def test_same_named_functions_are_told_apart_by_line(tree):
    root, source, lines = tree
    decorated = _route(source, "shared", "/shared", line=lines["decorated_shared"])
    assert checker.stub_routes([decorated], root) == [decorated]
    # the class's staticmethod of the same name is not the endpoint
    method = _route(source, "shared", "/shared", line=lines["shared"] + 40)
    assert checker.stub_routes([method], root) == []


def test_routes_outside_the_ops_source_are_ignored(tree):
    root, source, _ = tree
    foreign = _route(source, "stubbed", "/foreign")
    foreign["endpoint_in_ops_source"] = False
    assert checker.stub_routes([foreign], root) == []


def test_parameter_names_do_not_matter_but_the_path_does(tree, tmp_path):
    root, source, _ = tree
    manifest = {
        "/api/v1/things/{}": checker.ManifestRow(
            "rev1", "query-api", "/api/v1/things/{}"
        )
    }
    same = _route(source, "stubbed", "/api/v1/things/{thing_id}")
    assert checker.check([same], manifest, root) == []
    other = _route(source, "stubbed", "/api/v1/others/{thing_id}")
    problems = checker.check([other], manifest, root)
    assert len(problems) == 2  # the stub's path is unlisted AND the row names no route
    assert checker.normalize("/a/{x}/b/{y}/") == "/a/{}/b/{}"
    assert checker.normalize("/") == "/"


def test_a_method_on_a_listed_path_is_covered_by_the_path(tree):
    root, source, _ = tree
    manifest = {"/p": checker.ManifestRow("rev1", "query-api", "/p")}
    routes = [
        _route(source, "stubbed", "/p", method="GET"),
        _route(source, "stubbed_return", "/p", method="POST"),
    ]
    assert checker.check(routes, manifest, root) == []


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("rev1\tgo-api\t/api/v1/x", None),
        ("rev1\tgo-api", "3 tab-separated cells"),
        ("rev1\tgo-api\t/api/v1/x\textra", "3 tab-separated cells"),
        ("rev1\t\t/api/v1/x", "3 tab-separated cells"),
        ("rev1\tpython\t/api/v1/x", "plane 'python'"),
        ("rev1\tgo-api\t/api/v1/plans/[^/]+$", "anonymous {} parameters"),
        ("rev1\tgo-api\t/api/v1/plans/{plan_id}", "anonymous {} parameters"),
        ("rev1\tgo-api\t/api/v1/x/", "anonymous {} parameters"),
        ("rev1\tgo-api\tapi/v1/x", "anonymous {} parameters"),
    ],
)
def test_a_malformed_manifest_row_is_reported(tmp_path, line, expected):
    manifest = tmp_path / "m.tsv"
    manifest.write_text("# c\n\n" + line + "\n")
    rows, problems = checker.load_manifest(manifest)
    if expected is None:
        assert problems == [] and list(rows) == ["/api/v1/x"]
    else:
        assert len(problems) == 1 and expected in problems[0]


def test_a_duplicate_path_and_a_missing_manifest_are_reported(tmp_path):
    manifest = tmp_path / "m.tsv"
    manifest.write_text("rev1\tgo-api\t/a\nrev2\tquery-api\t/a\n")
    rows, problems = checker.load_manifest(manifest)
    assert len(rows) == 1 and any("listed twice" in problem for problem in problems)
    _, missing = checker.load_manifest(tmp_path / "absent.tsv")
    assert any("does not exist" in problem for problem in missing)


def test_a_template_covers_its_static_sibling_as_the_ingress_does(tree):
    root, source, _ = tree
    manifest = {
        "/api/v1/billing/plans/{}": checker.ManifestRow(
            "rev1", "query-api", "/api/v1/billing/plans/{}"
        )
    }
    pull = _route(source, "stubbed", "/api/v1/billing/plans/pull-stripe", method="POST")
    by_id = _route(
        source, "stubbed_return", "/api/v1/billing/plans/{plan_id}", method="GET"
    )
    assert checker.check([pull, by_id], manifest, root) == []
    # ... but not a longer path, a different prefix, or the collection itself
    for path in (
        "/api/v1/billing/plans",
        "/api/v1/billing/plans/a/b",
        "/api/v1/billing/planx/1",
    ):
        assert not checker.covers("/api/v1/billing/plans/{}", path), path
    assert checker.covers("/a/{}/b", "/a/x/b") and not checker.covers(
        "/a/{}/b", "/a/x/y/b"
    )
    assert checker.covers("/exact", "/exact")


def test_a_template_row_is_satisfied_by_a_static_sibling_alone(tree):
    root, source, _ = tree
    manifest = {
        "/api/v1/billing/plans/{}": checker.ManifestRow(
            "rev1", "query-api", "/api/v1/billing/plans/{}"
        )
    }
    only_static = _route(
        source, "stubbed", "/api/v1/billing/plans/pull-stripe", method="POST"
    )
    assert checker.check([only_static], manifest, root) == []
    unrelated = _route(source, "stubbed", "/api/v1/billing/other", method="POST")
    assert (
        len(checker.check([unrelated], manifest, root)) == 2
    )  # unlisted stub + orphan row


def test_covers_matches_one_non_empty_segment_and_escapes_the_rest():
    assert not checker.covers(
        "/a/{}/b", "/a//b"
    )  # a parameter is at least one character
    assert not checker.covers("/a.b/{}", "/aXb/x")  # a dot is a dot
    assert not checker.covers("/a+b/{}", "/aab/x")
    assert checker.covers("/a.b/{}", "/a.b/x")


# --- CHAOS-6865: the stub's plane must be the plane the manifest routes the path to


def _plane_check(tree, stub: str, plane: str) -> list[str]:
    root, source, _ = tree
    manifest = {"/p": checker.ManifestRow("rev1", plane, "/p")}
    return checker.check([_route(source, stub, "/p")], manifest, root)


@pytest.mark.parametrize(
    ("stub", "stub_plane"),
    [
        ("stubbed", "query-api"),  # omitted plane = the function's own default
        ("stubbed_return", "query-api"),
        ("stubbed_query_constant", "query-api"),  # QUERY_API constant
        ("stubbed_public_name", "go-api"),  # "go-api" literal
        ("stubbed_public_attribute", "go-api"),  # go_served.GO_API attribute
        ("stubbed_keyword_plane", "go-api"),  # plane=GO_API keyword
        ("stubbed_attribute_plane", "go-api"),  # go_served.GO_API as the argument
    ],
)
def test_a_stub_is_accepted_only_on_the_plane_it_names(tree, stub, stub_plane):
    other = "go-api" if stub_plane == "query-api" else "query-api"
    assert _plane_check(tree, stub, stub_plane) == []
    problems = _plane_check(tree, stub, other)
    assert len(problems) == 1, problems
    assert f"the stub says {stub_plane} owns the route" in problems[0]
    assert other in problems[0] and "wrong service" in problems[0]


@pytest.mark.parametrize(
    "stub", ["stubbed_computed_plane", "stubbed_unknown_literal_plane"]
)
def test_a_stub_plane_the_guard_cannot_read_is_refused_not_waved_through(tree, stub):
    for plane in ("go-api", "query-api"):
        problems = _plane_check(tree, stub, plane)
        assert len(problems) == 1 and "cannot tell which" in problems[0], problems


def test_a_mixed_plane_cover_accepts_the_stub_when_any_covering_row_agrees(tree):
    root, source, _ = tree
    manifest = {
        "/p/{}": checker.ManifestRow("rev1", "go-api", "/p/{}"),
        "/p/x": checker.ManifestRow("rev1", "query-api", "/p/x"),
    }
    route = _route(source, "stubbed", "/p/x")  # default plane query-api
    assert checker.check([route], manifest, root) == []


def test_swapping_every_manifest_plane_on_the_real_tree_fails(real_routes):
    manifest, _ = checker.load_manifest(MANIFEST_PATH)
    assert checker.check(real_routes, manifest, REPO_ROOT) == []
    swapped = {
        path: checker.ManifestRow(
            row.rev, "go-api" if row.plane == "query-api" else "query-api", path
        )
        for path, row in manifest.items()
    }
    problems = checker.check(real_routes, swapped, REPO_ROOT)
    stubs = checker.stub_routes(real_routes, REPO_ROOT)
    wrong = [problem for problem in problems if "wrong service" in problem]
    # one problem per stubbed route: a guard that ignores the plane reports none
    assert len(wrong) == len(stubs) > 0, (len(wrong), len(stubs))


# --- CHAOS-6870: the manifest is pinned to the ingress dump by its receipt line


def _receipt_case(tmp_path: Path, mutate) -> list[str]:
    """Problems for a copy of the real manifest after ``mutate(lines)``."""
    lines = MANIFEST_PATH.read_text().splitlines()
    mutate(lines)
    copy = tmp_path / "manifest.tsv"
    copy.write_text("\n".join(lines) + "\n")
    rows, problems = checker.load_manifest(copy)
    assert problems == []
    return checker.check_receipt(copy, rows)


def test_the_real_manifest_matches_its_receipt(tmp_path):
    rows, problems = checker.load_manifest(MANIFEST_PATH)
    assert problems == []
    assert checker.check_receipt(MANIFEST_PATH, rows) == []
    assert len(rows) >= 145  # the rev187 dump: a receipt over nothing proves nothing
    assert _receipt_case(tmp_path, lambda lines: None) == []


def _first_row(lines: list[str]) -> int:
    return next(
        index
        for index, line in enumerate(lines)
        if line and not line.startswith("#") and line.count("\t") == 2
    )


def test_a_dropped_row_fails_the_receipt(tmp_path):
    problems = _receipt_case(tmp_path, lambda lines: lines.pop(_first_row(lines)))
    assert any("records" in problem and "rows" in problem for problem in problems)
    assert any("digest" in problem for problem in problems)


def test_an_added_row_fails_the_receipt(tmp_path):
    problems = _receipt_case(
        tmp_path, lambda lines: lines.append("rev999\tgo-api\t/api/v1/admin/invented")
    )
    assert any("records" in problem for problem in problems)
    assert any("digest" in problem for problem in problems)


def test_a_changed_plane_keeps_the_count_but_fails_the_digest(tmp_path):
    def flip(lines: list[str]) -> None:
        index = _first_row(lines)
        rev, plane, path = lines[index].split("\t")
        lines[index] = "\t".join(
            [rev, "query-api" if plane == "go-api" else "go-api", path]
        )

    problems = _receipt_case(tmp_path, flip)
    assert len(problems) == 1 and "digest" in problems[0], problems


def test_a_changed_path_keeps_the_count_but_fails_the_digest(tmp_path):
    def rename(lines: list[str]) -> None:
        index = _first_row(lines)
        rev, plane, path = lines[index].split("\t")
        lines[index] = "\t".join([rev, plane, path + "-x"])

    problems = _receipt_case(tmp_path, rename)
    assert len(problems) == 1 and "digest" in problems[0], problems


def test_the_rev_column_is_not_part_of_the_receipt(tmp_path):
    def relabel(lines: list[str]) -> None:
        index = _first_row(lines)
        _, plane, path = lines[index].split("\t")
        lines[index] = "\t".join(["rev999", plane, path])

    assert _receipt_case(tmp_path, relabel) == []


@pytest.mark.parametrize("mode", ["missing", "duplicated", "malformed"])
def test_a_missing_duplicated_or_malformed_receipt_line_fails(tmp_path, mode):
    def mutate(lines: list[str]) -> None:
        index = next(i for i, line in enumerate(lines) if line.startswith("# receipt:"))
        if mode == "missing":
            del lines[index]
        elif mode == "duplicated":
            lines.insert(index, lines[index])
        else:
            lines[index] = "# receipt: rev187 rows=many sha256=nothex"

    problems = _receipt_case(tmp_path, mutate)
    assert len(problems) == 1 and "exactly one" in problems[0], problems


def test_the_cli_fails_on_a_receipt_mismatch(tmp_path, real_routes):
    lines = MANIFEST_PATH.read_text().splitlines()
    lines.pop(_first_row(lines))
    copy = tmp_path / "manifest.tsv"
    copy.write_text("\n".join(lines) + "\n")
    routes_json = tmp_path / "routes.json"
    routes_json.write_text(json.dumps({"routes": real_routes}))
    assert (
        checker.main(
            [
                "--root",
                str(REPO_ROOT),
                "--manifest",
                str(copy),
                "--routes-json",
                str(routes_json),
            ]
        )
        == 1
    )
