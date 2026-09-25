"""``ci/check_go_served_paths.py`` (CHAOS-6813): red/green against a real tree.

A Python route body may be reduced to the "served by Go" refusal stub only for a
path the deployed ingress routes to Go. The checker is loaded and run from
pytest (the precedent of ``tests/test_endpoint_profiles_contract.py``), which
puts it under a required context, and each of its three failure modes is proven
to fail: a gate that cannot be shown to fail is not a gate.
"""

from __future__ import annotations

import importlib.util
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
    assert len(stubs) == 32
    assert {checker.normalize(route["path"]) for route in stubs} <= set(manifest)


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
        "/api/v1/things/{}": checker.ManifestRow("rev1", "go-api", "/api/v1/things/{}")
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
    manifest = {"/p": checker.ManifestRow("rev1", "go-api", "/p")}
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
            "rev1", "go-api", "/api/v1/billing/plans/{}"
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
