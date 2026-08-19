"""Contract artifacts must resolve from an INSTALLED distribution, not only a checkout.

CHAOS-3933. ``provider_unit_route.py`` resolved the provider matrix as
``Path(__file__).resolve().parents[3] / "contracts" / ...``. Three hops from
``src/dev_health_ops/workers/`` is the repository root; three hops from
``site-packages/dev_health_ops/workers/`` is the interpreter's ``lib``
directory, because the intermediate directory is ``site-packages`` rather than
``src``. Nothing failed while dispatch ran inside the Celery runner image,
which carries the source tree. The Go sync-dispatch bridge then moved dispatch
onto the ``api`` service, which runs the installed wheel, and every production
``dispatch_sync_run`` returned 500:

    FileNotFoundError: [Errno 2] No such file or directory:
    '/usr/local/lib/python3.14/contracts/provider-matrix/v1/matrix.json'

The code had not changed. The LAYOUT under it had.

Resolution and packaging are one contract and each half is useless alone: a
resolver that checks the installed location still finds nothing if
``pyproject.toml`` does not ship the tree, and a shipped tree is never read by
a resolver that only looks in a checkout. Both halves are asserted here.

A checkout-only test cannot see either failure -- it passes today, before any
fix, which is precisely why the bug reached production.
"""

from __future__ import annotations

import fnmatch
import subprocess
import sys
import sysconfig
import textwrap
from pathlib import Path

import pytest
import tomllib

from dev_health_ops import contract_artifacts

ROOT = Path(__file__).resolve().parents[2]

# The trees dev_health_ops reads at runtime, and the loader that reads each.
RUNTIME_CONTRACT_TREES = {
    ("provider-matrix", "v1"): "matrix.json",
    ("sync-dispatch", "v1"): "transport-routes.json",
}


def _data_files() -> dict[str, list[str]]:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        config = tomllib.load(handle)
    return config["tool"]["setuptools"]["data-files"]


@pytest.mark.parametrize("parts,artifact", list(RUNTIME_CONTRACT_TREES.items()))
def test_runtime_contract_tree_is_packaged(
    parts: tuple[str, ...], artifact: str
) -> None:
    """pyproject must ship every contract tree read at runtime.

    Declaring the destination is not enough: the globs must actually match the
    artifact the loader opens, so renaming a file without updating the glob
    fails here rather than in production.
    """

    destination = "contracts/" + "/".join(parts)
    data_files = _data_files()
    assert destination in data_files, (
        f"{destination} is read at runtime by dev_health_ops but is not in "
        "[tool.setuptools.data-files]. It will be absent from the installed "
        "distribution and every read of it will raise FileNotFoundError."
    )
    relative = f"{destination}/{artifact}"
    assert any(
        fnmatch.fnmatch(relative, pattern) for pattern in data_files[destination]
    ), (
        f"{relative} is not matched by any glob for {destination}: "
        f"{data_files[destination]}"
    )


@pytest.mark.parametrize("parts,artifact", list(RUNTIME_CONTRACT_TREES.items()))
def test_every_shipped_artifact_exists_on_disk(
    parts: tuple[str, ...], artifact: str
) -> None:
    """The glob must match a file that is actually there.

    Without this, a typo'd destination would satisfy the test above by matching
    nothing at all -- a packaging declaration that ships an empty directory
    reads as success.
    """

    tree = ROOT / "contracts" / Path(*parts)
    assert (tree / artifact).is_file(), f"{tree / artifact} does not exist"


def test_resolver_prefers_the_checkout() -> None:
    for parts, artifact in RUNTIME_CONTRACT_TREES.items():
        resolved = contract_artifacts.contract_directory(*parts)
        assert (resolved / artifact).is_file(), (
            f"{artifact} did not resolve from the checkout: {resolved}"
        )


def test_resolver_falls_back_to_the_installed_data_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With no checkout above the package, resolution must use the data path.

    This is the branch that did not exist. It simulates the installed layout by
    pointing the checkout anchor at a directory with no ``contracts/`` in it --
    exactly what ``site-packages/dev_health_ops`` produces -- and asserts the
    resolver answers with sysconfig's data path rather than a path under the
    interpreter's lib directory.
    """

    monkeypatch.setattr(contract_artifacts, "_CHECKOUT_ROOT", ROOT / "does-not-exist")
    expected_root = Path(sysconfig.get_path("data")) / "contracts"
    for parts in RUNTIME_CONTRACT_TREES:
        resolved = contract_artifacts.contract_directory(*parts)
        assert resolved == expected_root.joinpath(*parts), (
            f"installed-layout resolution for {parts} returned {resolved}"
        )


def test_loaders_resolve_from_the_installed_layout() -> None:
    """Each loader -- not just the resolver -- must answer correctly when installed.

    This is the assertion that would have caught CHAOS-3933, and the one a
    checkout-only test structurally cannot make. With the checkout anchor
    pointed at a directory containing no ``contracts/``, a loader built on
    ``contract_directory`` answers under sysconfig's data path, while one built
    on a ``parents[N]`` count answers under the interpreter's lib directory --
    the exact wrong path from the production traceback.

    Run in a SUBPROCESS on purpose. ``provider_unit_route`` computes its path at
    import time, so an in-process version of this test has to
    ``importlib.reload`` it -- and reload installs a NEW module object while
    every earlier ``from ... import`` in the session keeps binding the old one.
    That divergence is not hypothetical: the reload version of this test made
    tests/test_sync_units.py::test_dispatch_sync_run_mixed_transport_routes_every_ready_pair_independently
    fail whenever it ran first in the same process, and passed cleanly on its
    own -- a full-suite-only failure that reads as someone else's flake. A fresh
    interpreter proves the same property and mutates nothing.
    """

    probe = textwrap.dedent(
        """
        import pathlib, sysconfig
        from dev_health_ops import contract_artifacts

        # Simulate site-packages: an anchor with no contracts/ beneath it.
        contract_artifacts._CHECKOUT_ROOT = pathlib.Path("/nonexistent-checkout-root")

        from dev_health_ops.sync import dispatch_routes
        from dev_health_ops.workers import provider_unit_route

        print(provider_unit_route._DEFAULT_MATRIX_CONTRACT_PATH)
        print(dispatch_routes.default_transport_routes_path())
        """
    )
    completed = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        cwd=ROOT,
        check=False,
    )
    assert completed.returncode == 0, (
        f"installed-layout probe failed:\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    printed = [line for line in completed.stdout.splitlines() if line.startswith("/")]
    assert len(printed) == 2, f"probe printed {printed!r}, expected two paths"

    data_root = Path(sysconfig.get_path("data")) / "contracts"
    for name, raw in zip(
        (
            "provider_unit_route._DEFAULT_MATRIX_CONTRACT_PATH",
            "dispatch_routes.default_transport_routes_path()",
        ),
        printed,
        strict=True,
    ):
        resolved = Path(raw)
        assert data_root in resolved.parents, (
            f"{name} resolved to {resolved} under the installed layout. It is not "
            "using contract_artifacts.contract_directory -- a parents[N] count "
            "lands in the interpreter lib directory once installed, which is "
            "how CHAOS-3933 reached production."
        )


def test_loaders_use_the_shared_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Both known loaders must go through contract_artifacts.

    Pinning the resolver rather than the string keeps a future third consumer
    from reintroducing its own parents[N] count: the count is a function of
    where a module happens to live, so moving a file one directory deeper
    breaks resolution with no other symptom.
    """

    from dev_health_ops.sync import dispatch_routes
    from dev_health_ops.workers import provider_unit_route

    matrix = provider_unit_route._DEFAULT_MATRIX_CONTRACT_PATH
    routes = dispatch_routes.default_transport_routes_path()
    for resolved in (matrix, routes):
        assert "site-packages" not in resolved.parts, (
            "a contract artifact resolved INSIDE site-packages, which means a "
            "parents[N] count leaked back in"
        )
        assert resolved.is_file(), f"{resolved} does not exist"
