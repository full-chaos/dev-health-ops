"""Resolve checked-in contract artifacts in a checkout OR an installed distribution.

Every consumer of a ``contracts/`` artifact needs the same two-step answer, and
getting it wrong is silent until runtime. ``provider_unit_route.py`` resolved
its matrix as ``Path(__file__).resolve().parents[3] / "contracts" / ...``, which
is the repository root in a source checkout and ``/usr/local/lib/python3.14`` in
an installed wheel, because the intermediate directory is ``site-packages``
rather than ``src``. Nothing failed until the Go sync-dispatch bridge moved
dispatch onto the installed ``api`` service, at which point every production
``dispatch_sync_run`` returned 500 (CHAOS-3933).

Two properties this module exists to hold:

* The anchor is the PACKAGE directory, not the calling module. Counting
  ``parents[N]`` per call site means the count is a function of where the file
  happens to live, so moving a module one directory deeper breaks resolution
  with no other symptom. ``dev_health_ops`` is always one level below ``src``.
* Resolution and packaging are ONE contract. A path this function can compute
  is still useless if ``pyproject.toml`` does not ship the tree, so the two are
  documented against each other in both places.
"""

from __future__ import annotations

import sysconfig
from pathlib import Path

# .../src/dev_health_ops -> parents[1] is the repository root in a checkout.
# In an installed distribution this resolves to the interpreter's lib directory,
# which will not contain "contracts", so the checkout branch simply misses and
# the installed branch answers.
_PACKAGE_ROOT = Path(__file__).resolve().parent
_CHECKOUT_ROOT = _PACKAGE_ROOT.parents[1]


def contract_directory(*parts: str) -> Path:
    """Return a ``contracts/`` subdirectory, preferring a source checkout.

    ``parts`` is the path BELOW ``contracts/`` -- for example
    ``contract_directory("provider-matrix", "v1")``.

    The checkout answer wins when it exists so that a developer editing a
    contract sees the edit without reinstalling. The installed answer is
    ``sysconfig.get_path("data")/contracts/...``, matching where
    ``[tool.setuptools.data-files]`` places the trees.

    Neither branch is verified to CONTAIN the artifact the caller wants: callers
    already fail closed on a missing file with a message naming it, and a
    directory-exists check here would only move that failure earlier without
    making it clearer.
    """

    relative = Path(*parts)
    checkout = _CHECKOUT_ROOT / "contracts" / relative
    if checkout.is_dir():
        return checkout
    return Path(sysconfig.get_path("data")) / "contracts" / relative
