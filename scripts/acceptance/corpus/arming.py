"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: the armed-or-throw check, factored out
as a pure function so it is unit-testable without invoking pytest's own
fixture/collection machinery.

Generalizes the existing smoke-script convention
(``os.getenv("ASK_DEV_LIVE_ACCEPTANCE") != "1"`` -> exit 64, see
``smoke_ask_dev_exact_commit.py``'s ``main()``) for a pytest-native runner:
there is no meaningful "exit code" inside a pytest test, so the live corpus
runner's session fixture converts :class:`NotArmedError` into
``pytest.fail(..., pytrace=False)`` -- a definite, red FAILED outcome, never
a ``pytest.skip`` (a skip is not unconditionally treated as non-passing by
every consumer of a pytest report the way it is inside this repo's own
``wave31_manifest.py`` execution checker; failing is unambiguous everywhere).
"""

from __future__ import annotations

import os
from collections.abc import Mapping

__all__ = ["ARM_ENV_VAR", "NotArmedError", "require_armed"]

ARM_ENV_VAR = "ASK_DEV_LIVE_ACCEPTANCE"


class NotArmedError(Exception):
    """The live-acceptance arming env var is not set to ``"1"``."""


def require_armed(env: Mapping[str, str] | None = None) -> None:
    source = env if env is not None else os.environ
    if source.get(ARM_ENV_VAR) != "1":
        raise NotArmedError(
            f"{ARM_ENV_VAR}=1 is required before the Wave 4 corpus runner "
            "touches the network -- this must fail loud, never silently "
            "skip or proceed unarmed"
        )
