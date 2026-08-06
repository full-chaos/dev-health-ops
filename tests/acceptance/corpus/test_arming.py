"""Unit coverage for ``scripts.acceptance.corpus.arming``."""

from __future__ import annotations

import pytest

from scripts.acceptance.corpus.arming import NotArmedError, require_armed


class TestRequireArmed:
    def test_armed_is_a_silent_no_op(self) -> None:
        require_armed({"ASK_DEV_LIVE_ACCEPTANCE": "1"})

    def test_missing_raises(self) -> None:
        with pytest.raises(NotArmedError, match="ASK_DEV_LIVE_ACCEPTANCE=1"):
            require_armed({})

    def test_wrong_value_raises(self) -> None:
        with pytest.raises(NotArmedError):
            require_armed({"ASK_DEV_LIVE_ACCEPTANCE": "true"})

    def test_zero_raises(self) -> None:
        with pytest.raises(NotArmedError):
            require_armed({"ASK_DEV_LIVE_ACCEPTANCE": "0"})
