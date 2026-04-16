"""Assert _extract_category_rationale compiles each category pattern at most once."""

from __future__ import annotations

import re
from unittest.mock import patch


def test_category_pattern_compiled_once_per_key():
    from dev_health_ops.api.services import work_unit_explain as mod

    # Clear any cached compile state from prior tests so we measure fresh compiles.
    if hasattr(mod, "_compiled_category_pattern"):
        mod._compiled_category_pattern.cache_clear()

    compile_calls: list[str] = []
    real_compile = re.compile

    def spy_compile(pattern, flags=0):
        compile_calls.append(pattern)
        return real_compile(pattern, flags)

    with patch.object(mod.re, "compile", side_effect=spy_compile):
        text = "feature_delivery work was extensive. Maintenance: refactoring loops."
        categories = {"feature_delivery": 0.5, "maintenance": 0.5}
        mod._extract_category_rationale(text, categories)
        mod._extract_category_rationale(text, categories)

    # Each category compiles at most once across both invocations.
    compiled_category_patterns = [p for p in compile_calls if "[^.]*" in p]
    # 2 categories, each should appear 0 or 1 time (cache hit on second call)
    counts = {
        k: sum(1 for p in compiled_category_patterns if k in p) for k in categories
    }
    for k, n in counts.items():
        assert n <= 1, f"{k} re-compiled {n} times across calls (expected <=1)"
