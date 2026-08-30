# Provider Unification Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate ~760 LOC of duplication across GitHub / GitLab / Jira / Linear providers plus related API/LLM/metrics modules by extracting shared helpers, decorators, base classes, and query builders — without changing runtime behavior.

**Architecture:** Nine focused, independently-reviewable tasks. Each task lands via TDD: failing test, minimal helper, one-by-one migration of existing callers (preserving the original names as re-exports where possible), full provider suite green, commit. Tasks 1–4, 7–9 have no blockers and may be parallelized; Task 5 depends on Task 4; Task 6 depends on Tasks 3 and 5.

**Tech Stack:** Python 3.11+ / FastAPI, PyGithub, python-gitlab, custom Atlassian shim, Linear GraphQL, ClickHouse, SQLAlchemy (async), pytest, `uv` runner.

---

## Pre-Flight Checklist

Before starting any task, an executor must:

- [ ] Confirm cwd is the repo root: `/Users/chris/projects/full-chaos/dev-health/ops`
- [ ] Confirm current branch is NOT `main`. If it is, create a feature branch:
  ```bash
  cd /Users/chris/projects/full-chaos/dev-health/ops
  git checkout -b refactor/provider-unification
  ```
- [ ] Verify baseline tests pass:
  ```bash
  uv run pytest tests/providers -q
  ```
  If ANY tests fail on `main` before you start, STOP — investigate before refactoring.

Commands used throughout this plan:
- Run provider tests: `uv run pytest tests/providers -q`
- Run a single test: `uv run pytest tests/path/to/test.py::TestClass::test_name -v`
- Run a file: `uv run pytest tests/path/to/test.py -v`

---

## Task 1: Extract Normalize Helpers (`_as_str`, `_as_int`, `_as_node_list`, `_labels_from_nodes`, `_get`)

**Estimated diff size:** +110 / −55 LOC.

**Rationale:** `providers/github/normalize.py` defines `_as_dict`, `_as_str`, `_as_int`, `_as_node_list`, `_labels_from_nodes`. `providers/gitlab/normalize.py` defines `_get`. `providers/linear/normalize.py` defines a variadic `_get(obj, *keys)`. `providers/jira/normalize.py` has a different `_get_field` that descends into `.fields` — that one is Jira-specific and STAYS put. We consolidate the overlapping helpers into `src/dev_health_ops/providers/normalize_helpers.py` and re-export from the per-provider modules (keep existing private names) so callers and tests don't break.

**Files:**
- Create: `src/dev_health_ops/providers/normalize_helpers.py`
- Create: `tests/providers/test_normalize_helpers.py`
- Modify: `src/dev_health_ops/providers/github/normalize.py:94-135` (replace inline `_as_*` defs with imports)
- Modify: `src/dev_health_ops/providers/gitlab/normalize.py:33-37` (replace inline `_get` with import)
- Modify: `src/dev_health_ops/providers/linear/normalize.py:49-57` (replace inline `_get` with import)

- [ ] **Step 1.1: Write failing test for `normalize_helpers`**

Create `tests/providers/test_normalize_helpers.py`:

```python
"""Tests for shared normalize helpers."""

from __future__ import annotations

from dev_health_ops.providers.normalize_helpers import (
    as_dict,
    as_int,
    as_node_list,
    as_str,
    get_attr,
    get_nested,
    labels_from_nodes,
)


class TestAsStr:
    def test_none_returns_none(self) -> None:
        assert as_str(None) is None

    def test_string_passthrough(self) -> None:
        assert as_str("hello") == "hello"

    def test_int_coerced(self) -> None:
        assert as_str(42) == "42"

    def test_empty_string_kept(self) -> None:
        assert as_str("") == ""


class TestAsInt:
    def test_none_returns_none(self) -> None:
        assert as_int(None) is None

    def test_bool_coerced(self) -> None:
        assert as_int(True) == 1
        assert as_int(False) == 0

    def test_float_truncated(self) -> None:
        assert as_int(3.9) == 3

    def test_numeric_string(self) -> None:
        assert as_int("42") == 42

    def test_non_numeric_string_returns_none(self) -> None:
        assert as_int("abc") is None

    def test_dict_returns_none(self) -> None:
        assert as_int({"x": 1}) is None


class TestAsDict:
    def test_dict_passthrough(self) -> None:
        assert as_dict({"a": 1}) == {"a": 1}

    def test_non_dict_returns_empty(self) -> None:
        assert as_dict(None) == {}
        assert as_dict("x") == {}
        assert as_dict([1, 2]) == {}


class TestAsNodeList:
    def test_list_of_dicts(self) -> None:
        assert as_node_list([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]

    def test_mixed_filtered(self) -> None:
        assert as_node_list([{"a": 1}, "x", None, {"b": 2}]) == [{"a": 1}, {"b": 2}]

    def test_non_list_returns_empty(self) -> None:
        assert as_node_list(None) == []
        assert as_node_list({"x": 1}) == []


class TestLabelsFromNodes:
    def test_dict_nodes(self) -> None:
        assert labels_from_nodes([{"name": "bug"}, {"name": "ui"}]) == ["bug", "ui"]

    def test_object_nodes(self) -> None:
        class N:
            def __init__(self, name: str) -> None:
                self.name = name

        assert labels_from_nodes([N("one"), N("two")]) == ["one", "two"]

    def test_none_returns_empty(self) -> None:
        assert labels_from_nodes(None) == []

    def test_missing_name_skipped(self) -> None:
        assert labels_from_nodes([{"x": 1}, {"name": "keep"}]) == ["keep"]


class TestGetAttr:
    def test_dict_lookup(self) -> None:
        assert get_attr({"a": 1}, "a") == 1

    def test_attribute_lookup(self) -> None:
        class Obj:
            x = "val"

        assert get_attr(Obj(), "x") == "val"

    def test_missing_returns_none(self) -> None:
        assert get_attr({}, "missing") is None
        assert get_attr(object(), "missing") is None


class TestGetNested:
    def test_single_key_dict(self) -> None:
        assert get_nested({"a": 1}, "a") == 1

    def test_chain_dict(self) -> None:
        assert get_nested({"a": {"b": {"c": 42}}}, "a", "b", "c") == 42

    def test_chain_mixed(self) -> None:
        class Leaf:
            def __init__(self, v: int) -> None:
                self.v = v

        assert get_nested({"node": Leaf(7)}, "node", "v") == 7

    def test_chain_none_short_circuits(self) -> None:
        assert get_nested({"a": None}, "a", "b") is None
```

- [ ] **Step 1.2: Run test — expect fail (module missing)**

```bash
uv run pytest tests/providers/test_normalize_helpers.py -v
```

Expected output: `ModuleNotFoundError: No module named 'dev_health_ops.providers.normalize_helpers'`.

- [ ] **Step 1.3: Implement `normalize_helpers.py`**

Create `src/dev_health_ops/providers/normalize_helpers.py`:

```python
"""Shared shape-agnostic helpers used by provider normalize modules.

These helpers accept raw API payloads that may be either ``dict``-shaped
(GraphQL / REST JSON) or object-shaped (PyGithub / python-gitlab / linear
mock-classes) and coerce them into predictable Python types.

Jira's ``_get_field`` descends into ``.fields`` specifically and is NOT
duplicated here — it stays in ``providers.jira.normalize``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

__all__ = [
    "as_dict",
    "as_str",
    "as_int",
    "as_node_list",
    "labels_from_nodes",
    "get_attr",
    "get_nested",
]


class _Named(Protocol):
    name: object


def as_dict(value: object) -> dict[str, object]:
    """Return ``value`` if it is a dict, else an empty dict."""
    return value if isinstance(value, dict) else {}


def as_str(value: object) -> str | None:
    """Coerce ``value`` to a string. ``None`` stays ``None``."""
    if value is None:
        return None
    return value if isinstance(value, str) else str(value)


def as_int(value: object) -> int | None:
    """Coerce ``value`` to an int. Bool -> 0/1. Non-numeric returns ``None``."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    return None


def as_node_list(value: object) -> list[dict[str, object]]:
    """Return ``value`` as a list of dicts (filtering non-dicts)."""
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def labels_from_nodes(
    nodes: Sequence[Mapping[str, object] | _Named] | None,
) -> list[str]:
    """Extract ``name`` strings from a sequence of dicts or objects."""
    labels: list[str] = []
    for node in nodes or []:
        name = (
            (node or {}).get("name")
            if isinstance(node, dict)
            else getattr(node, "name", None)
        )
        if name:
            labels.append(str(name))
    return labels


def get_attr(obj: Any, key: str) -> Any:
    """Single-level lookup: ``dict.get(key)`` or ``getattr(obj, key, None)``."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def get_nested(obj: Any, *keys: str) -> Any:
    """Walk nested dict/object keys, short-circuiting on ``None``.

    ``get_nested(issue, "assignee", "email")`` is equivalent to
    ``issue.get("assignee", {}).get("email")`` or ``issue.assignee.email``,
    whichever applies at each level.
    """
    for key in keys:
        if obj is None:
            return None
        if isinstance(obj, dict):
            obj = obj.get(key)
        else:
            obj = getattr(obj, key, None)
    return obj
```

- [ ] **Step 1.4: Run new test — expect pass**

```bash
uv run pytest tests/providers/test_normalize_helpers.py -v
```

Expected: all tests pass.

- [ ] **Step 1.5: Migrate `github/normalize.py` to use shared helpers**

Edit `src/dev_health_ops/providers/github/normalize.py`:

Replace the block at lines 94-135 (the `_as_dict`, `_as_str`, `_as_int`, `_as_node_list`, `_labels_from_nodes` definitions) with imports + thin re-exports so any in-module callers still work:

```python
from dev_health_ops.providers.normalize_helpers import (
    as_dict as _as_dict,
    as_int as _as_int,
    as_node_list as _as_node_list,
    as_str as _as_str,
    labels_from_nodes as _labels_from_nodes,
)
```

Delete lines 94-135 (the six helper definitions). Put the imports near the top of the file, after the existing `from dev_health_ops.providers.status_mapping import StatusMapping` import.

- [ ] **Step 1.6: Migrate `gitlab/normalize.py`**

Edit `src/dev_health_ops/providers/gitlab/normalize.py`:

Replace lines 33-37 (the `_get` definition) with:

```python
from dev_health_ops.providers.normalize_helpers import get_attr as _get
```

Place the import alongside the existing imports near the top of the file. Delete the inline `def _get(...)`.

- [ ] **Step 1.7: Migrate `linear/normalize.py`**

Linear's `_get` is variadic (`*keys`). Edit `src/dev_health_ops/providers/linear/normalize.py`:

Replace lines 49-57 (the `_get` definition) with:

```python
from dev_health_ops.providers.normalize_helpers import get_nested as _get
```

Delete the inline `def _get(...)`. Place the import alongside existing imports.

- [ ] **Step 1.8: Run full provider test suite**

```bash
uv run pytest tests/providers -q
uv run pytest tests/test_github_provider.py tests/test_gitlab_provider.py tests/test_linear_provider.py -q
```

Expected: all tests pass with no behavior changes.

- [ ] **Step 1.9: Commit**

```bash
git add src/dev_health_ops/providers/normalize_helpers.py \
        tests/providers/test_normalize_helpers.py \
        src/dev_health_ops/providers/github/normalize.py \
        src/dev_health_ops/providers/gitlab/normalize.py \
        src/dev_health_ops/providers/linear/normalize.py
git commit -m "$(cat <<'EOF'
refactor(providers): extract shared normalize helpers

Consolidate duplicated shape-coercion helpers (as_str, as_int,
as_node_list, labels_from_nodes, get_attr, get_nested) into
providers/normalize_helpers.py. Keeps the original private aliases in
github/gitlab/linear normalize modules for backwards compatibility.
Jira's field-specific `_get_field` stays local.
EOF
)"
```

---

## Task 2: Generic `_iter_with_limit` Helper for GitHub Client

**Estimated diff size:** +35 / −40 LOC.

**Rationale:** Six methods in `providers/github/client.py` repeat the same bounded-iteration loop. Extract into a private helper on the class.

**Files:**
- Modify: `src/dev_health_ops/providers/github/client.py` (add helper; update 6 methods)
- Create: `tests/providers/test_github_iter_with_limit.py`

- [ ] **Step 2.1: Write failing test**

Create `tests/providers/test_github_iter_with_limit.py`:

```python
"""Test GitHubWorkClient._iter_with_limit generic helper."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient


@pytest.fixture
def client() -> GitHubWorkClient:
    with patch("github.Github"), patch(
        "dev_health_ops.providers.github.client.GitHubGraphQLClient"
    ):
        return GitHubWorkClient(auth=GitHubAuth(token="fake"))


class TestIterWithLimit:
    def test_no_limit_yields_all(self, client: GitHubWorkClient) -> None:
        source = [MagicMock(), MagicMock(), MagicMock()]
        result = list(client._iter_with_limit(source, limit=None))
        assert result == source

    def test_limit_truncates(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5]
        result = list(client._iter_with_limit(source, limit=3))
        assert result == [1, 2, 3]

    def test_limit_zero_yields_none(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3]
        result = list(client._iter_with_limit(source, limit=0))
        assert result == []

    def test_limit_larger_than_source(self, client: GitHubWorkClient) -> None:
        source = [1, 2]
        result = list(client._iter_with_limit(source, limit=10))
        assert result == [1, 2]

    def test_custom_filter_skips_items(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5]

        def skip_evens(x: Any) -> bool:
            return x % 2 == 0  # predicate returns True when item should be SKIPPED

        result = list(client._iter_with_limit(source, limit=None, skip=skip_evens))
        assert result == [1, 3, 5]

    def test_filter_plus_limit(self, client: GitHubWorkClient) -> None:
        source = [1, 2, 3, 4, 5, 6]

        def skip_evens(x: Any) -> bool:
            return x % 2 == 0

        result = list(client._iter_with_limit(source, limit=2, skip=skip_evens))
        assert result == [1, 3]
```

- [ ] **Step 2.2: Run test — expect fail (`_iter_with_limit` not on class)**

```bash
uv run pytest tests/providers/test_github_iter_with_limit.py -v
```

Expected: `AttributeError: 'GitHubWorkClient' object has no attribute '_iter_with_limit'`.

- [ ] **Step 2.3: Add the helper method**

Edit `src/dev_health_ops/providers/github/client.py`. Directly after the `get_repo` method (line 94), add:

```python
    def _iter_with_limit(
        self,
        source: Iterable[Any],
        *,
        limit: int | None,
        skip: Callable[[Any], bool] | None = None,
    ) -> Iterable[Any]:
        """Yield items from ``source`` respecting ``limit`` and optional skip filter.

        ``skip`` receives each item and returns ``True`` when the item should be
        excluded (used for PR-vs-issue filtering on the issues feed).
        """
        if limit is not None and int(limit) <= 0:
            return
        count = 0
        for item in source:
            if skip is not None and skip(item):
                continue
            yield item
            count += 1
            if limit is not None and count >= int(limit):
                return
```

Add to the imports at the top of the file (line 4):

```python
from collections.abc import Callable, Iterable
```

Replace the existing `from collections.abc import Iterable` import.

- [ ] **Step 2.4: Run test — expect pass**

```bash
uv run pytest tests/providers/test_github_iter_with_limit.py -v
```

Expected: all tests pass.

- [ ] **Step 2.5: Migrate `iter_issues` to use the helper**

Edit `iter_issues` (lines 96-115). Replace the body with:

```python
    def iter_issues(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        since: datetime | None = None,
        limit: int | None = None,
    ) -> Iterable[_GitHubIssueLike]:
        gh_repo = self.get_repo(owner=owner, repo=repo)
        issues = gh_repo.get_issues(state=state, since=since)
        yield from self._iter_with_limit(
            issues,
            limit=limit,
            skip=lambda issue: getattr(issue, "pull_request", None) is not None,
        )
```

- [ ] **Step 2.6: Migrate `iter_issue_events`**

Replace lines 117-129 body with:

```python
    def iter_issue_events(
        self, issue: _GitHubIssueLike, *, limit: int | None = None
    ) -> Iterable[object]:
        """Iterate issue events via REST."""
        yield from self._iter_with_limit(issue.get_events(), limit=limit)
```

- [ ] **Step 2.7: Migrate `iter_pull_requests`**

Replace lines 131-151 body with:

```python
    def iter_pull_requests(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        sort: str = "updated",
        direction: str = "desc",
        limit: int | None = None,
    ) -> Iterable[_GitHubPullRequestLike]:
        """Iterate pull requests in a repository via REST."""
        gh_repo = self.get_repo(owner=owner, repo=repo)
        pulls = gh_repo.get_pulls(state=state, sort=sort, direction=direction)
        yield from self._iter_with_limit(pulls, limit=limit)
```

- [ ] **Step 2.8: Migrate `iter_issue_comments`**

Replace lines 153-165 body with:

```python
    def iter_issue_comments(
        self, issue: _GitHubIssueLike, *, limit: int | None = None
    ) -> Iterable[object]:
        """Iterate comments on an issue via REST."""
        yield from self._iter_with_limit(issue.get_comments(), limit=limit)
```

- [ ] **Step 2.9: Migrate `iter_pr_review_comments`**

Replace lines 176-188 body with:

```python
    def iter_pr_review_comments(
        self, pr: _GitHubPullRequestLike, *, limit: int | None = None
    ) -> Iterable[object]:
        """Iterate review comments on a pull request."""
        yield from self._iter_with_limit(pr.get_review_comments(), limit=limit)
```

- [ ] **Step 2.10: Migrate `iter_repo_milestones`**

Replace lines 190-208 body with:

```python
    def iter_repo_milestones(
        self,
        *,
        owner: str,
        repo: str,
        state: str = "all",
        limit: int | None = None,
    ) -> Iterable[object]:
        """Iterate milestones in a repository via REST."""
        gh_repo = self.get_repo(owner=owner, repo=repo)
        yield from self._iter_with_limit(gh_repo.get_milestones(state=state), limit=limit)
```

- [ ] **Step 2.11: Run full provider suite**

```bash
uv run pytest tests/providers tests/test_github_provider.py -q
```

Expected: all pass.

- [ ] **Step 2.12: Commit**

```bash
git add src/dev_health_ops/providers/github/client.py tests/providers/test_github_iter_with_limit.py
git commit -m "$(cat <<'EOF'
refactor(providers/github): extract _iter_with_limit helper

Replace 6 nearly-identical bounded-iteration loops in GitHubWorkClient
(iter_issues, iter_issue_events, iter_pull_requests, iter_issue_comments,
iter_pr_review_comments, iter_repo_milestones) with a single private
helper. No behavior changes.
EOF
)"
```

---

## Task 3: Rate-Limit Context Manager (`gate_call`)

**Estimated diff size:** +90 / −80 LOC.

**Rationale:** `providers/gitlab/client.py` has 10+ methods wrapping API calls with `self.gate.wait_sync()` / `self.gate.reset()` / `self.gate.penalize(None)`. `providers/jira/client.py::_request_json` has a similar pattern (with `penalize` driven by HTTP 429 `Retry-After` header). Extract a context-manager-based helper so the rate-limit logic lives in one place. Keep the header-aware behavior for Jira by letting the caller pass a `retry_after_provider` hook.

**Files:**
- Create: `src/dev_health_ops/providers/_ratelimit.py`
- Create: `tests/providers/test_ratelimit.py`
- Modify: `src/dev_health_ops/providers/gitlab/client.py` (wrap API calls)
- Modify: `src/dev_health_ops/providers/jira/client.py` (wrap `_request_json`)

- [ ] **Step 3.1: Write failing test**

Create `tests/providers/test_ratelimit.py`:

```python
"""Tests for providers._ratelimit.gate_call context manager."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dev_health_ops.connectors.utils.rate_limit_queue import (
    RateLimitConfig,
    RateLimitGate,
)
from dev_health_ops.providers._ratelimit import gate_call


class TestGateCall:
    def test_success_calls_wait_then_reset(self) -> None:
        gate = MagicMock(spec=RateLimitGate)
        with gate_call(gate):
            pass
        gate.wait_sync.assert_called_once()
        gate.reset.assert_called_once()
        gate.penalize.assert_not_called()

    def test_failure_calls_penalize_and_reraises(self) -> None:
        gate = MagicMock(spec=RateLimitGate)

        with pytest.raises(RuntimeError, match="boom"):
            with gate_call(gate):
                raise RuntimeError("boom")

        gate.wait_sync.assert_called_once()
        gate.reset.assert_not_called()
        gate.penalize.assert_called_once_with(None)

    def test_failure_with_explicit_retry_after(self) -> None:
        gate = MagicMock(spec=RateLimitGate)

        with pytest.raises(ValueError):
            with gate_call(gate, retry_after=12.5):
                raise ValueError("throttled")

        gate.penalize.assert_called_once_with(12.5)

    def test_swallow_flag_suppresses_exception(self) -> None:
        gate = MagicMock(spec=RateLimitGate)
        with gate_call(gate, swallow=True):
            raise RuntimeError("swallow me")

        gate.penalize.assert_called_once_with(None)

    def test_real_gate_integration_success(self) -> None:
        gate = RateLimitGate(RateLimitConfig(initial_backoff_seconds=0.01))
        # Use the real gate to confirm wait/reset don't raise
        with gate_call(gate):
            value = 42
        assert value == 42
```

- [ ] **Step 3.2: Run test — expect fail (module missing)**

```bash
uv run pytest tests/providers/test_ratelimit.py -v
```

Expected: `ModuleNotFoundError: No module named 'dev_health_ops.providers._ratelimit'`.

- [ ] **Step 3.3: Implement `_ratelimit.py`**

Create `src/dev_health_ops/providers/_ratelimit.py`:

```python
"""Rate-limit helper shared across provider clients.

Wraps the common ``wait_sync() / reset() / penalize()`` boilerplate around
API calls into a single context manager. Use as::

    with gate_call(self.gate):
        result = self.api.do_something()

On normal exit the gate is reset (clearing backoff state). On exception
the gate is penalized (deferring the next allowed call). The exception
propagates by default; pass ``swallow=True`` when the caller prefers
to log-and-continue.

For explicit server-provided delays (e.g. HTTP 429 ``Retry-After``
header), use the helper form::

    with gate_call(self.gate, retry_after=retry_after_seconds):
        ...

``retry_after`` is passed through to ``gate.penalize`` only on failure.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from dev_health_ops.connectors.utils.rate_limit_queue import RateLimitGate


@contextmanager
def gate_call(
    gate: RateLimitGate,
    *,
    retry_after: float | None = None,
    swallow: bool = False,
) -> Iterator[None]:
    """Context manager that wraps a gated API call.

    Args:
        gate: the ``RateLimitGate`` to use.
        retry_after: optional explicit delay (seconds) to pass to
            ``gate.penalize`` if the wrapped block raises.
        swallow: if True, exceptions are logged via ``penalize`` and
            suppressed (the ``with`` block exits normally).
    """
    gate.wait_sync()
    try:
        yield
    except Exception:
        gate.penalize(retry_after)
        if swallow:
            return
        raise
    else:
        gate.reset()


def penalize_from_response(
    gate: RateLimitGate, response: Any, *, default: float | None = None
) -> float:
    """Apply a penalty driven by an HTTP response's ``Retry-After`` header.

    Returns the applied delay (seconds). ``response`` is any object with a
    ``headers`` mapping. Invalid / missing headers fall back to ``default``.
    """
    retry_after = default
    try:
        raw = response.headers.get("Retry-After") if response is not None else None
        if raw is not None:
            retry_after = float(raw)
    except (TypeError, ValueError):
        retry_after = default
    return gate.penalize(retry_after)
```

- [ ] **Step 3.4: Run test — expect pass**

```bash
uv run pytest tests/providers/test_ratelimit.py -v
```

Expected: all tests pass.

- [ ] **Step 3.5: Migrate `gitlab/client.py::get_project`**

Edit `src/dev_health_ops/providers/gitlab/client.py`. Add to the top of the file (after the existing `RateLimitGate` import):

```python
from dev_health_ops.providers._ratelimit import gate_call
```

Replace the current `get_project` body (lines 78-86):

```python
    def get_project(self, project_id_or_path: str) -> Any:
        with gate_call(self.gate):
            return self.gl.projects.get(project_id_or_path)
```

- [ ] **Step 3.6: Migrate remaining gitlab methods using `wait_sync`/`reset`/`penalize`**

Using the line locations from the audit (Finding 3), migrate each method one-by-one. For each block that looks like:

```python
try:
    self.gate.wait_sync()
    result = <api call>
    self.gate.reset()
    return result
except Exception as exc:
    logger.debug("...", exc)
    self.gate.penalize(None)
    return <fallback>
```

…rewrite as:

```python
try:
    with gate_call(self.gate):
        result = <api call>
    return result
except Exception as exc:
    logger.debug("...", exc)
    return <fallback>
```

Affected methods (by line range, inclusive of `try:` to end of block):
- `get_issue_notes` (lines 129-144)
- `get_mr_notes` (lines 146-162)
- `get_issue_label_events` (lines ~163-181)
- `get_issue_state_events` (lines ~182-200)
- `get_mr_state_events` (lines ~201-219)
- `get_issue_links` (lines ~220-234)
- `get_epic_state_events` (lines ~235-263)
- `iter_group_milestones` (lines ~264-278)
- `iter_project_milestones` (lines ~279-290, uses `gate_call` without swallow)
- Remaining blocks at lines 308-328, 339-345, 358-364, 375-383

Work method-by-method. After each method edit, run:

```bash
uv run pytest tests/test_gitlab_provider.py -q
```

If a block's fallback branch does NOT exist (no `except`), use:

```python
with gate_call(self.gate):
    result = <api call>
return result
```

- [ ] **Step 3.7: Migrate `jira/client.py::_request_json`**

Edit `src/dev_health_ops/providers/jira/client.py`. Add the imports near the top:

```python
from dev_health_ops.providers._ratelimit import gate_call, penalize_from_response
```

Replace the body of `_request_json` (lines 114-141) with:

```python
    def _request_json(self, *, path: str, params: dict[str, Any]) -> dict[str, Any]:
        import requests

        url = self._url(path)
        self.gate.wait_sync()
        try:
            resp = self.session.get(url, params=params, timeout=self.timeout_seconds)
            if resp.status_code == 429:
                applied = penalize_from_response(self.gate, resp)
                logger.info("Jira rate limited; backoff %.1fs (HTTP 429)", applied)
            resp.raise_for_status()
            self.gate.reset()
            data = resp.json()
            return data if isinstance(data, dict) else {}
        except requests.HTTPError as exc:
            try:
                body = exc.response.text if exc.response is not None else ""
            except Exception:
                body = ""
            logger.debug(
                "Jira request failed: %s %s params=%s body=%s", "GET", url, params, body
            )
            raise
```

Note: we do NOT convert `_request_json` to use `gate_call` because Jira's HTTP 429 handling must happen BEFORE `raise_for_status()`, and the penalty uses the response header (not `None`). We keep the explicit `wait_sync()` / `reset()` but delegate the header-parsing to `penalize_from_response`.

- [ ] **Step 3.8: Run full suite**

```bash
uv run pytest tests/providers tests/test_github_provider.py tests/test_gitlab_provider.py tests/test_linear_provider.py -q
```

Expected: all pass.

- [ ] **Step 3.9: Commit**

```bash
git add src/dev_health_ops/providers/_ratelimit.py \
        tests/providers/test_ratelimit.py \
        src/dev_health_ops/providers/gitlab/client.py \
        src/dev_health_ops/providers/jira/client.py
git commit -m "$(cat <<'EOF'
refactor(providers): extract rate-limit gate_call context manager

Replace duplicated wait_sync/reset/penalize blocks in gitlab/client.py
(10+ sites) with a single context manager in providers/_ratelimit.py.
Jira's _request_json retains explicit header-driven penalty handling
via penalize_from_response helper. No behavior changes.
EOF
)"
```

---

## Task 4: Consolidate `_env_flag` Helper

**Estimated diff size:** +40 / −40 LOC.

**Rationale:** All four `provider.py` files define an identical `_env_flag(name, default)` helper. Move it to `providers/utils.py`. Github's `provider.py` also has an inline `int(os.getenv("GITHUB_COMMENTS_LIMIT") or ...)` pattern with warning-on-ValueError — add an `env_int` helper and route Github through it.

**Files:**
- Create: `src/dev_health_ops/providers/utils.py`
- Create: `tests/providers/test_utils.py`
- Modify: `src/dev_health_ops/providers/github/provider.py:35-44` (replace `_env_flag`), `lines 154-163` (replace `raw_comments_limit` block)
- Modify: `src/dev_health_ops/providers/gitlab/provider.py:36-45`
- Modify: `src/dev_health_ops/providers/jira/provider.py:36-45`
- Modify: `src/dev_health_ops/providers/linear/provider.py:34-43`

- [ ] **Step 4.1: Write failing test**

Create `tests/providers/test_utils.py`:

```python
"""Tests for providers/utils.py env parsing helpers."""

from __future__ import annotations

import pytest

from dev_health_ops.providers.utils import env_flag, env_int


class TestEnvFlag:
    def test_unset_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_FLAG", raising=False)
        assert env_flag("TEST_FLAG", True) is True
        assert env_flag("TEST_FLAG", False) is False

    @pytest.mark.parametrize("value", ["1", "true", "True", "YES", "on", "ON"])
    def test_truthy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("TEST_FLAG", value)
        assert env_flag("TEST_FLAG", False) is True

    @pytest.mark.parametrize("value", ["0", "false", "FALSE", "no", "NO", "off", "Off"])
    def test_falsy_values(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        monkeypatch.setenv("TEST_FLAG", value)
        assert env_flag("TEST_FLAG", True) is False

    def test_unknown_value_returns_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TEST_FLAG", "maybe")
        assert env_flag("TEST_FLAG", True) is True
        assert env_flag("TEST_FLAG", False) is False

    def test_whitespace_trimmed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_FLAG", "  true  ")
        assert env_flag("TEST_FLAG", False) is True


class TestEnvInt:
    def test_unset_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("TEST_INT", raising=False)
        assert env_int("TEST_INT", 7) == 7

    def test_valid_int(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "42")
        assert env_int("TEST_INT", 0) == 42

    def test_invalid_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "notanumber")
        assert env_int("TEST_INT", 99) == 99

    def test_empty_returns_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "")
        assert env_int("TEST_INT", 99) == 99

    def test_negative_allowed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("TEST_INT", "-5")
        assert env_int("TEST_INT", 0) == -5
```

- [ ] **Step 4.2: Run test — expect fail (module missing)**

```bash
uv run pytest tests/providers/test_utils.py -v
```

Expected: `ModuleNotFoundError: No module named 'dev_health_ops.providers.utils'`.

- [ ] **Step 4.3: Create `utils.py`**

Create `src/dev_health_ops/providers/utils.py`:

```python
"""Shared utilities for provider modules: env parsing, etc."""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

__all__ = ["env_flag", "env_int"]

_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}


def env_flag(name: str, default: bool) -> bool:
    """Read a boolean environment variable.

    Truthy values (case-insensitive, whitespace-trimmed): ``1``, ``true``,
    ``yes``, ``on``. Falsy: ``0``, ``false``, ``no``, ``off``. Any other
    value (or unset) returns ``default``.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in _TRUTHY:
        return True
    if normalized in _FALSY:
        return False
    return default


def env_int(name: str, default: int) -> int:
    """Read an integer environment variable, falling back to ``default``.

    Logs a warning when the variable is set but not parseable as an int.
    """
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid %s value %r; falling back to %d", name, raw, default)
        return default
```

- [ ] **Step 4.4: Run test — expect pass**

```bash
uv run pytest tests/providers/test_utils.py -v
```

Expected: all tests pass.

- [ ] **Step 4.5: Migrate `github/provider.py`**

Edit `src/dev_health_ops/providers/github/provider.py`. Add to the imports block (after line 30):

```python
from dev_health_ops.providers.utils import env_flag as _env_flag
from dev_health_ops.providers.utils import env_int
```

Delete lines 35-44 (the inline `_env_flag` definition).

Replace the `raw_comments_limit` block at lines 154-163:

```python
        comments_limit = env_int("GITHUB_COMMENTS_LIMIT", 500)
```

- [ ] **Step 4.6: Migrate `gitlab/provider.py`**

Edit `src/dev_health_ops/providers/gitlab/provider.py`. Add to imports:

```python
from dev_health_ops.providers.utils import env_flag as _env_flag
```

Delete lines 36-45 (the inline `_env_flag` definition).

- [ ] **Step 4.7: Migrate `jira/provider.py`**

Edit `src/dev_health_ops/providers/jira/provider.py`. Add to imports:

```python
from dev_health_ops.providers.utils import env_flag as _env_flag
```

Delete lines 36-45 (the inline `_env_flag` definition).

- [ ] **Step 4.8: Migrate `linear/provider.py`**

Edit `src/dev_health_ops/providers/linear/provider.py`. Add to imports:

```python
from dev_health_ops.providers.utils import env_flag as _env_flag
```

Delete lines 34-43 (the inline `_env_flag` definition).

- [ ] **Step 4.9: Run full suite**

```bash
uv run pytest tests/providers -q
uv run pytest tests/test_github_provider.py tests/test_gitlab_provider.py tests/test_linear_provider.py -q
```

Expected: all pass.

- [ ] **Step 4.10: Commit**

```bash
git add src/dev_health_ops/providers/utils.py \
        tests/providers/test_utils.py \
        src/dev_health_ops/providers/github/provider.py \
        src/dev_health_ops/providers/gitlab/provider.py \
        src/dev_health_ops/providers/jira/provider.py \
        src/dev_health_ops/providers/linear/provider.py
git commit -m "$(cat <<'EOF'
refactor(providers): consolidate _env_flag and add env_int helper

Move four identical _env_flag implementations into providers/utils.py.
Add env_int helper; route GitHub provider's GITHUB_COMMENTS_LIMIT
parsing through it (removes the inline int(os.getenv(...)) block).
Local private aliases (_env_flag) preserved so call-sites stay
unchanged.
EOF
)"
```

---

## Task 5: Unified `from_env()` Base via `EnvSpec`

**Estimated diff size:** +110 / −55 LOC.

**Rationale:** Three client classes (`GitLabWorkClient`, `JiraClient`, `LinearClient`) each define an ad-hoc `from_env` classmethod. GitHub's `GitHubWorkClient` reads env vars inline in `GitHubProvider.ingest`. Introduce a small `EnvSpec` dataclass + helper that declares required/optional env-var names and produces a dict ready to pass to the auth constructor. Add `GitHubWorkClient.from_env()` using the same pattern.

**Dependency:** Task 4 must be complete (this task uses `providers.utils` as the landing zone for the helper).

**Files:**
- Modify: `src/dev_health_ops/providers/utils.py` (add `EnvSpec`, `read_env_spec`)
- Modify: `tests/providers/test_utils.py` (add tests for `EnvSpec`)
- Modify: `src/dev_health_ops/providers/github/client.py` (add `from_env`)
- Modify: `src/dev_health_ops/providers/gitlab/client.py:70-76`
- Modify: `src/dev_health_ops/providers/jira/client.py:88-103`
- Modify: `src/dev_health_ops/providers/linear/client.py:335-340`
- Modify: `src/dev_health_ops/providers/github/provider.py:135-141` (replace inline env read with `GitHubWorkClient.from_env()`)

- [ ] **Step 5.1: Append failing test to `tests/providers/test_utils.py`**

Append to `tests/providers/test_utils.py`:

```python
from dev_health_ops.providers.utils import EnvSpec, read_env_spec


class TestReadEnvSpec:
    def test_all_required_present(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_TOKEN", "abc")
        monkeypatch.setenv("MY_URL", "https://x")
        spec = EnvSpec(
            required={"token": "MY_TOKEN", "url": "MY_URL"},
            optional={},
            missing_error="MY_TOKEN and MY_URL are required",
        )
        assert read_env_spec(spec) == {"token": "abc", "url": "https://x"}

    def test_required_missing_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MY_TOKEN", raising=False)
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={},
            missing_error="Token required (set MY_TOKEN)",
        )
        with pytest.raises(ValueError, match="Token required"):
            read_env_spec(spec)

    def test_required_empty_string_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("MY_TOKEN", "")
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={},
            missing_error="Token required",
        )
        with pytest.raises(ValueError, match="Token required"):
            read_env_spec(spec)

    def test_optional_with_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_TOKEN", "abc")
        monkeypatch.delenv("MY_URL", raising=False)
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={"url": ("MY_URL", "https://default.example")},
            missing_error="required",
        )
        assert read_env_spec(spec) == {
            "token": "abc",
            "url": "https://default.example",
        }

    def test_optional_none_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_TOKEN", "abc")
        monkeypatch.delenv("MY_URL", raising=False)
        spec = EnvSpec(
            required={"token": "MY_TOKEN"},
            optional={"url": ("MY_URL", None)},
            missing_error="required",
        )
        assert read_env_spec(spec) == {"token": "abc", "url": None}

    def test_multiple_required_missing_lists_all(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("A", raising=False)
        monkeypatch.delenv("B", raising=False)
        spec = EnvSpec(
            required={"a": "A", "b": "B"},
            optional={},
            missing_error="A and B are required",
        )
        with pytest.raises(ValueError, match="A and B are required"):
            read_env_spec(spec)
```

- [ ] **Step 5.2: Run the new tests — expect fail**

```bash
uv run pytest tests/providers/test_utils.py::TestReadEnvSpec -v
```

Expected: `ImportError: cannot import name 'EnvSpec'`.

- [ ] **Step 5.3: Add `EnvSpec` + `read_env_spec` to `utils.py`**

Edit `src/dev_health_ops/providers/utils.py`. Append at the bottom:

```python
from dataclasses import dataclass, field


@dataclass(frozen=True)
class EnvSpec:
    """Declarative specification of env vars for a client's ``from_env``.

    ``required``: mapping of ``field_name -> ENV_VAR_NAME``. Missing or
        empty values cause ``read_env_spec`` to raise ``ValueError`` with
        ``missing_error`` as the message.
    ``optional``: mapping of ``field_name -> (ENV_VAR_NAME, default)``.
        Unset env vars fall back to ``default`` (may be ``None``).
    ``missing_error``: human-readable error message used when any
        required var is missing. Include the env var names so the error
        is actionable.
    """

    required: dict[str, str] = field(default_factory=dict)
    optional: dict[str, tuple[str, object]] = field(default_factory=dict)
    missing_error: str = "Required environment variables missing"


def read_env_spec(spec: EnvSpec) -> dict[str, object]:
    """Read env vars as declared by ``spec``.

    Raises ``ValueError(spec.missing_error)`` if any required var is
    missing or empty. Returns a dict suitable for passing as kwargs to
    the auth dataclass / client constructor.
    """
    result: dict[str, object] = {}
    for key, env_name in spec.required.items():
        value = os.getenv(env_name) or ""
        if not value:
            raise ValueError(spec.missing_error)
        result[key] = value
    for key, (env_name, default) in spec.optional.items():
        value = os.getenv(env_name)
        result[key] = value if value else default
    return result
```

And add `EnvSpec`, `read_env_spec` to `__all__`:

```python
__all__ = ["env_flag", "env_int", "EnvSpec", "read_env_spec"]
```

- [ ] **Step 5.4: Run tests — expect pass**

```bash
uv run pytest tests/providers/test_utils.py -v
```

Expected: all tests pass.

- [ ] **Step 5.5: Migrate `gitlab/client.py::from_env`**

Edit `src/dev_health_ops/providers/gitlab/client.py`. Add import:

```python
from dev_health_ops.providers.utils import EnvSpec, read_env_spec
```

Replace lines 70-76 with:

```python
    @classmethod
    def from_env(cls) -> GitLabWorkClient:
        env = read_env_spec(
            EnvSpec(
                required={"token": "GITLAB_TOKEN"},
                optional={"base_url": ("GITLAB_URL", "https://gitlab.com")},
                missing_error="GitLab token required (set GITLAB_TOKEN)",
            )
        )
        return cls(
            auth=GitLabAuth(
                token=str(env["token"]),
                base_url=str(env["base_url"]),
            )
        )
```

- [ ] **Step 5.6: Migrate `jira/client.py::from_env`**

Edit `src/dev_health_ops/providers/jira/client.py`. Add import (alongside the other `providers/utils` imports):

```python
from dev_health_ops.providers.utils import EnvSpec, read_env_spec
```

Read the existing `from_env` body (lines 88-103) to learn the shape. Replace with:

```python
    @classmethod
    def from_env(cls) -> JiraClient:
        env = read_env_spec(
            EnvSpec(
                required={
                    "base_url": "JIRA_BASE_URL",
                    "email": "JIRA_EMAIL",
                    "api_token": "JIRA_API_TOKEN",
                },
                missing_error=(
                    "Jira env vars required: JIRA_BASE_URL, JIRA_EMAIL, "
                    "JIRA_API_TOKEN"
                ),
            )
        )
        return cls(
            auth=JiraAuth(
                base_url=_normalize_jira_base_url(str(env["base_url"])),
                email=str(env["email"]),
                api_token=str(env["api_token"]),
            )
        )
```

- [ ] **Step 5.7: Migrate `linear/client.py::from_env`**

Edit `src/dev_health_ops/providers/linear/client.py`. Add import:

```python
from dev_health_ops.providers.utils import EnvSpec, read_env_spec
```

Replace lines 335-340 with:

```python
    @classmethod
    def from_env(cls) -> LinearClient:
        env = read_env_spec(
            EnvSpec(
                required={"api_key": "LINEAR_API_KEY"},
                missing_error="Linear API key required (set LINEAR_API_KEY)",
            )
        )
        return cls(auth=LinearAuth(api_key=str(env["api_key"])))
```

- [ ] **Step 5.8: Add `GitHubWorkClient.from_env()`**

Edit `src/dev_health_ops/providers/github/client.py`. Add to imports:

```python
from dev_health_ops.providers.utils import EnvSpec, read_env_spec
```

After the `__init__` method (line 91, directly before `def get_repo`), add:

```python
    @classmethod
    def from_env(cls) -> GitHubWorkClient:
        env = read_env_spec(
            EnvSpec(
                required={"token": "GITHUB_TOKEN"},
                optional={"base_url": ("GITHUB_BASE_URL", None)},
                missing_error="GITHUB_TOKEN environment variable is required",
            )
        )
        base = env["base_url"]
        return cls(
            auth=GitHubAuth(
                token=str(env["token"]),
                base_url=str(base) if base else None,
            )
        )
```

Also ensure `import os` is available inside the module (it already is, since `from_env` needs `os.getenv` — though we route through `read_env_spec` now, `os` is still imported indirectly through `utils`; no change needed here if there is no existing `import os`). Check the file: it does NOT import `os` today, so no new top-level import is required.

- [ ] **Step 5.9: Migrate `github/provider.py::ingest` to use `GitHubWorkClient.from_env()`**

Edit `src/dev_health_ops/providers/github/provider.py`. Replace lines 135-141 (the block starting with `token = os.getenv("GITHUB_TOKEN")`) with:

```python
        client = GitHubWorkClient.from_env()
```

Remove the `import os` line at the top of the file if nothing else in the file uses it. Check: `os.getenv` appears earlier in the file for `_env_flag` — but after Task 4 that usage also went away. Grep within the file to confirm `os` is unused, then delete `import os` (line 11).

Run:

```bash
uv run pytest tests/providers/test_utils.py tests/test_github_provider.py -v
```

- [ ] **Step 5.10: Run full provider suite**

```bash
uv run pytest tests/providers tests/test_github_provider.py tests/test_gitlab_provider.py tests/test_linear_provider.py -q
```

Expected: all pass.

- [ ] **Step 5.11: Commit**

```bash
git add src/dev_health_ops/providers/utils.py \
        tests/providers/test_utils.py \
        src/dev_health_ops/providers/github/client.py \
        src/dev_health_ops/providers/github/provider.py \
        src/dev_health_ops/providers/gitlab/client.py \
        src/dev_health_ops/providers/jira/client.py \
        src/dev_health_ops/providers/linear/client.py
git commit -m "$(cat <<'EOF'
refactor(providers): unify from_env via EnvSpec / read_env_spec

Replace ad-hoc env-var reads in GitLab / Jira / Linear client
classmethods with declarative EnvSpec + read_env_spec helper. Add
GitHubWorkClient.from_env and move GitHubProvider.ingest to use it
(removing the inline os.getenv block).
EOF
)"
```

---

## Task 6: `ProviderWithClient` Base Class

**Estimated diff size:** +130 / −140 LOC.

**Rationale:** The four `provider.py` modules repeat the same scaffolding: lazy `status_mapping` / `identity` loaders, a `capabilities` frozen dataclass, an `ingest` method that (1) validates `ctx`, (2) instantiates a client, (3) builds empty list aggregates, (4) runs provider-specific ingestion, (5) returns a `ProviderBatch`. The ingestion body itself stays provider-specific — we only extract the scaffolding.

**Dependency:** Tasks 3 and 5 must be complete (the new base assumes `*.from_env()` exists on every client).

**Files:**
- Modify: `src/dev_health_ops/providers/base.py` (add `ProviderWithClient`)
- Create: `tests/providers/test_provider_with_client.py`
- Modify: `src/dev_health_ops/providers/github/provider.py` (inherit, remove boilerplate)
- Modify: `src/dev_health_ops/providers/gitlab/provider.py`
- Modify: `src/dev_health_ops/providers/jira/provider.py`
- Modify: `src/dev_health_ops/providers/linear/provider.py`

- [ ] **Step 6.1: Write failing test**

Create `tests/providers/test_provider_with_client.py`:

```python
"""Tests for ProviderWithClient base class."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dev_health_ops.providers.base import (
    IngestionContext,
    IngestionWindow,
    ProviderBatch,
    ProviderCapabilities,
    ProviderWithClient,
)
from dev_health_ops.providers.identity import IdentityResolver
from dev_health_ops.providers.status_mapping import StatusMapping


class _FakeClient:
    @classmethod
    def from_env(cls) -> "_FakeClient":
        return cls()


class _FakeProvider(ProviderWithClient[_FakeClient]):
    name = "fake"
    capabilities = ProviderCapabilities(work_items=True)
    client_factory = _FakeClient.from_env

    def _ingest_with_client(
        self, *, client: _FakeClient, ctx: IngestionContext
    ) -> ProviderBatch:
        return ProviderBatch()


class TestProviderWithClient:
    def test_status_mapping_lazy_default(self) -> None:
        provider = _FakeProvider()
        # First access triggers load.
        mapping = provider.status_mapping
        assert isinstance(mapping, StatusMapping)
        # Repeated access returns the same instance.
        assert provider.status_mapping is mapping

    def test_status_mapping_injected(self) -> None:
        mock = MagicMock(spec=StatusMapping)
        provider = _FakeProvider(status_mapping=mock)
        assert provider.status_mapping is mock

    def test_identity_lazy_default(self) -> None:
        provider = _FakeProvider()
        resolver = provider.identity
        assert isinstance(resolver, IdentityResolver)
        assert provider.identity is resolver

    def test_identity_injected(self) -> None:
        mock = MagicMock(spec=IdentityResolver)
        provider = _FakeProvider(identity=mock)
        assert provider.identity is mock

    def test_ingest_delegates_to_subclass(self) -> None:
        provider = _FakeProvider()
        ctx = IngestionContext(window=IngestionWindow(), repo="test/repo")
        result = provider.ingest(ctx)
        assert isinstance(result, ProviderBatch)
```

- [ ] **Step 6.2: Run test — expect fail (class missing)**

```bash
uv run pytest tests/providers/test_provider_with_client.py -v
```

Expected: `ImportError: cannot import name 'ProviderWithClient' from 'dev_health_ops.providers.base'`.

- [ ] **Step 6.3: Add `ProviderWithClient` to `base.py`**

Edit `src/dev_health_ops/providers/base.py`. Append at the bottom:

```python
from typing import Callable, ClassVar, Generic, TypeVar

from dev_health_ops.providers.identity import IdentityResolver, load_identity_resolver
from dev_health_ops.providers.status_mapping import StatusMapping, load_status_mapping

_TClient = TypeVar("_TClient")


class ProviderWithClient(Provider, Generic[_TClient]):
    """Base class for providers that wrap an API client.

    Subclasses set the ``client_factory`` class attribute (typically
    ``SomeClient.from_env``) and implement ``_ingest_with_client``. The
    base handles:

    - Lazy loading of ``status_mapping`` and ``identity`` with injection
      support via ``__init__`` kwargs.
    - ``ingest()`` boilerplate: call ``client_factory()``, hand off to
      the subclass-defined ``_ingest_with_client`` method.
    """

    client_factory: ClassVar[Callable[[], object]]

    def __init__(
        self,
        *,
        status_mapping: StatusMapping | None = None,
        identity: IdentityResolver | None = None,
    ) -> None:
        self._status_mapping = status_mapping
        self._identity = identity

    @property
    def status_mapping(self) -> StatusMapping:
        if self._status_mapping is None:
            self._status_mapping = load_status_mapping()
        return self._status_mapping

    @property
    def identity(self) -> IdentityResolver:
        if self._identity is None:
            self._identity = load_identity_resolver()
        return self._identity

    def ingest(self, ctx: IngestionContext) -> ProviderBatch:
        client = type(self).client_factory()
        return self._ingest_with_client(client=client, ctx=ctx)

    def _ingest_with_client(
        self, *, client: _TClient, ctx: IngestionContext
    ) -> ProviderBatch:
        raise NotImplementedError
```

- [ ] **Step 6.4: Run new test — expect pass**

```bash
uv run pytest tests/providers/test_provider_with_client.py -v
```

Expected: all tests pass.

- [ ] **Step 6.5: Migrate `GitHubProvider`**

Edit `src/dev_health_ops/providers/github/provider.py`:

Replace the class scaffolding from line 47 (class definition) through line 141 (`client = GitHubWorkClient(auth=auth)` or `client = GitHubWorkClient.from_env()` after Task 5) with:

```python
from dev_health_ops.providers.base import ProviderWithClient
from dev_health_ops.providers.github.client import GitHubWorkClient


class GitHubProvider(ProviderWithClient[GitHubWorkClient]):
    """
    Provider implementation for GitHub.
    """

    name = "github"
    capabilities = ProviderCapabilities(
        work_items=True,
        status_transitions=True,
        dependencies=True,
        interactions=True,
        sprints=True,
        reopen_events=True,
        priority=True,
    )
    client_factory = GitHubWorkClient.from_env

    def _ingest_with_client(
        self, *, client: GitHubWorkClient, ctx: IngestionContext
    ) -> ProviderBatch:
```

And the body of the former `ingest` (lines 142 onward, from `if not ctx.repo:` to the `return ProviderBatch(...)`) becomes the body of `_ingest_with_client` — with two changes:

1. The initial `if not ctx.repo:` validation stays.
2. Remove the `client = GitHubWorkClient(...)` block that was in `ingest`.
3. Remove the `from dev_health_ops.providers.github.client import GitHubAuth, GitHubWorkClient` deferred import — the import is now at module top.

Delete the now-unused `__init__`, `status_mapping`, and `identity` property definitions (lines ~72-98) — they come from the base class.

- [ ] **Step 6.6: Migrate `GitLabProvider`**

Read `src/dev_health_ops/providers/gitlab/provider.py` fully to locate its boilerplate (the `__init__`, two properties, and `ingest` entry that builds the client). Apply the same transform:

- Change `class GitLabProvider(Provider):` to `class GitLabProvider(ProviderWithClient[GitLabWorkClient]):`
- Add `client_factory = GitLabWorkClient.from_env`
- Delete the `__init__` and the two properties
- Rename `def ingest(self, ctx):` to `def _ingest_with_client(self, *, client, ctx):`
- Remove the inline `client = GitLabWorkClient.from_env()` line inside the former `ingest`
- Hoist the `from dev_health_ops.providers.gitlab.client import ...` deferred import to module scope

Add import near the top:

```python
from dev_health_ops.providers.base import ProviderWithClient
from dev_health_ops.providers.gitlab.client import GitLabWorkClient
```

- [ ] **Step 6.7: Migrate `JiraProvider`**

Apply the same transform to `src/dev_health_ops/providers/jira/provider.py`:

- Change `class JiraProvider(Provider):` to `class JiraProvider(ProviderWithClient[JiraClient]):`
- Set `client_factory = JiraClient.from_env`
- Delete `__init__` + properties
- Rename `ingest` to `_ingest_with_client(self, *, client, ctx)`
- Remove inline `client = JiraClient.from_env()` from the body
- Hoist the deferred import for `JiraClient` to module scope

- [ ] **Step 6.8: Migrate `LinearProvider`**

Apply the same transform to `src/dev_health_ops/providers/linear/provider.py`:

- Change `class LinearProvider(Provider):` to `class LinearProvider(ProviderWithClient[LinearClient]):`
- Set `client_factory = LinearClient.from_env`
- Delete `__init__` + properties
- Rename `ingest` to `_ingest_with_client(self, *, client, ctx)`
- Remove inline `client = LinearClient.from_env()` from the body
- Hoist deferred `LinearClient` import

- [ ] **Step 6.9: Run full provider suite**

```bash
uv run pytest tests/providers tests/test_github_provider.py tests/test_gitlab_provider.py tests/test_linear_provider.py tests/test_provider_contract.py -q
```

Expected: all pass. If any test patches `GitHubWorkClient(auth=...)` directly, update it to patch `GitHubWorkClient.from_env` instead. Specifically check:

```bash
uv run pytest tests/test_github_provider.py -v 2>&1 | grep -iE "FAIL|ERROR" | head
```

- [ ] **Step 6.10: Fix any test patching issues (if any)**

If a test does `with patch("dev_health_ops.providers.github.provider.GitHubWorkClient") as mock_client_cls: mock_client_cls.return_value = mock_instance`, it keeps working because `client_factory = GitHubWorkClient.from_env` resolves the class attribute at call time. If a test does `with patch.object(GitHubProvider, "ingest")` or relies on the specific `GitHubAuth(token=...)` constructor call, adjust to patch `GitHubWorkClient.from_env` (returning the mock client instance).

- [ ] **Step 6.11: Commit**

```bash
git add src/dev_health_ops/providers/base.py \
        tests/providers/test_provider_with_client.py \
        src/dev_health_ops/providers/github/provider.py \
        src/dev_health_ops/providers/gitlab/provider.py \
        src/dev_health_ops/providers/jira/provider.py \
        src/dev_health_ops/providers/linear/provider.py
git commit -m "$(cat <<'EOF'
refactor(providers): introduce ProviderWithClient base class

Extract shared provider scaffolding (lazy status_mapping / identity,
client instantiation, ingest boilerplate) into ProviderWithClient in
providers/base.py. Each of GitHub / GitLab / Jira / Linear now only
declares name, capabilities, client_factory, and a focused
_ingest_with_client method.
EOF
)"
```

---

## Task 7: Centralize `get_session` FastAPI Dependency

**Estimated diff size:** +25 / −35 LOC.

**Rationale:** Three API routers each define a local `async def get_session()` that yields a postgres session. Consolidate.

**Files:**
- Create: `src/dev_health_ops/api/dependencies.py`
- Create: `tests/api/test_dependencies.py`
- Modify: `src/dev_health_ops/api/admin/routers/common.py:22-24`
- Modify: `src/dev_health_ops/api/telemetry/router.py:19-21`
- Modify: `src/dev_health_ops/api/billing/invoice_routes.py:26-28`

- [ ] **Step 7.1: Write failing test**

Create `tests/api/test_dependencies.py`:

```python
"""Tests for shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_get_postgres_session_dep_yields_session() -> None:
    from dev_health_ops.api.dependencies import get_postgres_session_dep

    fake_session = MagicMock(spec=AsyncSession)

    class _FakeCtx:
        async def __aenter__(self) -> AsyncSession:
            return fake_session

        async def __aexit__(self, *a: object) -> None:
            return None

    with patch(
        "dev_health_ops.api.dependencies.get_postgres_session",
        return_value=_FakeCtx(),
    ):
        agen: AsyncGenerator[AsyncSession, None] = get_postgres_session_dep()
        session = await agen.__anext__()
        assert session is fake_session
        with pytest.raises(StopAsyncIteration):
            await agen.__anext__()
```

- [ ] **Step 7.2: Run test — expect fail (module missing)**

```bash
uv run pytest tests/api/test_dependencies.py -v
```

Expected: `ModuleNotFoundError: No module named 'dev_health_ops.api.dependencies'`.

- [ ] **Step 7.3: Create `api/dependencies.py`**

Create `src/dev_health_ops/api/dependencies.py`:

```python
"""Shared FastAPI dependencies."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.db import get_postgres_session

__all__ = ["get_postgres_session_dep"]


async def get_postgres_session_dep() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async postgres session, managing lifecycle via ``get_postgres_session``."""
    async with get_postgres_session() as session:
        yield session
```

- [ ] **Step 7.4: Run test — expect pass**

```bash
uv run pytest tests/api/test_dependencies.py -v
```

Expected: pass.

- [ ] **Step 7.5: Migrate `admin/routers/common.py`**

Edit `src/dev_health_ops/api/admin/routers/common.py`. Replace lines 22-24 (the local `async def get_session()`) with an import + alias so callers (e.g. `Depends(get_session)`) keep working:

```python
from dev_health_ops.api.dependencies import get_postgres_session_dep as get_session
```

Remove the now-unused `from collections.abc import AsyncGenerator` and `from dev_health_ops.db import get_postgres_session` imports if nothing else in the file uses them. Check with grep before deleting.

- [ ] **Step 7.6: Migrate `telemetry/router.py`**

Edit `src/dev_health_ops/api/telemetry/router.py`. Replace lines 19-21 with:

```python
from dev_health_ops.api.dependencies import get_postgres_session_dep as get_session
```

Prune `AsyncGenerator` / `get_postgres_session` / `AsyncSession` imports if now-unused. Verify the rest of the file uses only `get_session` — the alias preserves call-site compatibility.

- [ ] **Step 7.7: Migrate `billing/invoice_routes.py`**

Edit `src/dev_health_ops/api/billing/invoice_routes.py`. Replace lines 26-28 with:

```python
from dev_health_ops.api.dependencies import get_postgres_session_dep as get_session
```

Prune unused imports as above.

- [ ] **Step 7.8: Run API tests**

```bash
uv run pytest tests/api -q
```

Expected: all pass.

- [ ] **Step 7.9: Commit**

```bash
git add src/dev_health_ops/api/dependencies.py \
        tests/api/test_dependencies.py \
        src/dev_health_ops/api/admin/routers/common.py \
        src/dev_health_ops/api/telemetry/router.py \
        src/dev_health_ops/api/billing/invoice_routes.py
git commit -m "$(cat <<'EOF'
refactor(api): centralize get_session FastAPI dependency

Move the identical three-line async get_session into
api/dependencies.py::get_postgres_session_dep. Each router keeps a
local alias so existing Depends(get_session) call-sites continue to
work unchanged.
EOF
)"
```

---

## Task 8: LLM JSON Utilities

**Estimated diff size:** +70 / −45 LOC.

**Rationale:** `llm/providers/openai.py::validate_json_or_empty` and `llm/explainers/investment_mix_explainer.py::_extract_json_object` both parse/recover JSON from LLM output (one strict round-trip, the other extracts the first `{...}` block). Consolidate into `llm/json_utils.py` so future explainers reuse the logic.

**Files:**
- Create: `src/dev_health_ops/llm/json_utils.py`
- Create: `tests/llm/test_json_utils.py`
- Modify: `src/dev_health_ops/llm/providers/openai.py:57-65`
- Modify: `src/dev_health_ops/llm/explainers/investment_mix_explainer.py:74-112`

- [ ] **Step 8.1: Verify `tests/llm` exists**

```bash
ls /Users/chris/projects/full-chaos/dev-health/ops/tests/llm 2>&1 || echo "need to create"
```

If missing, the test file `tests/llm/test_json_utils.py` will be the first file there — pytest auto-discovers.

- [ ] **Step 8.2: Write failing test**

Create `tests/llm/test_json_utils.py`:

```python
"""Tests for LLM JSON utilities."""

from __future__ import annotations

from dev_health_ops.llm.json_utils import (
    extract_json_object,
    validate_json_or_empty,
)


class TestValidateJsonOrEmpty:
    def test_valid_json_roundtrips(self) -> None:
        assert validate_json_or_empty('{"a": 1}') == '{"a": 1}'

    def test_valid_json_compact(self) -> None:
        result = validate_json_or_empty('{\n  "a": 1\n}')
        assert result == '{"a": 1}'

    def test_invalid_returns_empty(self) -> None:
        assert validate_json_or_empty("not json") == ""

    def test_empty_returns_empty(self) -> None:
        assert validate_json_or_empty("") == ""
        assert validate_json_or_empty("   ") == ""

    def test_non_ascii_preserved(self) -> None:
        assert validate_json_or_empty('{"n": "café"}') == '{"n": "café"}'


class TestExtractJsonObject:
    def test_direct_object(self) -> None:
        assert extract_json_object('{"a": 1}') == {"a": 1}

    def test_object_within_text(self) -> None:
        text = "Here is the result: {\"a\": 1, \"b\": 2}. Thanks!"
        assert extract_json_object(text) == {"a": 1, "b": 2}

    def test_empty_returns_none(self) -> None:
        assert extract_json_object("") is None
        assert extract_json_object("   \n") is None

    def test_no_braces_returns_none(self) -> None:
        assert extract_json_object("no braces here") is None

    def test_malformed_returns_none(self) -> None:
        assert extract_json_object("{not valid json}") is None

    def test_non_object_returns_none(self) -> None:
        # Arrays are not objects.
        assert extract_json_object("[1, 2, 3]") is None

    def test_nested_object(self) -> None:
        result = extract_json_object('prefix {"a": {"b": [1, 2]}} suffix')
        assert result == {"a": {"b": [1, 2]}}
```

- [ ] **Step 8.3: Run test — expect fail (module missing)**

```bash
uv run pytest tests/llm/test_json_utils.py -v
```

Expected: `ModuleNotFoundError: No module named 'dev_health_ops.llm.json_utils'`.

- [ ] **Step 8.4: Create `json_utils.py`**

Create `src/dev_health_ops/llm/json_utils.py`:

```python
"""Shared JSON parsing helpers for LLM outputs.

LLM responses are often near-JSON but may contain surrounding prose or
whitespace. ``validate_json_or_empty`` is a strict gate used by
structured-output validators; ``extract_json_object`` is a recovery
helper that finds the first top-level ``{...}`` block and parses it.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

__all__ = ["validate_json_or_empty", "extract_json_object"]


def validate_json_or_empty(text: str) -> str:
    """Return a compact JSON string if ``text`` parses as JSON, else empty.

    Whitespace-only and empty inputs return ``""``. Invalid JSON also
    returns ``""`` (no logging — used in hot OpenAI validation path).
    """
    if not text or not text.strip():
        return ""
    try:
        obj = json.loads(text)
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ""


def extract_json_object(text: str) -> dict[str, Any] | None:
    """Extract and parse the first top-level JSON object from ``text``.

    Logs a warning when extraction fails. Returns ``None`` when:
      - ``text`` is empty or whitespace
      - no balanced ``{...}`` block is found
      - the block is not valid JSON
      - the parsed value is not a dict (arrays, scalars return None)
    """
    if not text or not text.strip():
        logger.warning("LLM response is empty or whitespace-only")
        return None

    candidate = text.strip()
    start = candidate.find("{")
    end = candidate.rfind("}")

    if start == -1 or end == -1 or end < start:
        safe_preview = text[:500].replace("\r", "\\r").replace("\n", "\\n")
        logger.warning(
            "Failed to find JSON object in LLM response. "
            "Preview of text (%d chars shown, total %d): %r",
            len(safe_preview),
            len(text),
            safe_preview,
        )
        return None

    json_str = candidate[start : end + 1]
    try:
        parsed = json.loads(json_str)
    except json.JSONDecodeError as exc:
        safe_preview = json_str[:500].replace("\r", "\\r").replace("\n", "\\n")
        logger.warning(
            "JSON decode error in LLM response: %s. "
            "Text preview (%d chars shown, total %d): %r",
            exc,
            len(safe_preview),
            len(json_str),
            safe_preview,
        )
        return None

    if not isinstance(parsed, dict):
        logger.warning("Parsed JSON is not a dictionary")
        return None
    return parsed
```

- [ ] **Step 8.5: Run test — expect pass**

```bash
uv run pytest tests/llm/test_json_utils.py -v
```

Expected: all pass.

- [ ] **Step 8.6: Migrate `openai.py`**

Edit `src/dev_health_ops/llm/providers/openai.py`. Replace the `validate_json_or_empty` definition (lines 57-65) with a re-export:

```python
from dev_health_ops.llm.json_utils import validate_json_or_empty  # noqa: F401  (re-exported)
```

Remove the `import json` if it is no longer used elsewhere in the file (grep the file first; if other code uses `json`, leave it).

- [ ] **Step 8.7: Migrate `investment_mix_explainer.py`**

Edit `src/dev_health_ops/llm/explainers/investment_mix_explainer.py`. Replace the `_extract_json_object` definition (lines 74-112) with:

```python
from dev_health_ops.llm.json_utils import extract_json_object as _extract_json_object
```

Place the import alongside the existing imports at the top. Delete lines 74-112.

- [ ] **Step 8.8: Run explainer + openai tests**

```bash
uv run pytest tests/analytics/test_investment_mix_explainer.py tests/api/test_investment_mix_parsing.py tests/api/test_openai_provider_fix.py -q
```

Expected: all pass.

- [ ] **Step 8.9: Commit**

```bash
git add src/dev_health_ops/llm/json_utils.py \
        tests/llm/test_json_utils.py \
        src/dev_health_ops/llm/providers/openai.py \
        src/dev_health_ops/llm/explainers/investment_mix_explainer.py
git commit -m "$(cat <<'EOF'
refactor(llm): extract shared JSON parsing helpers

Consolidate validate_json_or_empty (strict gate) and _extract_json_object
(recovery path) into llm/json_utils.py. Existing private aliases
preserved in call-sites so behavior is unchanged.
EOF
)"
```

---

## Task 9: `OrgScopedQuery` Builder

**Estimated diff size:** +110 / −60 LOC.

**Rationale:** `ClickHouseDataLoader` calls `self._org_filter(...)` and `self._inject_org_id(params)` at 30+ sites. Extract a small immutable builder that holds the org id and emits both the SQL snippet and the updated params dict. This makes unit-testing individual queries easier and removes the duplicated `if self.org_id:` branches from every loader method.

**Files:**
- Create: `src/dev_health_ops/metrics/query_builder.py`
- Create: `tests/metrics/test_query_builder.py`
- Modify: `src/dev_health_ops/metrics/loaders/clickhouse.py:46-71` (delegate to builder)

- [ ] **Step 9.1: Write failing test**

Create `tests/metrics/test_query_builder.py`:

```python
"""Tests for OrgScopedQuery builder."""

from __future__ import annotations

from dev_health_ops.metrics.query_builder import OrgScopedQuery


class TestOrgScopedQuery:
    def test_empty_org_no_filter(self) -> None:
        q = OrgScopedQuery("")
        assert q.filter() == ""
        assert q.filter(alias="c") == ""
        assert q.inject({"x": 1}) == {"x": 1}

    def test_with_org_emits_filter(self) -> None:
        q = OrgScopedQuery("acme")
        assert q.filter() == " AND org_id = {org_id:String}"

    def test_aliased_filter(self) -> None:
        q = OrgScopedQuery("acme")
        assert q.filter(alias="c") == " AND c.org_id = {org_id:String}"

    def test_inject_adds_org_id(self) -> None:
        q = OrgScopedQuery("acme")
        params = {"start": "2024-01-01", "end": "2024-12-31"}
        result = q.inject(params)
        assert result == {
            "start": "2024-01-01",
            "end": "2024-12-31",
            "org_id": "acme",
        }

    def test_inject_is_non_mutating(self) -> None:
        q = OrgScopedQuery("acme")
        original = {"k": 1}
        q.inject(original)
        assert "org_id" not in original

    def test_bool_truthiness(self) -> None:
        assert bool(OrgScopedQuery("")) is False
        assert bool(OrgScopedQuery("acme")) is True
```

- [ ] **Step 9.2: Run test — expect fail (module missing)**

```bash
uv run pytest tests/metrics/test_query_builder.py -v
```

Expected: `ModuleNotFoundError: No module named 'dev_health_ops.metrics.query_builder'`.

- [ ] **Step 9.3: Create `query_builder.py`**

Create `src/dev_health_ops/metrics/query_builder.py`:

```python
"""Query-fragment builder used by ClickHouse data loaders.

Encapsulates the "AND org_id = {org_id:String}" filter and parameter
injection so individual loader methods don't repeat the same branch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = ["OrgScopedQuery"]


@dataclass(frozen=True)
class OrgScopedQuery:
    """Immutable helper for scoping ClickHouse queries to an org."""

    org_id: str = ""

    def __bool__(self) -> bool:
        return bool(self.org_id)

    def filter(self, *, alias: str = "") -> str:
        """Return ``" AND {alias?.}org_id = {org_id:String}"`` or ``""``."""
        if not self.org_id:
            return ""
        col = f"{alias}.org_id" if alias else "org_id"
        return f" AND {col} = {{org_id:String}}"

    def inject(self, params: dict[str, Any]) -> dict[str, Any]:
        """Return a new params dict with ``org_id`` added when set.

        Non-mutating: the original ``params`` dict is untouched.
        """
        if not self.org_id:
            return params
        merged = dict(params)
        merged["org_id"] = self.org_id
        return merged
```

- [ ] **Step 9.4: Run test — expect pass**

```bash
uv run pytest tests/metrics/test_query_builder.py -v
```

Expected: all pass.

- [ ] **Step 9.5: Migrate `ClickHouseDataLoader` to hold an `OrgScopedQuery`**

Edit `src/dev_health_ops/metrics/loaders/clickhouse.py`.

Add import at the top (alongside other `dev_health_ops.metrics` imports):

```python
from dev_health_ops.metrics.query_builder import OrgScopedQuery
```

Replace the `__init__` (lines 55-57) and the two private methods (lines 59-71) with:

```python
    def __init__(self, client: Any, org_id: str = "") -> None:
        self.client = client
        self.org_id = org_id
        self._scope = OrgScopedQuery(org_id)

    def _org_filter(self, *, alias: str = "") -> str:
        """Return an ``AND org_id = …`` clause when *org_id* is set."""
        return self._scope.filter(alias=alias)

    def _inject_org_id(self, params: dict[str, Any]) -> dict[str, Any]:
        """Inject *org_id* into query parameters when set."""
        return self._scope.inject(params)
```

Keep `self.org_id` as a public attribute — the existing `if self.org_id:` check on line 89 (inside `load_git_rows`) relies on it. Leave that block alone; the builder is a refinement of the two helpers, not a full replacement of every org-scope use.

- [ ] **Step 9.6: Run metrics tests**

```bash
uv run pytest tests/metrics tests/test_clickhouse_org_id.py -q
```

Expected: all pass.

- [ ] **Step 9.7: Commit**

```bash
git add src/dev_health_ops/metrics/query_builder.py \
        tests/metrics/test_query_builder.py \
        src/dev_health_ops/metrics/loaders/clickhouse.py
git commit -m "$(cat <<'EOF'
refactor(metrics): extract OrgScopedQuery builder for ClickHouse loader

Pull the "AND org_id = {org_id:String}" filter plus non-mutating param
injection into a small immutable dataclass in metrics/query_builder.py.
ClickHouseDataLoader delegates _org_filter / _inject_org_id to the
builder. No behavior changes.
EOF
)"
```

---

## Final Verification

After all tasks land:

- [ ] Run the complete provider and impacted-module test suites:

```bash
uv run pytest tests/providers tests/test_github_provider.py tests/test_gitlab_provider.py tests/test_linear_provider.py tests/test_provider_contract.py tests/api tests/metrics tests/llm tests/analytics/test_investment_mix_explainer.py -q
```

- [ ] Run the full test suite to catch any indirect breakage:

```bash
uv run pytest -q
```

- [ ] Confirm git log shows nine focused commits (one per task):

```bash
git log --oneline main..HEAD
```

Expected: 9 commits, roughly in the order Task 1..9 (with Tasks 1-4, 7-9 potentially interleaved if parallelized).

---

## Dependency Graph

```
Task 1 (normalize helpers)     — no blockers  ──┐
Task 2 (iter_with_limit)       — no blockers  ──┤
Task 3 (rate-limit gate_call)  — no blockers  ──┼──▶ Task 6 (ProviderWithClient)  [needs 3, 5]
Task 4 (env_flag/env_int)      — no blockers  ──▶ Task 5 (from_env base / EnvSpec)  [needs 4] ──▶ Task 6
Task 7 (get_session dep)       — no blockers
Task 8 (LLM JSON utils)        — no blockers
Task 9 (OrgScopedQuery)        — no blockers
```

Parallelizable wave 1 (no blockers): **Tasks 1, 2, 3, 4, 7, 8, 9** — can all land independently.

Serial wave after Wave 1: **Task 5** (requires Task 4 to land).

Final: **Task 6** (requires both Task 3 and Task 5).

Recommended execution order for a single reviewer: 1 → 2 → 4 → 3 → 5 → 6 → 7 → 8 → 9.

---

## Audit Findings — Corrections

During verification the following findings from the audit were adjusted. Callers of this plan should know:

1. **Finding 1** was partially off: `_get` / `_as_str` / `_as_int` / `_as_node_list` / `_labels_from_nodes` are NOT all present in every provider's `normalize.py`. Reality:
   - `github/normalize.py` has `_as_dict`, `_as_str`, `_as_int`, `_as_node_list`, `_labels_from_nodes` (no `_get`).
   - `gitlab/normalize.py` has `_get` only.
   - `linear/normalize.py` has a variadic `_get(obj, *keys)` and its own `_parse_iso` (distinct from the shared `normalize_common.parse_iso_datetime`).
   - `jira/normalize.py` has `_get_field` which descends into `.fields` — Jira-specific, stays put.

   Task 1 reflects this: it extracts the five actually-duplicated shape coercers plus a flat `get_attr` (gitlab-style) and a variadic `get_nested` (linear-style). Jira is untouched.

2. **Finding 4** said "github's inline `_env_int`". There is no function by that name in `github/provider.py`. The pattern is an inline `raw = os.getenv("GITHUB_COMMENTS_LIMIT"); try: int(raw) except ValueError: log+fallback` at lines 154-163. Task 4 creates an `env_int` helper and replaces the inline block (net saving ~10 LOC, not a full function extraction).

3. **Finding 3** asked for "decorator OR context manager". We chose the context manager (`gate_call`) because Jira's `_request_json` needs access to the HTTP response object (to read `Retry-After`) before committing to reset/penalize — this is awkward with a decorator but natural with `with`. Decorator-style could wrap pure-happy-path methods; we leave that as a follow-up if desired.

4. All other findings (2, 5, 6, 7, 8, 9) verified as stated in the audit.
