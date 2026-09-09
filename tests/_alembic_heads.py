"""The alembic branch heads, DERIVED from the migration graph.

Twelve test files hardcoded the application_schema head as a literal.
Every one of them asserts the same thing -- "after upgrading to
``application_schema@head`` the version table reads the head" -- and that
claim does not name a revision. So each of the twelve failed on the
migration that moved the head, for a reason unrelated to what it asserts,
and the next migration would have failed them all again.

CHAOS-5425's 0128 was the instance that surfaced it, and it surfaced twice:
four of the twelve were caught by the local gate, and two more only by CI,
because most of these tests skip without ``DEV_HEALTH_POSTGRES_TEST_URI``
and the local gate does not set it. Deriving the value removes the whole
class rather than the six that happened to run.

The river-cutover head stays a LITERAL on purpose. ``0066`` is a fixed,
authorized cutover point rather than a moving tip, and pinning it is
exactly what makes an accidental third branch visible.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

#: The river-cutover branch head. Fixed by decision, not by whatever the
#: graph happens to end at -- see the module docstring.
RIVER_CUTOVER_HEAD = "0066"

_ALEMBIC_DIR = Path(__file__).parents[1] / "src" / "dev_health_ops" / "alembic"


def application_schema_head() -> str:
    """The single head of the ``application_schema`` branch.

    Asserts there is exactly one. A branch that has silently grown a second
    head is a real problem, and returning the first of several would hide
    it behind a passing test.
    """
    config = Config()
    config.set_main_option("script_location", str(_ALEMBIC_DIR))
    heads = ScriptDirectory.from_config(config).get_revisions("application_schema@head")
    assert len(heads) == 1, f"expected one application_schema head, got {heads}"
    return heads[0].revision


def expected_heads() -> set[str]:
    """Both branch heads: the fixed cutover point and the derived tip."""
    return {RIVER_CUTOVER_HEAD, application_schema_head()}
