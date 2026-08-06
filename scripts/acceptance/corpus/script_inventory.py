"""CHAOS-3219 Wave 4 Phase 2 Lane 2a: script-inventory startup preflight.

Requirement (team-lead mandate, item 1): a corpus case citing a missing
scripted-provider script must FAIL LOUDLY at startup, never silently skip
or fall through.

Recon finding this exists to close: production's own
``provider_scripts.py`` has NO such check. A registry case id absent from
``role-<role>.json``'s ``cases`` map is, by that module's own README,
"not an error by itself -- it simply means no question routes to it yet",
and at request time an unmapped question is *indistinguishable on the wire*
from an ordinary one -- it silently falls through to the generic default
heuristic (see ``provider_scripts.ScriptEngine.resolve`` returning
``None``). That is exactly correct production behavior (it keeps every
pre-CHAOS-3219 smoke/probe/oracle working unconditionally), but it is the
WRONG behavior for a corpus runner: a case whose scripted decisions were
never authored would silently get an unscripted, nondeterministic-shaped
answer from the default heuristic and could pass or fail its assertions for
reasons having nothing to do with what the case claims to prove. This
module is the runner-side check production deliberately does not make.
"""

from __future__ import annotations

from collections.abc import Iterable

__all__ = [
    "ScriptInventoryError",
    "missing_scripted_cases",
    "check_script_inventory",
]


class ScriptInventoryError(Exception):
    """One or more corpus cases have no matching scripted-provider entry."""


def missing_scripted_cases(
    case_ids: Iterable[str], scripted_case_ids: Iterable[str]
) -> list[str]:
    """Corpus case ids with no entry in the active role's script file.

    ``scripted_case_ids`` is the raw ``role_script.cases`` key set (a
    ``RoleScript`` from ``dev_health_ops.llm.agent.provider_scripts`` has
    exactly this shape) -- this function takes plain string iterables, not
    the ``RoleScript`` type itself, so it has no import-time dependency on
    the scripted-provider module and can be unit tested with fabricated
    data.
    """

    return sorted(set(case_ids) - set(scripted_case_ids))


def check_script_inventory(
    case_ids: Iterable[str],
    scripted_case_ids: Iterable[str],
    *,
    role: str,
) -> None:
    """Raise :class:`ScriptInventoryError` if any case id lacks a script.

    Call this once, at session start, before any corpus case executes a
    single HTTP request -- not per-case, and never downgraded to a skip.
    A run that "skipped" the missing cases would still exit non-negative
    for the cases it did run, reading as partial coverage rather than the
    authoring gap it actually is.
    """

    missing = missing_scripted_cases(case_ids, scripted_case_ids)
    if missing:
        raise ScriptInventoryError(
            f"{len(missing)} corpus case(s) have no entry in role-{role}.json's "
            f"scripted-provider decisions -- a corpus run must never execute "
            "these against the unscripted default heuristic (nondeterministic, "
            "proves nothing about the case's own claim): "
            f"{missing!r}. Author their provider-scripts/role-{role}.json "
            "entries before this case can run in the corpus, or remove the "
            "case file if it is not yet ready."
        )
