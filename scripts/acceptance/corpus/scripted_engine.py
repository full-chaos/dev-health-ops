"""CHAOS-3219 Phase 3: prove the scripted decision/fault engine is actually
loaded before an armed corpus run measures anything.

Why this module exists, stated plainly because the cost was real: the
2026-08-07 04:55 armed run recorded 140 receipts, reported the corpus green,
and had exercised **no scripted fault or refusal at all**. The
``ask-dev-scripted-openai`` container could not see its script directory --
the ``api`` image target ships only the built wheel, no ``tests/`` tree, and
the compose overlay mounted nothing -- so ``provider_scripts._scripts_dir()``
resolved a relative path against WORKDIR ``/app``, ``load_registry_ids``
raised ``scripts_directory_unavailable``, and ``try_load_engine`` swallowed
it and returned ``None``.

That swallow is CORRECT for organic production traffic (an infra failure must
not turn an ordinary user question into an error) and catastrophic for an
acceptance run, where the entire point is that the scripted matrix answers.
All 19 scripted cases -- 10 faults and 9 decision scripts, including every
``adv.injection-request.*`` refusal and both ``provider-fail.*`` cases --
fell through to the unscripted default heuristic, returned an ordinary
answer, and PASSED.

The wiring is fixed in ``tests/acceptance/compose.ask-dev.yml``. This module
is the part that keeps it fixed: **a fault matrix that did not load must fail
the run, loudly, forever.** A mount can rot silently; a missing env var can
be dropped in a refactor; an image rebuild can change the working directory.
None of those may ever again be indistinguishable from a green run.

This is deliberately NOT part of ``db_verify``: that module's documented cap
is three read-only DATABASE concerns, and this is a service-readiness probe,
not a database read.
"""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .compose_context import ComposeContext

__all__ = [
    "ScriptedEngineStatus",
    "ScriptedEngineUnavailableError",
    "require_scripted_engine_loaded",
    "scripted_engine_status",
]

#: Run inside the scripted-provider container. Prints exactly one JSON line.
#: Imports the SAME helper the service's own ``/healthz`` returns, rather
#: than re-deriving "is it loaded" here -- a second implementation of that
#: question could disagree with the one the service actually reports, which
#: is the class of defect this whole module exists to catch.
_PROBE = (
    "import json;"
    "from dev_health_ops.llm.agent.scripted_openai_service import "
    "scripted_engine_health;"
    "print(json.dumps(scripted_engine_health()))"
)


class ScriptedEngineUnavailableError(RuntimeError):
    """The scripted engine could not be proven loaded.

    Raised for BOTH "the probe could not run" and "the probe ran and said
    not loaded". Both must stop an armed run: an unmeasurable precondition
    is not a satisfied one, and treating a failed probe as a pass would
    reintroduce exactly the silence this module was written to end.
    """


@dataclass(frozen=True, slots=True)
class ScriptedEngineStatus:
    loaded: bool
    role: str
    cases: int
    script_digest: str
    reason: str | None
    raw: Mapping[str, Any]


def scripted_engine_status(
    context: ComposeContext,
    *,
    service: str = "ask-dev-scripted-openai",
    runner: Any = subprocess.run,
    timeout: float = 60.0,
) -> ScriptedEngineStatus:
    """Ask the scripted-provider container whether its engine is loaded."""

    command: Sequence[str] = ("python", "-c", _PROBE)
    args = [*context.base_args(), "exec", "-T", service, *command]
    try:
        # stdin=DEVNULL for the same load-bearing reason db_verify documents:
        # `-T` suppresses the TTY but leaves fd 0 forwarded, so an inherited
        # open pipe (any shell-script driver) hangs this call forever -- a
        # silent hang that burns the stack slot rather than failing.
        result = runner(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            stdin=subprocess.DEVNULL,
        )
    except FileNotFoundError as exc:
        raise ScriptedEngineUnavailableError(
            f"docker compose is not available on this host: {exc}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise ScriptedEngineUnavailableError(
            f"probing {service} for scripted-engine status timed out after {timeout}s"
        ) from exc
    if result.returncode != 0:
        raise ScriptedEngineUnavailableError(
            f"probing {service} for scripted-engine status exited "
            f"{result.returncode}: {result.stderr.strip()}"
        )

    stripped = result.stdout.strip()
    if not stripped:
        raise ScriptedEngineUnavailableError(
            f"probing {service} produced no output; stderr={result.stderr.strip()!r}"
        )
    # The container emits telemetry banners on stdout before our line, so take
    # the last non-empty line rather than assuming the payload stands alone.
    last = [line for line in stripped.splitlines() if line.strip()][-1]
    try:
        payload = json.loads(last)
    except json.JSONDecodeError as exc:
        raise ScriptedEngineUnavailableError(
            f"probing {service} returned unparseable output {last!r}: {exc}"
        ) from exc
    if not isinstance(payload, Mapping):
        raise ScriptedEngineUnavailableError(
            f"probing {service} returned a non-object payload: {payload!r}"
        )

    engine = payload.get("scripted_engine")
    if not isinstance(engine, Mapping):
        raise ScriptedEngineUnavailableError(
            "the scripted provider's health payload carries no "
            f"'scripted_engine' block: {payload!r}"
        )
    # `cases` is an int in every shape this service emits, but a malformed or
    # truncated payload must still leave through THIS module's typed error --
    # the caller (`test_wave4_corpus_runner_live._scripted_engine_precondition`)
    # catches only ScriptedEngineUnavailableError, so a bare ValueError would
    # escape as an opaque traceback instead of the diagnostic failure this
    # module exists to produce. Found by probing the parser, not by review.
    raw_cases = engine.get("cases")
    if raw_cases is None:
        # A degraded payload legitimately omits `cases` -- it never loaded a
        # script to count. `loaded` is what rejects that shape, not this.
        cases = 0
    elif isinstance(raw_cases, bool) or not isinstance(raw_cases, int):
        # Deliberately strict: `engine.get("cases") or 0` was the first
        # attempt and it silently coerced falsy junk -- `[]`, `{}` -- into a
        # perfectly valid 0, turning a malformed payload into a confident
        # wrong answer. bool is excluded because it is an int subclass, so
        # `True` would otherwise report one case.
        raise ScriptedEngineUnavailableError(
            f"the scripted provider reported a non-numeric case count "
            f"{raw_cases!r} ({type(raw_cases).__name__})"
        )
    else:
        cases = raw_cases

    return ScriptedEngineStatus(
        loaded=bool(engine.get("loaded")),
        role=str(engine.get("role", "")),
        cases=cases,
        script_digest=str(engine.get("script_digest") or ""),
        reason=(str(engine["reason"]) if engine.get("reason") else None),
        raw=payload,
    )


def require_scripted_engine_loaded(
    context: ComposeContext,
    *,
    expected_role: str,
    expected_digest: str,
    service: str = "ask-dev-scripted-openai",
    runner: Any = subprocess.run,
) -> ScriptedEngineStatus:
    """Fail the armed run unless the container serves EXACTLY the script this
    run asserts against.

    Identity, not quantity. An earlier revision compared only a case-count
    floor and claimed in its own comment to catch stale, partial and
    wrong-role mounts -- codex adversarial review (HIGH, confirmed against
    source) showed that claim was FALSE: a wrong role, a stale compose
    project, or an unrelated script with an equal or larger case count all
    cleared a floor while serving different decisions than the corpus
    asserts. ``status.role`` was parsed and then never compared at all.

    So both halves are checked here:

    * ``expected_role`` -- the container must be serving the role the host
      loaded, not merely *a* role;
    * ``expected_digest`` -- ``role_script_identity_digest`` over
      ``by_fingerprint``, the map ``ScriptEngine.resolve`` actually routes
      on, so the check fails if any question routes to a different case id.
      A question edited by one word moves this digest; a count would not.

    ``loaded`` is still checked first because it produces the far more
    actionable message for the failure that actually happened in the field.
    """

    status = scripted_engine_status(context, service=service, runner=runner)
    if not status.loaded:
        raise ScriptedEngineUnavailableError(
            "the scripted decision/fault engine is NOT loaded in "
            f"{service!r} (role={status.role!r}): {status.reason}. Every "
            "scripted fault and refusal would silently degrade to the "
            "unscripted default heuristic and the run would report green "
            "having measured nothing -- refusing to proceed. Check that "
            "compose.ask-dev.yml still mounts the provider-scripts directory "
            "and sets ASK_DEV_SCRIPTED_PROVIDER_SCRIPTS_DIR."
        )
    if status.role != expected_role:
        raise ScriptedEngineUnavailableError(
            f"{service!r} is serving role {status.role!r} but this run "
            f"asserts against {expected_role!r} -- the container would answer "
            "with a different decision matrix than the corpus expects."
        )
    if not status.script_digest:
        raise ScriptedEngineUnavailableError(
            f"{service!r} reported no script_digest, so its script identity "
            "could not be verified. An unverifiable identity is not a "
            "matching one -- refusing to proceed."
        )
    if status.script_digest != expected_digest:
        raise ScriptedEngineUnavailableError(
            f"{service!r} loaded role {status.role!r} with {status.cases} "
            f"case(s) but its script identity digest {status.script_digest!r} "
            f"does not match this run's {expected_digest!r}. The container is "
            "serving a DIFFERENT script than the corpus asserts against -- a "
            "stale mount, a stale compose project, or an edited question. A "
            "case count cannot detect this, which is why it is not what is "
            "compared."
        )
    return status
