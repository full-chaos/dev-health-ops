"""Contract for CHAOS-3219 Phase 5's live acceptance lane.

``.github/workflows/ask-dev-acceptance.yml`` is the first workflow that boots
the Ask Dev acceptance stack. Nothing in CI can prove it works before it is
merged -- ``schedule`` only fires on the default branch -- so what this file
pins is everything that can be checked statically: that the triggers are the
ones ruling D1 allows, that the substantive steps cannot be skipped or have
their failure swallowed, and that the reporting job's verdict comes from
``ci/aggregate_gate_results.sh`` with a policy under which a skipped job is
never a pass.

This is the plan doc's §5 item 5 ("static wiring guards: conformance tests
that read the launcher/workflow text and assert every required job is invoked,
in order, with no ``|| true``, no ``continue-on-error``"), applied to the
workflow rather than the launcher.

WHY THE CHECKS ARE FUNCTIONS AND NOT ASSERTIONS. A conformance test that only
asserts against the committed file proves the file passes today; it does not
prove the test would notice if the file stopped passing. Every check below is
a function returning the violations it found, so each one can be run twice:
once against the real workflow, where it must find nothing, and once against a
copy with that specific guard removed, where THAT check -- not merely some
check -- must find something. A mutation that fails to change the text is
reported as INVALID rather than passing quietly.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

pytest.skip(allow_module_level=True, reason="Deprecated Ask Dev")

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "ask-dev-acceptance.yml"
AGGREGATOR = ROOT / "ci" / "aggregate_gate_results.sh"

LIVE_JOB = "acceptance-live"
GATE_JOB = "acceptance-live-gate"
BOOT_STEP = "Boot the acceptance stack (canonical launcher)"
CORPUS_STEP = "Run the armed Wave 4 corpus"

#: The scripts this lane exists to execute. Named here rather than inline so a
#: rename in either repo half fails this test instead of silently un-wiring the
#: lane.
#: `|| <anything>` after a command. `|| true` was the original spelling; the
#: fallback's identity is irrelevant -- any of them supplies the step's exit
#: status in place of the failure's.
_OR_SWALLOW = re.compile(r"\|\|\s*\S")
_SET_PLUS_E = re.compile(r"^\s*set\s+[-+a-z]*\+[a-z]*e", re.MULTILINE)
_SET_PIPEFAIL = re.compile(
    r"^\s*set\s+-[a-z]*o?\s*pipefail|^\s*set\s+-o\s+pipefail", re.MULTILINE
)
_UNSET_PIPEFAIL = re.compile(r"^\s*set\s+\+o\s+pipefail", re.MULTILINE)

LAUNCHER = "scripts/acceptance/run_ask_dev_compose.sh"
CORPUS_RUNNER = "scripts/acceptance/run_wave4_corpus.sh"


# dict[Any, Any], not dict[str, Any]: PyYAML resolves the bare key `on` to
# the BOOLEAN True under YAML 1.1, so a workflow mapping genuinely has a
# non-str key and claiming otherwise would be a type that lies.
CORPUS_DIR = ROOT / "tests" / "acceptance" / "world" / "ask-dev-world.v1" / "corpus"


def _active_corpus_case_count() -> int:
    """How many cases run_report will actually count.

    NOT the file count. 143 files, 91 of them status='active' and 52
    status='declared-blocked' -- and declared-blocked cases execute without
    touching the product, so run_report deliberately excludes them. Reading the
    file count instead is what produced an unsatisfiable floor of 100.
    """
    active = 0
    for path in sorted(CORPUS_DIR.glob("case-*.json")):
        try:
            case = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:  # pragma: no cover - a malformed case is another test's job
            continue
        if case.get("status") == "active":
            active += 1
    return active


def _load(text: str) -> dict[Any, Any]:
    doc = yaml.safe_load(text)
    assert isinstance(doc, dict)
    return doc


def _triggers(doc: dict[Any, Any]) -> dict[str, Any]:
    # ...so the trigger block can arrive under either key depending on how it
    # is quoted in the file.
    block = doc.get("on", doc.get(True))
    assert isinstance(block, dict), "workflow has no mapping-shaped trigger block"
    return block


def _jobs(doc: dict[Any, Any]) -> dict[str, Any]:
    jobs = doc.get("jobs")
    assert isinstance(jobs, dict)
    return jobs


def _steps(doc: dict[Any, Any], job: str) -> list[dict[str, Any]]:
    steps = _jobs(doc).get(job, {}).get("steps", [])
    return [step for step in steps if isinstance(step, dict)]


def _step(doc: dict[Any, Any], job: str, name: str) -> dict[str, Any] | None:
    for step in _steps(doc, job):
        if step.get("name") == name:
            return step
    return None


# --------------------------------------------------------------------------
# Checks. Each returns a list of violations; empty means the guard is intact.
# --------------------------------------------------------------------------


def check_triggers_are_schedule_and_dispatch_only(text: str) -> list[str]:
    """Ruling D1: the live stack run is not a PR-tier job.

    Also refuses ``merge_group``: the merge queue has been disabled repo-wide
    since 2026-08-01 (CHAOS-3519), so that arm would be configuration nobody
    can observe working.
    """
    triggers = _triggers(_load(text))
    violations = []
    if set(triggers) != {"schedule", "workflow_dispatch"}:
        violations.append(
            f"triggers are {sorted(map(str, triggers))}; ruling D1 allows only "
            "schedule + workflow_dispatch for the live stack run"
        )
    schedule = triggers.get("schedule")
    if not isinstance(schedule, list) or not schedule:
        violations.append(
            "schedule trigger carries no cron entry, so nothing ever fires"
        )
    elif not all(isinstance(entry, dict) and entry.get("cron") for entry in schedule):
        violations.append("a schedule entry has no cron expression")
    return violations


def check_live_job_runs_on_a_github_hosted_runner(text: str) -> list[str]:
    """D1 again: GH-hosted, and a hosted label is the only thing that proves it."""
    jobs = _jobs(_load(text))
    if LIVE_JOB not in jobs:
        return [f"the workflow has no {LIVE_JOB} job at all"]
    runs_on = jobs[LIVE_JOB].get("runs-on")
    if not isinstance(runs_on, str) or not runs_on.startswith("ubuntu-"):
        return [f"{LIVE_JOB} runs-on is {runs_on!r}, not a GitHub-hosted ubuntu label"]
    return []


def check_live_job_is_time_bounded(text: str) -> list[str]:
    """A hung stack must release the runner well before GitHub's 6-hour cap."""
    jobs = _jobs(_load(text))
    if LIVE_JOB not in jobs:
        return [f"the workflow has no {LIVE_JOB} job at all"]
    timeout = jobs[LIVE_JOB].get("timeout-minutes")
    if not isinstance(timeout, int) or not 0 < timeout <= 300:
        return [
            f"{LIVE_JOB} timeout-minutes is {timeout!r}; expected a bound under 300"
        ]
    return []


def check_live_job_is_unconditional(text: str) -> list[str]:
    """The gate declares this job ``unconditional``; an ``if:`` would make that a lie."""
    jobs = _jobs(_load(text))
    if LIVE_JOB not in jobs:
        # Not pedantry: `.get(LIVE_JOB, {})` used to return {} for a renamed or
        # deleted job, and `"if" in {}` is False, so the check that owns the
        # unconditional property passed vacuously for the one change that
        # destroys it (adversarial review 2026-08-06).
        return [f"the workflow has no {LIVE_JOB} job at all"]
    job = jobs[LIVE_JOB]
    if "if" in job:
        return [
            f"{LIVE_JOB} carries `if: {job['if']}`, but {GATE_JOB} judges it under the "
            "'unconditional' policy, which treats every skip as unexplained"
        ]
    return []


def check_no_step_swallows_a_failure(text: str) -> list[str]:
    """No ``continue-on-error``, and no shell construct that eats a non-zero exit.

    ``|| true`` alone is not the vocabulary. Adversarial review 2026-08-06
    walked past this check with ``|| :``, ``|| echo skipping`` and a plain
    ``set +e`` -- all of which turn a failed launcher into a green step just as
    completely.
    """
    doc = _load(text)
    violations = []
    for job_name, job in _jobs(doc).items():
        if job.get("continue-on-error"):
            violations.append(f"job {job_name} sets continue-on-error")
        for step in job.get("steps", []):
            if not isinstance(step, dict):
                continue
            label = step.get("name", step.get("uses", "<unnamed>"))
            where = f"step {job_name}/{label}"
            if step.get("continue-on-error"):
                violations.append(f"{where} sets continue-on-error")
            code = "\n".join(_code_lines(str(step.get("run", ""))))
            if _OR_SWALLOW.search(code):
                violations.append(
                    f"{where} follows a command with `||`, which turns its failure into "
                    "a success -- the step's exit status is then the fallback's"
                )
            if _SET_PLUS_E.search(code):
                violations.append(f"{where} disables errexit with `set +e`")
    return violations


def _run_blocks(doc: dict[Any, Any]) -> list[tuple[str, str, str]]:
    blocks = []
    for job_name, job in _jobs(doc).items():
        for step in job.get("steps", []):
            if isinstance(step, dict) and step.get("run"):
                blocks.append(
                    (job_name, str(step.get("name", "<unnamed>")), str(step["run"]))
                )
    return blocks


def _normalize(expression: object) -> str:
    return " ".join(str(expression).split())


def _code_lines(run: str) -> list[str]:
    """The lines of a ``run:`` block that are shell code, not commentary.

    Only WHOLE-line comments are dropped. Splitting every line at the first
    ``#`` would silently truncate a shell string containing one, which turns a
    real ``|`` or ``||`` into an invisible one -- a false negative in a guard,
    which is the direction that matters.
    """
    return [line for line in run.splitlines() if not line.lstrip().startswith("#")]


def _has_real_pipe(line: str) -> bool:
    """True when the line pipes one command into another.

    Written as "remove the `||` operators, then look for a bare `|`" rather
    than "skip lines containing `||`". The old form discarded the whole line,
    so ``cmd | tee log || echo failed`` -- the exact shape this check exists to
    catch, with a fallback bolted on -- was invisible to it.
    """
    return "|" in line.replace("||", "")


def check_piped_steps_set_pipefail(text: str) -> list[str]:
    """A ``cmd | tee log`` step reports *tee's* status without ``pipefail``.

    This is not a style rule. Both substantive steps pipe their output through
    ``tee`` so the logs reach the uploaded artifact, and ``tee`` exits 0 for a
    launcher that aborted -- the exact "a run that booted nothing looks green"
    shape this lane exists to prevent, one character away.
    """
    violations = []
    for job_name, label, run in _run_blocks(_load(text)):
        code = "\n".join(_code_lines(run))
        if not any(_has_real_pipe(line) for line in _code_lines(run)):
            continue
        where = f"step {job_name}/{label}"
        if not _SET_PIPEFAIL.search(code):
            violations.append(
                f"{where} pipes output but never sets pipefail, so the pipeline reports "
                "the last command's status, not the failure's"
            )
        if _UNSET_PIPEFAIL.search(code):
            violations.append(f"{where} turns pipefail back off after setting it")
    return violations


def check_boot_step_invokes_the_canonical_launcher(text: str) -> list[str]:
    """The stack must come up through the launcher, not a copy of its sequence."""
    doc = _load(text)
    step = _step(doc, LIVE_JOB, BOOT_STEP)
    if step is None:
        return [f"{LIVE_JOB} has no step named {BOOT_STEP!r}"]
    violations = []
    run = str(step.get("run", ""))
    if LAUNCHER not in run:
        violations.append(f"{BOOT_STEP} does not invoke {LAUNCHER}")
    if "--web-root" not in run:
        violations.append(
            f"{BOOT_STEP} does not pass --web-root, so the launcher exits 64"
        )
    if str(step.get("env", {}).get("ASK_DEV_ACCEPTANCE_KEEP_STACK")) != "1":
        violations.append(
            f"{BOOT_STEP} does not set ASK_DEV_ACCEPTANCE_KEEP_STACK=1, so the launcher "
            "tears the stack down and the corpus step has nothing to run against"
        )
    if "if" in step:
        violations.append(f"{BOOT_STEP} carries an `if:` and can therefore be skipped")
    return violations


def check_corpus_step_runs_armed_after_the_boot(text: str) -> list[str]:
    """Armed, present, unskippable, and ordered after the boot it depends on."""
    doc = _load(text)
    step = _step(doc, LIVE_JOB, CORPUS_STEP)
    if step is None:
        return [f"{LIVE_JOB} has no step named {CORPUS_STEP!r}"]
    violations = []
    if CORPUS_RUNNER not in str(step.get("run", "")):
        violations.append(f"{CORPUS_STEP} does not invoke {CORPUS_RUNNER}")
    if str(step.get("env", {}).get("ASK_DEV_LIVE_ACCEPTANCE")) != "1":
        violations.append(
            f"{CORPUS_STEP} does not export ASK_DEV_LIVE_ACCEPTANCE=1, so the runner "
            "exits 64 rather than executing the corpus"
        )
    if "if" in step:
        violations.append(
            f"{CORPUS_STEP} carries an `if:` and can therefore be skipped"
        )

    names = [candidate.get("name") for candidate in _steps(doc, LIVE_JOB)]
    if BOOT_STEP in names and names.index(CORPUS_STEP) < names.index(BOOT_STEP):
        violations.append(
            f"{CORPUS_STEP} runs before {BOOT_STEP}; the stack is not up yet"
        )
    return violations


def check_corpus_talks_to_the_port_the_stack_published(text: str) -> list[str]:
    """The corpus must query the API this run actually booted.

    The launcher publishes ASK_DEV_ACCEPTANCE_API_PORT; the corpus reads
    ASK_DEV_ACCEPTANCE_API_URL. They are two variables holding one fact, and if
    they drift the corpus points at a port nothing is listening on -- which
    fails, loudly, but only after a full stack build. Cheaper to fail here.
    """
    doc = _load(text)
    port = str(
        _jobs(doc)
        .get(LIVE_JOB, {})
        .get("env", {})
        .get("ASK_DEV_ACCEPTANCE_API_PORT", "")
    )
    step = _step(doc, LIVE_JOB, CORPUS_STEP)
    if step is None:
        return [f"{LIVE_JOB} has no step named {CORPUS_STEP!r}"]
    url = str(step.get("env", {}).get("ASK_DEV_ACCEPTANCE_API_URL", ""))
    if not port:
        return [f"{LIVE_JOB} does not pin ASK_DEV_ACCEPTANCE_API_PORT"]
    if not url.endswith(f":{port}"):
        return [
            f"the corpus queries {url!r} but the stack publishes port {port}; one of the two "
            "was changed without the other"
        ]
    return []


QUOTA_STEP = "Read the quota ceilings the stack enforces"
QUOTA_VARS = (
    "ASK_DEV_PLATFORM_MONTHLY_REQUEST_MAX",
    "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD",
)


def check_corpus_is_budgeted_from_the_running_stack(text: str) -> list[str]:
    """The corpus runner refuses to start un-budgeted.

    QuotaBudget raises QuotaConfigurationError in every case's setup unless
    both ceilings are in its own process environment -- observed live before
    merge: the boot passed and 91 of 144 cases errored in setup in nine
    seconds. The values must come from the RUNNING stack, not from a literal
    in this workflow: a second copy of a number the stack enforces is a copy
    that drifts, and budgeting against a ceiling nobody enforces is worse than
    not budgeting.
    """
    doc = _load(text)
    step = _step(doc, LIVE_JOB, QUOTA_STEP)
    if step is None:
        return [f"{LIVE_JOB} has no step named {QUOTA_STEP!r}"]
    violations = []
    run = str(step.get("run", ""))
    for name in QUOTA_VARS:
        if name not in run:
            violations.append(f"{QUOTA_STEP} never reads {name}")
    if "printenv" not in run or "exec -T api" not in run:
        violations.append(
            f"{QUOTA_STEP} does not read the ceilings out of the running api container, "
            "so the runner may budget against a value the stack does not enforce"
        )
    if "GITHUB_ENV" not in run:
        violations.append(f"{QUOTA_STEP} never exports the ceilings to later steps")
    if "if" in step:
        violations.append(f"{QUOTA_STEP} carries an `if:` and can therefore be skipped")

    names = [candidate.get("name") for candidate in _steps(doc, LIVE_JOB)]
    if BOOT_STEP in names and CORPUS_STEP in names:
        if (
            not names.index(BOOT_STEP)
            < names.index(QUOTA_STEP)
            < names.index(CORPUS_STEP)
        ):
            violations.append(
                f"{QUOTA_STEP} must run after {BOOT_STEP} (the stack must exist to read from) "
                f"and before {CORPUS_STEP} (which needs the values)"
            )
    return violations


def check_corpus_has_an_attrition_floor(text: str) -> list[str]:
    """run_report's default floor is 1, which is the wrong floor for a lane.

    Adversarial review 2026-08-06 (MEDIUM-HIGH, reproduced with a synthetic
    JUnit report): a run executing ONE of ~144 cases exits 0 and this lane
    reports green having certified 0.7% of the corpus, because nothing between
    the case-file glob and the executed-count assertion knows how many cases
    there are supposed to be. This does not fix that -- an exact expected-case
    registry is Lane 5a's -- it just refuses to run with the floor of 1.
    """
    doc = _load(text)
    step = _step(doc, LIVE_JOB, CORPUS_STEP)
    if step is None:
        return [f"{LIVE_JOB} has no step named {CORPUS_STEP!r}"]
    raw = str(step.get("env", {}).get("ASK_DEV_CORPUS_MIN_EXECUTED", ""))
    if not raw.isdigit():
        return [
            f"{CORPUS_STEP} does not pin ASK_DEV_CORPUS_MIN_EXECUTED, so the run falls "
            "back to run_report's floor of 1 and a corpus that lost most of its case "
            "files would still report green"
        ]
    floor = int(raw)
    if floor < 2:
        return [
            f"{CORPUS_STEP} pins ASK_DEV_CORPUS_MIN_EXECUTED={raw}, which is the default "
            "floor this check exists to raise"
        ]
    active = _active_corpus_case_count()
    if floor > active:
        return [
            f"{CORPUS_STEP} pins ASK_DEV_CORPUS_MIN_EXECUTED={floor}, but only {active} "
            "corpus cases are status='active' and run_report counts no others -- this "
            "floor is UNSATISFIABLE and would fail every run however healthy. Dispatch "
            "run 1 failed exactly this way at 91 < 100."
        ]
    return []


DRAIN_STEP = "Drain the per-IP login window before the corpus"


def check_the_login_window_is_drained_before_the_corpus(text: str) -> list[str]:
    """The corpus cannot log in inside the boot's own rate-limit window.

    Dispatch run 1 (31146779949): all 91 active cases errored in setup on
    ``HTTP 429: Rate limit exceeded``. ``AUTH_LOGIN_IP_LIMIT`` is
    ``20/15minutes`` and PER IP; a CI job is one IP, and boot + corpus exceed
    20 logins in one window when they run back to back. Locally they are
    minutes apart, so no local run could have found this.

    A workaround, pinned so it cannot be quietly shortened into
    uselessness -- and so its removal is a deliberate act once session reuse
    lands, not an accident.
    """
    doc = _load(text)
    step = _step(doc, LIVE_JOB, DRAIN_STEP)
    if step is None:
        return [f"{LIVE_JOB} has no step named {DRAIN_STEP!r}"]
    violations = []
    run = str(step.get("run", ""))
    match = re.search(r"sleep\s+(\d+)", run)
    if match is None:
        violations.append(f"{DRAIN_STEP} does not wait at all")
    elif int(match.group(1)) < 900:
        violations.append(
            f"{DRAIN_STEP} waits {match.group(1)}s, under the 900s the "
            "20/15minutes window needs -- a partial drain still 429s, and does it "
            "after paying most of the wait"
        )
    if "if" in step:
        violations.append(f"{DRAIN_STEP} carries an `if:` and can therefore be skipped")

    names = [candidate.get("name") for candidate in _steps(doc, LIVE_JOB)]
    if BOOT_STEP in names and CORPUS_STEP in names:
        if (
            not names.index(BOOT_STEP)
            < names.index(DRAIN_STEP)
            < names.index(CORPUS_STEP)
        ):
            violations.append(
                f"{DRAIN_STEP} must run after {BOOT_STEP} (whose logins fill the window) "
                f"and before {CORPUS_STEP} (which needs it drained)"
            )
    return violations


def check_teardown_and_capture_always_run(text: str) -> list[str]:
    """Cleanup and log capture must survive a failed or cancelled run."""
    doc = _load(text)
    violations = []
    for name in (
        "Capture stack state and container logs",
        "Tear down the acceptance stack",
    ):
        step = _step(doc, LIVE_JOB, name)
        if step is None:
            violations.append(f"{LIVE_JOB} has no step named {name!r}")
        elif str(step.get("if", "")).strip() != "always()":
            violations.append(f"step {name!r} is not `if: always()`")
    return violations


def check_artifact_uploads_fail_on_an_empty_set(text: str) -> list[str]:
    """An upload that finds nothing and passes is an absent measurement reading as coverage."""
    doc = _load(text)
    uploads = [
        step
        for step in _steps(doc, LIVE_JOB)
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
    ]
    if len(uploads) < 2:
        return [f"expected receipt and log artifact uploads; found {len(uploads)}"]
    violations = []
    for step in uploads:
        label = step.get("name", "<unnamed>")
        with_block = step.get("with", {})
        if with_block.get("if-no-files-found") != "error":
            violations.append(
                f"upload step {label!r} does not set if-no-files-found: error, so a run "
                "that produced no evidence would upload nothing and report success"
            )
        if str(step.get("if", "")).strip() != "always()":
            violations.append(f"upload step {label!r} is not `if: always()`")
    return violations


def check_web_checkout_is_wired_for_the_playwright_leg(text: str) -> list[str]:
    """The launcher's final leg lives in dev-health-web; without it the run is half a run."""
    doc = _load(text)
    checkouts = [
        step
        for step in _steps(doc, LIVE_JOB)
        if str(step.get("uses", "")).startswith("actions/checkout@")
    ]
    web = [
        step
        for step in checkouts
        if str(step.get("with", {}).get("repository", "")).endswith("dev-health-web")
    ]
    if not web:
        return [
            f"{LIVE_JOB} never checks out dev-health-web, so the Playwright leg cannot run"
        ]
    if not str(web[0].get("with", {}).get("token", "")):
        return [
            "the dev-health-web checkout carries no token; github.token cannot read it"
        ]
    return []


def check_gate_job_delegates_to_the_shared_aggregator(text: str) -> list[str]:
    """The reporting job's verdict comes from the script whose rules are tested."""
    doc = _load(text)
    job = _jobs(doc).get(GATE_JOB)
    if job is None:
        return [f"the workflow has no {GATE_JOB} job, so nothing reports a verdict"]
    violations = []
    if job.get("needs") != [LIVE_JOB]:
        violations.append(
            f"{GATE_JOB} needs {job.get('needs')!r}, expected [{LIVE_JOB!r}]"
        )
    if str(job.get("if", "")).strip() != "always()":
        violations.append(
            f"{GATE_JOB} is `if: {job.get('if')}`; under anything but always() a cancelled "
            "run leaves this check skipped, which branch protection counts as satisfied"
        )

    step = next(
        (
            s
            for s in _steps(doc, GATE_JOB)
            if "aggregate_gate_results.sh" in str(s.get("run", ""))
        ),
        None,
    )
    if step is None:
        violations.append(f"{GATE_JOB} does not run ci/aggregate_gate_results.sh")
        return violations

    env = step.get("env", {})
    if env.get("GATE_HAS_SELECTOR") != "false":
        violations.append(
            "the gate step must declare GATE_HAS_SELECTOR=false: this workflow has no "
            "`changes` job, and an empty selector result must not be inferred"
        )
    if "CHANGES_RESULT" in env or "CHANGES_CODE" in env:
        violations.append(
            "the gate step passes changes inputs it has no selector job to produce"
        )
    gated = str(env.get("GATED_JOB_1", ""))
    if not gated.startswith(f"{LIVE_JOB}|unconditional|"):
        violations.append(
            f"GATED_JOB_1 is {gated!r}; expected '{LIVE_JOB}|unconditional|<result>' so that a "
            "skipped acceptance-live can never read as a pass"
        )
    if f"needs.{LIVE_JOB}.result" not in gated:
        violations.append("GATED_JOB_1 does not read the live job's real result")
    if not any(
        str(s.get("uses", "")).startswith("actions/checkout@")
        for s in _steps(doc, GATE_JOB)
    ):
        violations.append(
            f"{GATE_JOB} never checks out the repo the aggregator script lives in"
        )
    return violations


NOTIFY_JOB = "report-failure"


def check_a_failing_run_is_announced(text: str) -> list[str]:
    """A detector nobody reads is the failure it exists to catch, moved later.

    Both adversarial reviewers raised this independently (2026-08-06, MEDIUM):
    the gate flips the RUN CONCLUSION to failure, but this workflow fires only
    on schedule/dispatch, so its check can never be a branch-protection
    requirement and no repo mechanism watches run conclusions. Without an
    announcement the only channel is the Actions tab.
    """
    doc = _load(text)
    jobs = _jobs(doc)
    if NOTIFY_JOB not in jobs:
        return [
            f"the workflow has no {NOTIFY_JOB} job, so a red nightly announces nothing"
        ]
    job = jobs[NOTIFY_JOB]
    violations = []
    if GATE_JOB not in (job.get("needs") or []):
        violations.append(
            f"{NOTIFY_JOB} does not depend on {GATE_JOB}, so it cannot read the verdict"
        )
    condition = _normalize(job.get("if", ""))
    if "always()" not in condition:
        violations.append(
            f"{NOTIFY_JOB} is `if: {job.get('if')}` -- without always() it is itself "
            "skipped on the runs it exists to report"
        )
    if f"needs.{GATE_JOB}.result != 'success'" not in condition:
        violations.append(
            f"{NOTIFY_JOB} must fire on any non-success gate result, not only on "
            f"failure(): a cancelled or skipped gate is also an unreported night "
            f"(condition is {condition!r})"
        )
    if job.get("permissions", {}).get("issues") != "write":
        violations.append(f"{NOTIFY_JOB} cannot write an issue without issues: write")
    run = "\n".join(
        line
        for step in _steps(doc, NOTIFY_JOB)
        for line in _code_lines(str(step.get("run", "")))
    )
    if "gh issue create" not in run or "gh issue comment" not in run:
        violations.append(
            f"{NOTIFY_JOB} neither creates nor comments on an issue, so nothing is announced"
        )
    # The message must teach the reader to distinguish the failure shapes.
    # Exit 66 acquired a SECOND meaning when the attrition floor was added --
    # "executed nothing" and "executed below the floor" are different findings
    # and dispatch run 1 was the second -- so the message has to name both, not
    # just the executed-count line.
    for phrase in ("Exit 66", "attrition floor", "Exit 67", "unmeasured, not failing"):
        if phrase not in run:
            violations.append(
                f"{NOTIFY_JOB}'s message omits {phrase!r}, so a reader cannot tell which "
                "kind of red this was"
            )
    return violations


CHECKS: dict[str, Callable[[str], list[str]]] = {
    "triggers": check_triggers_are_schedule_and_dispatch_only,
    "hosted-runner": check_live_job_runs_on_a_github_hosted_runner,
    "timeout": check_live_job_is_time_bounded,
    "unconditional-job": check_live_job_is_unconditional,
    "no-swallowed-failures": check_no_step_swallows_a_failure,
    "pipefail": check_piped_steps_set_pipefail,
    "boot-step": check_boot_step_invokes_the_canonical_launcher,
    "corpus-step": check_corpus_step_runs_armed_after_the_boot,
    "api-port": check_corpus_talks_to_the_port_the_stack_published,
    "quota-budget": check_corpus_is_budgeted_from_the_running_stack,
    "attrition-floor": check_corpus_has_an_attrition_floor,
    "login-drain": check_the_login_window_is_drained_before_the_corpus,
    "always-cleanup": check_teardown_and_capture_always_run,
    "artifact-uploads": check_artifact_uploads_fail_on_an_empty_set,
    "web-checkout": check_web_checkout_is_wired_for_the_playwright_leg,
    "gate-job": check_gate_job_delegates_to_the_shared_aggregator,
    "failure-announced": check_a_failing_run_is_announced,
}


@pytest.fixture(scope="module")
def workflow_text() -> str:
    return WORKFLOW_PATH.read_text(encoding="utf-8")


@pytest.mark.parametrize("check_id", sorted(CHECKS))
def test_committed_workflow_satisfies_the_contract(
    check_id: str, workflow_text: str
) -> None:
    # Given the workflow as committed
    # When each contract check reads it
    # Then it finds nothing to report.
    assert CHECKS[check_id](workflow_text) == []


#: (check id, literal to replace, replacement). Each removes exactly ONE guard,
#: so the named check -- not merely some check -- has to notice. Clause-level,
#: per the repo's third verification rule.
_MUTATIONS: list[tuple[str, str, str]] = [
    # A PR-tier arm on the live stack run, which ruling D1 forbids.
    ("triggers", "  workflow_dispatch:\n", "  workflow_dispatch:\n  pull_request:\n"),
    # The nightly staleness detector with nothing to fire it.
    ("triggers", '    - cron: "23 4 * * *"', "    []"),
    (
        "hosted-runner",
        f"  {LIVE_JOB}:\n    runs-on: ubuntu-latest",
        f"  {LIVE_JOB}:\n    runs-on: self-hosted",
    ),
    ("timeout", "    timeout-minutes: 180", "    timeout-minutes: 0"),
    # The classic false green: the whole job made conditional on something that
    # can evaluate false, under a gate that calls skips unexplained.
    (
        "unconditional-job",
        f"  {LIVE_JOB}:\n    runs-on: ubuntu-latest",
        f"  {LIVE_JOB}:\n    if: github.event_name == 'push'\n    runs-on: ubuntu-latest",
    ),
    (
        "no-swallowed-failures",
        "./scripts/acceptance/run_wave4_corpus.sh 2>&1",
        "./scripts/acceptance/run_wave4_corpus.sh 2>&1 || true",
    ),
    (
        "no-swallowed-failures",
        "      - name: Run the armed Wave 4 corpus\n        working-directory: ops",
        "      - name: Run the armed Wave 4 corpus\n        continue-on-error: true\n        working-directory: ops",
    ),
    # One character: without pipefail the launcher's failure is hidden by tee's
    # exit status and the job goes green having booted nothing.
    (
        "pipefail",
        '          set -euo pipefail\n          mkdir -p "${LOG_DIR}"\n          ./scripts/acceptance/run_ask_dev_compose.sh',
        '          set -eu\n          mkdir -p "${LOG_DIR}"\n          ./scripts/acceptance/run_ask_dev_compose.sh',
    ),
    (
        "boot-step",
        '          ASK_DEV_ACCEPTANCE_KEEP_STACK: "1"',
        '          ASK_DEV_ACCEPTANCE_KEEP_STACK: "0"',
    ),
    (
        "boot-step",
        './scripts/acceptance/run_ask_dev_compose.sh --web-root "${WEB_ROOT}"',
        "true",
    ),
    ("corpus-step", "./scripts/acceptance/run_wave4_corpus.sh 2>&1", "true 2>&1"),
    (
        "corpus-step",
        '          ASK_DEV_LIVE_ACCEPTANCE: "1"',
        '          ASK_DEV_LIVE_ACCEPTANCE: "0"',
    ),
    (
        "quota-budget",
        "      - name: Read the quota ceilings the stack enforces",
        "      - name: Read the quota ceilings the stack enforces\n        if: false",
    ),
    (
        "quota-budget",
        'exec -T api printenv "${name}"',
        'echo 1000 # "${name}"',
    ),
    (
        "api-port",
        "          ASK_DEV_ACCEPTANCE_API_URL: http://127.0.0.1:18080",
        "          ASK_DEV_ACCEPTANCE_API_URL: http://127.0.0.1:18099",
    ),
    (
        "always-cleanup",
        "      - name: Tear down the acceptance stack\n        if: always()",
        "      - name: Tear down the acceptance stack",
    ),
    (
        "artifact-uploads",
        "          if-no-files-found: error",
        "          if-no-files-found: ignore",
    ),
    (
        "web-checkout",
        "          repository: full-chaos/dev-health-web",
        "          repository: full-chaos/dev-health-ops",
    ),
    (
        "gate-job",
        '          GATE_HAS_SELECTOR: "false"',
        '          GATE_HAS_SELECTOR: "true"',
    ),
    (
        "gate-job",
        f'          GATED_JOB_1: "{LIVE_JOB}|unconditional|',
        f'          GATED_JOB_1: "{LIVE_JOB}|path-filtered|',
    ),
    (
        "gate-job",
        "    if: always()\n    runs-on: ubuntu-latest",
        "    if: '!cancelled()'\n    runs-on: ubuntu-latest",
    ),
    ("gate-job", "        run: ./ci/aggregate_gate_results.sh", "        run: echo ok"),
    # --- clauses adversarial review (2026-08-06) found live but unpinned ---
    (
        "triggers",
        '    - cron: "23 4 * * *"',
        "    - {}",
    ),
    (
        "timeout",
        "    timeout-minutes: 180\n",
        "",
    ),
    (
        "no-swallowed-failures",
        f"  {LIVE_JOB}:\n    runs-on: ubuntu-latest",
        f"  {LIVE_JOB}:\n    continue-on-error: true\n    runs-on: ubuntu-latest",
    ),
    (
        "boot-step",
        "      - name: Boot the acceptance stack (canonical launcher)\n        working-directory: ops",
        "      - name: Boot the acceptance stack (canonical launcher)\n        if: false\n        working-directory: ops",
    ),
    (
        "boot-step",
        "      - name: Boot the acceptance stack (canonical launcher)",
        "      - name: Boot the stack sometime",
    ),
    (
        "corpus-step",
        "      - name: Run the armed Wave 4 corpus\n        working-directory: ops",
        "      - name: Run the armed Wave 4 corpus\n        if: github.event_name == 'push'\n        working-directory: ops",
    ),
    (
        "api-port",
        '      ASK_DEV_ACCEPTANCE_API_PORT: "18080"\n',
        "",
    ),
    (
        "quota-budget",
        'echo "${name}=${value}" >> "${GITHUB_ENV}"',
        'echo "${name}=${value}"',
    ),
    (
        "quota-budget",
        "ASK_DEV_PLATFORM_MONTHLY_COST_MAX_MICROUSD; do",
        "; do",
    ),
    (
        "attrition-floor",
        '          ASK_DEV_CORPUS_MIN_EXECUTED: "75"',
        '          ASK_DEV_CORPUS_MIN_EXECUTED: "1"',
    ),
    (
        "always-cleanup",
        "      - name: Capture stack state and container logs\n        if: always()",
        "      - name: Capture stack state and container logs",
    ),
    (
        "artifact-uploads",
        "      - name: Upload corpus receipts and JUnit report\n        if: always()",
        "      - name: Upload corpus receipts and JUnit report",
    ),
    (
        "web-checkout",
        "          token: ${{ secrets.GH_TOKEN }}\n",
        "",
    ),
    (
        "gate-job",
        f"    needs: [{LIVE_JOB}]",
        "    needs: [acceptance-live-gate]",
    ),
    (
        "gate-job",
        '          GATE_HAS_SELECTOR: "false"\n',
        '          GATE_HAS_SELECTOR: "false"\n          CHANGES_RESULT: "success"\n',
    ),
    (
        "gate-job",
        f'          GATED_JOB_1: "{LIVE_JOB}|unconditional|${{{{ needs.{LIVE_JOB}.result }}}}"',
        f'          GATED_JOB_1: "{LIVE_JOB}|unconditional|success"',
    ),
    (
        "gate-job",
        "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n      - name: Aggregate acceptance-live result",
        "      - name: Aggregate acceptance-live result",
    ),
    (
        "failure-announced",
        "  report-failure:\n    needs: [acceptance-live, acceptance-live-gate]",
        "  report-failure:\n    needs: [acceptance-live]",
    ),
    (
        "failure-announced",
        "    if: always() && needs.acceptance-live-gate.result != 'success'",
        "    if: failure()",
    ),
    (
        "failure-announced",
        "      issues: write",
        "      issues: read",
    ),
    (
        "failure-announced",
        "            gh issue create --repo",
        "            echo would-create --repo",
    ),
    (
        "failure-announced",
        '            "Exit 66 has TWO meanings; the message text distinguishes them." \\',
        '            "Something went wrong." \\',
    ),
    (
        "login-drain",
        "          sleep 960",
        "          sleep 5",
    ),
    (
        "login-drain",
        "      - name: Drain the per-IP login window before the corpus\n        run: |",
        "      - name: Drain the per-IP login window before the corpus\n        if: false\n        run: |",
    ),
    (
        "attrition-floor",
        '          ASK_DEV_CORPUS_MIN_EXECUTED: "75"',
        '          ASK_DEV_CORPUS_MIN_EXECUTED: "100"',
    ),
]


@pytest.mark.parametrize(
    ("check_id", "original", "replacement"),
    _MUTATIONS,
    ids=[f"{check_id}-{index}" for index, (check_id, _, _) in enumerate(_MUTATIONS)],
)
def test_each_guard_rejects_the_workflow_without_it(
    check_id: str, original: str, replacement: str, workflow_text: str
) -> None:
    # Given a copy of the workflow with exactly one guard removed
    mutated = workflow_text.replace(original, replacement, 1)
    # A mutation that did not apply proves nothing -- INVALID, not KILLED.
    assert mutated != workflow_text, (
        f"mutation for {check_id!r} did not match the committed workflow; the anchor text "
        f"{original!r} has moved, so this row is INVALID and is testing nothing"
    )
    # When the check that owns that guard reads the copy
    # Then it reports the violation, rather than some other check happening to.
    assert CHECKS[check_id](mutated), (
        f"check {check_id!r} accepted a workflow with its own guard removed"
    )


def test_the_aggregator_understands_the_policy_this_workflow_asks_for() -> None:
    # Given the workflow hands ci/aggregate_gate_results.sh a policy name
    # When that script is read
    # Then the name is one it implements -- an unrecognized policy would be
    # caught at run time, i.e. nightly, which is the wrong place to learn it.
    assert "unconditional)" in AGGREGATOR.read_text(encoding="utf-8")
    assert "GATE_HAS_SELECTOR" in AGGREGATOR.read_text(encoding="utf-8")


# --------------------------------------------------------------------------
# The corpus runner's side of the attrition floor. The workflow pins the value;
# these execute the script that consumes it.
# --------------------------------------------------------------------------

CORPUS_RUNNER_PATH = ROOT / CORPUS_RUNNER


@pytest.fixture
def corpus_runner_tree(tmp_path: Path) -> Path:
    """The real runner script, in a synthetic tree with a stub interpreter.

    The script derives its ops_root from ``BASH_SOURCE`` and refuses to run at
    all when ``<ops_root>/.venv/bin/python`` is absent -- which is the correct
    behaviour, and which made the first version of these tests pass locally for
    the wrong reason and fail on a GitHub runner, where dependencies are pip
    installed and no ``.venv`` exists. Copying the script's REAL BYTES into a
    tmp tree with a stubbed interpreter exercises the same file everywhere,
    instead of testing whichever machine happens to have a venv.
    """
    script = tmp_path / "scripts" / "acceptance" / CORPUS_RUNNER_PATH.name
    script.parent.mkdir(parents=True)
    script.write_bytes(CORPUS_RUNNER_PATH.read_bytes())
    script.chmod(0o755)

    interpreter = tmp_path / ".venv" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    # Loud rather than silent: every assertion below expects the script to
    # refuse BEFORE it needs an interpreter, so if one is ever invoked the test
    # fails with a message saying so instead of quietly succeeding.
    interpreter.write_text(
        "#!/bin/sh\necho 'stub interpreter invoked' >&2\nexit 99\n", encoding="utf-8"
    )
    interpreter.chmod(0o755)
    return script


def _run_corpus_runner(
    script: Path, min_executed: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(script)],
        capture_output=True,
        text=True,
        check=False,
        cwd=script.parent,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "HOME": str(script.parent),
            "ASK_DEV_LIVE_ACCEPTANCE": "1",
            "ASK_DEV_CORPUS_MIN_EXECUTED": min_executed,
        },
    )


@pytest.mark.parametrize("value", ["0", "-1", "many", "1.5", " 100", "100 "])
def test_a_floor_that_is_not_a_positive_integer_is_refused(
    value: str, corpus_runner_tree: Path
) -> None:
    # Given a floor this script cannot act on
    proc = _run_corpus_runner(corpus_runner_tree, value)

    # Then it refuses, rather than letting the value fall through to
    # run_report's default of 1 -- which is precisely the floor the variable
    # exists to raise, so silently defaulting would restore the defect while
    # looking configured.
    assert proc.returncode == 64, f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    assert "must be a positive integer" in proc.stderr


def test_the_floor_is_validated_before_the_corpus_runs(
    corpus_runner_tree: Path,
) -> None:
    # Given a bad floor, the refusal must arrive before pytest is invoked --
    # a config error that costs a twelve-minute corpus run first is a config
    # error nobody fixes early.
    proc = _run_corpus_runner(corpus_runner_tree, "nope")

    assert proc.returncode == 64
    assert "armed corpus run" not in proc.stdout
    assert "pytest exit" not in proc.stdout
    # And it got there without needing an interpreter at all.
    assert "stub interpreter invoked" not in proc.stderr


def test_the_floor_is_forwarded_to_the_assertion_that_uses_it() -> None:
    # The two halves of "the floor works" are validated differently, and this
    # is the half a stack-free test can only reach as text: proving the runner
    # actually HANDS the value to run_report requires a booted stack, because
    # everything before that line is a twelve-minute pytest session.
    # A mutation that accepted the variable and then dropped it -- leaving the
    # default of 1 in force while the log printed the configured floor --
    # survived every executable test here, so these two assertions exist.
    runner = CORPUS_RUNNER_PATH.read_text(encoding="utf-8")
    assert (
        'min_executed_args=(--min-executed "${ASK_DEV_CORPUS_MIN_EXECUTED}")' in runner
    )
    assert (
        'run_report "${report}" "${min_executed_args[@]+"${min_executed_args[@]}"}"'
        in runner
    )


@pytest.mark.parametrize(
    ("executed", "floor", "expected_exit"),
    [
        pytest.param(3, 2, 0, id="above-the-floor-passes"),
        pytest.param(2, 2, 0, id="exactly-the-floor-passes"),
        pytest.param(1, 2, 66, id="below-the-floor-is-NOT-EXECUTED"),
        # The defect the floor exists for: 1 of many, which the default floor
        # of 1 reports as a pass.
        pytest.param(1, 100, 66, id="one-case-of-a-hundred-is-refused"),
    ],
)
def test_run_report_honours_the_floor(
    tmp_path: Path, executed: int, floor: int, expected_exit: int
) -> None:
    # Given a JUnit report describing a run that executed `executed` real
    # corpus cases -- built in the shape run_report actually parses
    cases = "".join(
        f'<testcase classname="tests.acceptance.test_wave4_corpus_runner_live" '
        f'name="test_corpus_case[case-{index}]" time="0.1"></testcase>'
        for index in range(executed)
    )
    report = tmp_path / "junit-corpus.xml"
    report.write_text(
        f'<?xml version="1.0" encoding="utf-8"?><testsuites><testsuite '
        f'name="pytest" errors="0" failures="0" skipped="0" tests="{executed}" '
        f'time="1.0">{cases}</testsuite></testsuites>',
        encoding="utf-8",
    )

    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.acceptance.corpus.run_report",
            str(report),
            "--min-executed",
            str(floor),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=ROOT,
        env={
            "PATH": "/usr/bin:/bin:/usr/local/bin",
            "HOME": str(ROOT),
            "PYTHONPATH": f"{ROOT / 'src'}:{ROOT}",
        },
    )

    # Then the floor decides the verdict, and a short run exits with the
    # distinct EXIT_NOT_EXECUTED code rather than a pytest-looking 1.
    assert proc.returncode == expected_exit, (
        f"stdout={proc.stdout!r} stderr={proc.stderr!r}"
    )
