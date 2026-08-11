"""Baseline-component arm adapters (bring-up step 1: harness-design.md §7).

Each module here exposes one function, ``answer(oracle) -> ArmResponse`` --
the only arm-specific code any oracle ever sees, matching the discipline
``runner.py`` and ``oracles.py`` already hold every other arm to.

**Step 1 scope, stated plainly.** These adapters run against the pinned
corpus (:mod:`corpus.ground_truth`) directly -- there is no separate fixture
file to keep in sync, and no live stack, no LLM call, no environment slot.
That is deliberate: harness-design.md's bring-up table lists step 1 ("Arm
adapters against fixture data") as needing no slot and no landed dependency,
distinct from step 5 ("Arms N and E measured"), which is gated on
CHAOS-3563/3564/3565 landing.

**What step 1 can and cannot prove.** The pinned corpus is one static
snapshot of the world. Several oracles ask about a *different* snapshot --
a repo revoked, a source redacted, a watermark stalled, extraction down, the
graph store outaged -- states golden.py's test-only ``Scenario`` machinery
can select between but that a real query-time adapter has no wire-level way
to be told about (no such parameter exists in ``TemporalContextQuery``, on
purpose -- a real system reads its OWN live/current state, it is not handed
"which alternate world to pretend is real"). Against a single static
snapshot, an adapter cannot represent "revoked *for this one question* but
not for the others" -- that needs a real, stateful backend, which is exactly
what steps 3-4 (kind bring-up, projector) exist to provide. Both adapters
below therefore honestly answer against ONE current-world snapshot
(``gt.ALPHA_FULL_VISIBILITY``, nothing deleted or redacted) and are expected
to fail the scenario-specific oracles (staleness, redaction, revocation,
extraction-down, graph-outage, squash-coverage) for that reason -- not
because of a bug in the adapter, but because those oracles test a dynamic
state step 1 cannot simulate. See each module's docstring for what it can
answer for real.
"""

from __future__ import annotations
