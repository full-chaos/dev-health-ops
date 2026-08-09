"""CHAOS-3619: the comparative graph-assisted Ask Dev product-value trial.

Both arms -- the native baseline and the Graphiti-backed graph arm -- run
over the frozen CHAOS-3616 corpus, hand their packet to the real CHAOS-3618
shadow seam, and are scored by the frozen oracles per question family and
per evaluation dimension.

**Deliberately outside ``testpaths``.** ``pytest.ini`` collects ``tests``
only, so nothing here runs as a side effect of the standard suite. That is
the same choice ``trials/chaos_3499`` made and it exists for one reason: a
trial that has not been run must read as NOT MEASURED, loudly, rather than
as a green suite. The guards that protect the runner's own logic DO live
under ``tests/`` and are gate-covered; the measurement itself is an explicit
invocation that writes an artifact.

**Raw records are the source of truth.** ``run_trial.py`` writes
``docs/trial-results.records.json``; ``report.py`` renders the markdown from
that file and never from a live run. A committed test re-derives the
report's load-bearing claims from the records, so the document cannot drift
away from the measurement that produced it.

**No aggregate score, anywhere.** The frozen scoring registry types
``aggregate_prohibited`` as ``Literal[True]`` on all 28 dimensions. A single
headline number would route around that in the one place a reader would
actually look, and the correction addendum names that as the failure mode:
one arm improving ambiguity while harming driver precision must stay
visible.
"""

from __future__ import annotations
