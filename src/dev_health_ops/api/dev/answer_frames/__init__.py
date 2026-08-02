"""Server-owned answer synthesis for CHAOS-3297 (Wave 3.1 answer frames).

Amendment TRD v2 §4.5: the server, not the final model turn, is the source
of truth for an Ask Dev answer. This package holds the pieces that consume
a validated ``dev_answer_frame.v1`` and produce the public ``dev_answer.v2``
without requiring an LLM to have run at all.

CHAOS-3297 stack #4 (narrative fallback) owns two modules here:

* ``narrative_request`` (plan §a P5) — the bounded, allowlisted projection
  of a frame that a certified narrative provider is permitted to see.
* ``narrative_fallback`` (plan requirement 7-8) — the provider call,
  layered validation, and deterministic-fallback selection that make an
  optional narrative unable to change anything the frame already decided.

The six frame builders (status, remaining work, observed change, registered
metrics, metric comparison, data trust), the builder registry, the
deterministic renderer, and outcome/readiness derivation are stack #3's
(CHAOS-3297) territory and land in sibling modules of this package.
"""

from __future__ import annotations
