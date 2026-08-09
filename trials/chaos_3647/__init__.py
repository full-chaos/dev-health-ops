"""CHAOS-3647: the semantic/hybrid retrieval leg, measured against the baseline.

The CHAOS-3619 trial ran ``DeterministicEmbedder`` and exact-id subject
resolution. Independent review gate 2 was right that this supports claims
about graph traversal and structured alias association and supports **no**
claim about Graphiti's semantic value — "the project that kept cycling in
review" is not a lookup.

This package adds a leg, and only a leg. The deterministic run in
``trials/chaos_3619/results/`` remains the pinned baseline; nothing here
writes to it, imports its results, or changes how it resolved anything. What
is measured is a single substitution: replace stored-text lookup with
Graphiti's own BM25 and cosine primitives over vectors a real embedding model
wrote, hold the projection, partition, questions and authorization set fixed,
and record what changes.

**The three legs, and why three rather than two.** The deterministic leg
failed the colloquial cases at *mention extraction*, not at matching: the
production interpreter yields no mention at all for "What about the auth
work?". A two-leg comparison would therefore attribute an extraction gap to
retrieval and call it semantic value. So the deterministic path is run twice
— once over extracted mentions, which reproduces the pinned baseline, and
once over the raw question — and the semantic leg is run over the raw
question, which is the input a retriever is actually for. The middle leg is
what makes the delta attributable.
"""

from __future__ import annotations

__all__ = ["SEMANTIC_TRIAL_SCHEMA_VERSION"]

#: This artifact's own shape version, distinct from the CHAOS-3619 trial's.
#: A consumer reading both must be able to tell them apart without guessing
#: from field names.
SEMANTIC_TRIAL_SCHEMA_VERSION = "chaos_3647_semantic_leg_results.v1"
