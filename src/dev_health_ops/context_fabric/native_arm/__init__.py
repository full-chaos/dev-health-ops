"""CHAOS-3618: the current/native Ask Dev investigation arm.

Projects what the CURRENT Ask Dev path already assembles — server-owned
interpretation, subject preflight, the deterministic plan executor and the
canonical status/health/workload/deficiency/data-health services — into
``ask_dev_investigation_packet.v1``, the contract CHAOS-3615 froze for both
arms of the corrected CHAOS-3614 trial.

The point of this arm is to be *honest*, not to score well. A missing graph
association is reported missing; native output is never inflated into a
richer relationship claim than the run actually established; and no
case-specific logic exists to make a particular trial question look better.
:mod:`.capabilities` is where every such judgement is written down and
made testable.
"""

from __future__ import annotations

from .capabilities import (
    NATIVE_QUESTION_FAMILY,
    NATIVE_RELATIONSHIP_CAPABILITY,
    NATIVE_SUBJECT_KIND,
    NATIVE_UNOBSERVED_SOURCE_CLASSES,
    OBSERVABLE_SOURCE_CLASSES,
    STATUS_FACT_ENTITY_KIND,
    UNREACHABLE_SUBJECT_KINDS,
    NativeGapMechanism,
    NativeRelationshipCapability,
    NativeRelationshipState,
    classify_question_family,
    comparison_shape_for,
)

__all__ = [
    "NATIVE_QUESTION_FAMILY",
    "NATIVE_RELATIONSHIP_CAPABILITY",
    "NATIVE_SUBJECT_KIND",
    "NATIVE_UNOBSERVED_SOURCE_CLASSES",
    "OBSERVABLE_SOURCE_CLASSES",
    "STATUS_FACT_ENTITY_KIND",
    "UNREACHABLE_SUBJECT_KINDS",
    "NativeGapMechanism",
    "NativeRelationshipCapability",
    "NativeRelationshipState",
    "classify_question_family",
    "comparison_shape_for",
]
