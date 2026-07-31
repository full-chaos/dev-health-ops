"""``dev_narrative.v1`` — presentation-only optional provider output.

Amendment TRD v2 §4.5: "Optional provider output may include only
presentation text mapped to existing frame section IDs. It cannot add
subjects, facts, numbers, metrics, outcomes, health states, severity,
evidence, or recommendations not already present in the frame."

Cross-object consistency against a specific ``DevAnswerFrame`` (does every
referenced fact/section ID actually exist in that frame?) is checked by
``validators.validate_narrative_fact_references`` — a free function, not a
pydantic validator on this model, because a narrative does not embed its
frame (the frame is the larger, independently persisted object; embedding
it here would duplicate it on the wire).
"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import AwareDatetime, Field, model_validator

from dev_health_ops.api.dev.contracts import DevModelMetadata

from .base import ContractModelV2, LongText, OpaqueID, ShortText

__all__ = ["DevNarrative"]


class DevNarrative(ContractModelV2):
    schema_version: Literal["dev_narrative.v1"]
    narrative_id: OpaqueID
    run_id: OpaqueID
    frame_id: OpaqueID
    mode: Literal["provider", "deterministic_fallback"]
    body: LongText
    referenced_fact_ids: list[OpaqueID] = Field(default_factory=list, max_length=200)
    referenced_section_ids: list[OpaqueID] = Field(default_factory=list, max_length=20)
    provider_metadata: DevModelMetadata | None = None
    generated_at: AwareDatetime
    validation_warnings: list[ShortText] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def validate_mode_payload(self) -> Self:
        if self.mode == "provider" and self.provider_metadata is None:
            raise ValueError("a provider narrative requires provider_metadata")
        if self.mode == "deterministic_fallback" and self.provider_metadata is not None:
            raise ValueError(
                "a deterministic_fallback narrative cannot carry provider_metadata"
            )
        return self
