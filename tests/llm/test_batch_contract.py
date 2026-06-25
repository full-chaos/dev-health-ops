from __future__ import annotations

import pytest

from dev_health_ops.llm.providers.batch import (
    BatchCapability,
    BatchItemRequest,
    BatchProviderFeature,
    batch_capability_for,
)


class _SupportingProvider:
    provider_name = "supporting"

    def batch_capability(self, model=None):
        return BatchCapability(
            provider="supporting",
            model=model or "model-a",
            supported=True,
            features=frozenset({BatchProviderFeature.SUBMIT}),
        )


class _PlainProvider:
    provider_name = "plain"


def test_batch_item_request_requires_stable_custom_id():
    with pytest.raises(ValueError, match="custom_id"):
        BatchItemRequest(custom_id="", prompt="prompt")

    with pytest.raises(ValueError, match="whitespace"):
        BatchItemRequest(custom_id="bad id", prompt="prompt")

    request = BatchItemRequest(custom_id=" item-1 ", prompt="prompt")
    assert request.custom_id == "item-1"


def test_batch_capability_is_explicit_for_supporting_provider():
    capability = batch_capability_for(_SupportingProvider(), "model-b")

    assert capability.supported is True
    assert capability.provider == "supporting"
    assert capability.model == "model-b"
    assert BatchProviderFeature.SUBMIT in capability.features


def test_batch_capability_defaults_to_unsupported():
    capability = batch_capability_for(_PlainProvider(), "model-c")

    assert capability.supported is False
    assert capability.provider == "plain"
    assert capability.model == "model-c"
