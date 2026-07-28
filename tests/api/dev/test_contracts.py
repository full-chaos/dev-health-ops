from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from dev_health_ops.api.dev.contract_fixtures import (
    negative_fixtures,
    positive_fixtures,
    stream_fixtures,
)
from dev_health_ops.api.dev.contracts import (
    CONTRACT_MODELS,
    DevAnswer,
    DevCapabilities,
    DevStreamEvent,
    validate_stream,
)
from dev_health_ops.api.dev.export_contracts import (
    check_artifacts,
    expected_artifacts,
)


@pytest.mark.parametrize("schema_version", CONTRACT_MODELS)
def test_positive_fixture_validates(schema_version: str) -> None:
    CONTRACT_MODELS[schema_version].model_validate(positive_fixtures()[schema_version])


@pytest.mark.parametrize(
    ("schema_version", "case", "payload"),
    [
        (schema_version, case, payload)
        for schema_version, cases in negative_fixtures().items()
        for case, payload in cases
    ],
)
def test_negative_fixture_is_rejected(
    schema_version: str,
    case: str,
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError, match=".+"):
        CONTRACT_MODELS[schema_version].model_validate(payload)


def test_answer_rejects_unknown_evidence_and_metric_ids() -> None:
    answer = positive_fixtures()["dev_answer.v1"]
    unknown_evidence = deepcopy(answer)
    unknown_evidence["claims"][0]["evidence_ref_ids"] = ["ev_unknown"]
    with pytest.raises(ValidationError, match="unknown evidence"):
        DevAnswer.model_validate(unknown_evidence)

    unknown_metric = deepcopy(answer)
    unknown_metric["claims"][0]["metric_ref_ids"] = ["metric_unknown"]
    with pytest.raises(ValidationError, match="unknown metric"):
        DevAnswer.model_validate(unknown_metric)


def test_stream_sequences_require_exactly_one_terminal_then_done() -> None:
    fixtures = stream_fixtures()
    validate_stream([DevStreamEvent.model_validate(item) for item in fixtures["valid"]])
    for name, payloads in fixtures.items():
        if name == "valid":
            continue
        with pytest.raises((ValidationError, ValueError)):
            validate_stream([DevStreamEvent.model_validate(item) for item in payloads])


def test_capability_gates_are_independent() -> None:
    values = positive_fixtures()["dev_capabilities.v1"]
    capabilities = DevCapabilities.model_validate(values)
    assert capabilities.ask_dev is False
    assert capabilities.byo_llm is True
    assert capabilities.agent_context_runtime is False


def test_checked_in_contract_artifacts_have_no_drift() -> None:
    check_artifacts(expected_artifacts())


def test_contract_schemas_are_provider_neutral_and_closed() -> None:
    artifacts = expected_artifacts()
    schemas = "\n".join(
        contents for path, contents in artifacts.items() if path.startswith("schemas/")
    )
    assert 'additionalProperties": false' in schemas
    for provider_specific in ("openai_api_key", "anthropic_api_key", "tool_choice"):
        assert provider_specific not in schemas
