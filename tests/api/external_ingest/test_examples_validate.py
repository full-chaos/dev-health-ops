"""Every canonical example validates against its record-kind model (CHAOS-2692).

Directly satisfies the acceptance criterion "Examples pass validation" and
automatically covers any record kind added later, since it's parametrized
over the registry's ``iter_record_kinds()`` rather than a hardcoded list.
"""

from __future__ import annotations

import pytest

from dev_health_ops.api.external_ingest import schema_registry as registry

_KIND_MODEL_PAIRS = registry.iter_record_kinds()


@pytest.mark.parametrize(
    "kind,model", _KIND_MODEL_PAIRS, ids=[kind for kind, _ in _KIND_MODEL_PAIRS]
)
def test_example_validates_against_its_model(kind, model):
    example = registry.load_example(kind)

    model.model_validate(example)  # raises on failure


def test_every_record_kind_has_an_example():
    for kind, _model in _KIND_MODEL_PAIRS:
        assert registry.load_example(kind)
