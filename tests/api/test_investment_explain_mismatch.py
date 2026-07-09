from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dev_health_ops.api.services.investment_mix_explain import explain_investment_mix
from dev_health_ops.llm.providers.base import CompletionResult


@pytest.mark.asyncio
async def test_explain_investment_mix_mismatch_warning():
    """Test that theme/subcategory mismatch logs a warning and continues."""
    with (
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_investment_response"
        ) as mock_build,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_work_unit_investments"
        ) as mock_units,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.get_provider"
        ) as mock_get_provider,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.logger"
        ) as mock_logger,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.record_explanation_parse"
        ) as mock_record_parse,
    ):
        # Setup mocks
        mock_investment = MagicMock()
        mock_investment.theme_distribution = {}
        mock_investment.subcategory_distribution = {}
        mock_build.return_value = mock_investment

        mock_units.return_value = []

        mock_provider = MagicMock()
        mock_provider.complete = AsyncMock(
            return_value=CompletionResult(
                text='{"summary": "test", "top_findings": [], "confidence": {"level": "low"}, "what_to_check_next": [], "anti_claims": []}',
                input_tokens=1,
                output_tokens=1,
                model="mock",
            )
        )
        mock_get_provider.return_value = mock_provider

        class MockFilters:
            def model_dump(self, mode=None):
                return {"scope": {"level": "org"}}

            @property
            def why(self):
                return MagicMock(work_category=[])

        filters = MockFilters()

        # Call with mismatched theme and subcategory
        # theme="maintenance", subcategory="feature_delivery.customer"
        # should be fixed to theme="feature_delivery"
        await explain_investment_mix(
            db_url="clickhouse://localhost:9000/test",
            filters=filters,
            theme="maintenance",
            subcategory="feature_delivery.customer",
            llm_provider="mock",
        )

        # Check that warning was logged
        mock_logger.warning.assert_called()
        mismatch_call = next(
            call
            for call in mock_logger.warning.call_args_list
            if "Theme/subcategory mismatch" in call.args[0]
        )
        args = mismatch_call.args
        assert "Theme/subcategory mismatch" in args[0]
        assert "maintenance" in args[1]
        assert "feature_delivery.customer" in args[2]
        assert "feature_delivery" in args[3]
        assert mock_record_parse.call_args.kwargs["status"] == "invalid_llm_output"


@pytest.mark.asyncio
async def test_explanation_failure_telemetry_uses_org_scoped_model():
    captured: dict[str, object] = {}

    @contextmanager
    def capture_metrics(**kwargs):
        captured.update(kwargs)
        yield MagicMock()

    with (
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_investment_response"
        ) as mock_build,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.build_work_unit_investments"
        ) as mock_units,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.is_llm_available",
            return_value=True,
        ),
        patch(
            "dev_health_ops.api.services.investment_mix_explain.resolve_provider_name",
            return_value="openai",
        ),
        patch(
            "dev_health_ops.api.services.investment_mix_explain.resolve_model_name",
            return_value="org-byo-model",
        ) as mock_resolve_model,
        patch(
            "dev_health_ops.api.services.investment_mix_explain.llm_call_metrics",
            side_effect=capture_metrics,
        ),
        patch(
            "dev_health_ops.api.services.investment_mix_explain.get_provider"
        ) as mock_get_provider,
    ):
        mock_investment = MagicMock()
        mock_investment.theme_distribution = {}
        mock_investment.subcategory_distribution = {}
        mock_build.return_value = mock_investment
        mock_units.return_value = []
        mock_provider = MagicMock()
        mock_provider.complete = AsyncMock(side_effect=RuntimeError("provider failed"))
        mock_get_provider.return_value = mock_provider
        filters = MagicMock()
        filters.model_dump.return_value = {"scope": {"level": "org"}}
        filters.why.work_category = []

        with pytest.raises(RuntimeError, match="provider failed"):
            await explain_investment_mix(
                db_url="clickhouse://localhost:9000/test",
                filters=filters,
                org_id="org-a",
                llm_provider="openai",
                force_refresh=True,
            )

    mock_resolve_model.assert_called_once_with("openai", None, org_id="org-a")
    assert captured["model"] == "org-byo-model"
