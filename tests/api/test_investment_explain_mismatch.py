import pytest
from unittest.mock import MagicMock, patch, AsyncMock
from dev_health_ops.api.services.investment_mix_explain import explain_investment_mix


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
        patch("dev_health_ops.api.services.investment_mix_explain.get_provider") as mock_get_provider,
        patch("dev_health_ops.api.services.investment_mix_explain.logger") as mock_logger,
    ):
        # Setup mocks
        mock_investment = MagicMock()
        mock_investment.theme_distribution = {}
        mock_investment.subcategory_distribution = {}
        mock_build.return_value = mock_investment

        mock_units.return_value = []

        mock_provider = MagicMock()
        mock_provider.complete = AsyncMock(
            return_value='{"summary": "test", "top_findings": [], "confidence": {"level": "low"}, "what_to_check_next": [], "anti_claims": []}'
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
        args, kwargs = mock_logger.warning.call_args
        assert "Theme/subcategory mismatch" in args[0]
        assert "maintenance" in args[1]
        assert "feature_delivery.customer" in args[2]
        assert "feature_delivery" in args[3]
