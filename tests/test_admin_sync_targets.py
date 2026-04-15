from dev_health_ops.api.admin.routers.sync import PROVIDER_SYNC_TARGETS


def test_provider_sync_targets_include_feature_flag_sources():
    assert "feature-flags" in PROVIDER_SYNC_TARGETS["gitlab"]
    assert PROVIDER_SYNC_TARGETS["launchdarkly"] == ["feature-flags"]
