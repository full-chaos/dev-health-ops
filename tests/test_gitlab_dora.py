from dev_health_ops.connectors import GitLabConnector


def test_gitlab_connector_dora_method_is_retired() -> None:
    assert not hasattr(GitLabConnector, "get_dora_metrics")
