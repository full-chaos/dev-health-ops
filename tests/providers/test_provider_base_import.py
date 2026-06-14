from __future__ import annotations

import importlib


def test_github_testops_pipeline_imports_without_connectors_preload() -> None:
    module = importlib.import_module("dev_health_ops.providers.github.testops_pipeline")

    assert module.GitHubActionsAdapter.provider == "github_actions"


def test_connector_exceptions_keep_shared_identity() -> None:
    connector_exceptions = importlib.import_module(
        "dev_health_ops.connectors.exceptions"
    )
    shared_exceptions = importlib.import_module("dev_health_ops.exceptions")

    assert connector_exceptions.APIException is shared_exceptions.APIException
    assert (
        connector_exceptions.AuthenticationException
        is shared_exceptions.AuthenticationException
    )
