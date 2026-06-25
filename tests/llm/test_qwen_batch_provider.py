from __future__ import annotations

from dev_health_ops.llm.providers.batch import BatchProviderFeature
from dev_health_ops.llm.providers.qwen import QwenProvider


def test_qwen_batch_capability_uses_dashscope_config_without_openai_credentials(
    monkeypatch,
):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    provider = QwenProvider(
        api_key="dashscope-key",
        base_url="https://dashscope.example/v1",
        model="qwen-plus",
    )

    capability = provider.batch_capability()

    assert capability.supported is True
    assert capability.provider == "qwen"
    assert capability.model == "qwen-plus"
    assert BatchProviderFeature.SUBMIT in capability.features
