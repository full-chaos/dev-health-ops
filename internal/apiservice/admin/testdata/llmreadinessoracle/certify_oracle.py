"""CHAOS-6976 live-python-oracle producer.

Runs the REAL production readiness probe -- readiness.py's
AgentReadinessService.certify over openai_compatible.py's
OpenAICompatibleAgentProvider (build_completion_request/decide/
_normalize_response), unmodified -- against a caller-supplied base_url, and
prints the resulting AgentReadinessRecord's outcome/safe_error_code as JSON.

Deliberately bypasses resolve_byo_certification_provider /
_byo_candidate / validate_llm_base_url: those settings-resolution and
SSRF-gate functions are UNCHANGED by this port (this route reuses the same
Go helpers CHAOS-6252a's own venue-oracle test already covers) and SSRF-block
loopback/private base_urls by design, which a local oracle stub server
necessarily is. The wire probe itself -- untested by any existing oracle --
is the only new, high-risk surface this script targets.

Usage: certify_oracle.py <base_url> <model>
Prints: {"outcome": "ready"|"failed", "safe_error_code": str|null}
"""

import asyncio
import json
import sys


class _MemoryStore:
    def __init__(self):
        self.saved = None

    async def load(self):
        return None

    async def save(self, record):
        self.saved = record


async def main() -> None:
    base_url, model = sys.argv[1], sys.argv[2]

    from dev_health_ops.llm.agent.openai_compatible import (
        OpenAICompatibleAgentProvider,
    )
    from dev_health_ops.llm.agent.readiness import AgentReadinessService

    provider = OpenAICompatibleAgentProvider(
        api_key="oracle-test-key", model=model, base_url=base_url
    )
    service = AgentReadinessService(_MemoryStore())
    try:
        record = await service.certify(
            provider,
            provider_name="openai",
            model=model,
            fingerprint="oracle-fingerprint",
        )
    finally:
        await provider.aclose()

    print(
        json.dumps(
            {
                "outcome": record.outcome.value,
                "safe_error_code": record.safe_error_code,
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
