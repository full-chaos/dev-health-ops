"""Golden generator for ResolveModelName (provider.go) -- CHAOS-4977 step 6.

Calls the REAL resolve_model_name for each case, in a FRESH subprocess per
case (module-level env reads plus any import-time caching make in-process
env mutation unreliable across cases) with a controlled, minimal
environment -- no ambient LLM_* vars from the host leak in.

Scope: only the provider kinds this Go port can construct (mock, none,
openai, local) -- org BYO resolution has no Go port anywhere in this repo
yet (see provider.go's package doc comment), so no case here exercises it.

Run from the repo root (needs `uv sync --extra dev` once):
    uv run python cmd/query-api/internal/investmentexplain/testdata/generate_resolve_model_name_golden.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

OUT_DIR = Path(__file__).parent

CASES: dict[str, dict[str, Any]] = {
    "mock_ignores_model_arg": {"provider": "mock", "model": "whatever", "env": {}},
    "none_returns_none": {"provider": "none", "model": "whatever", "env": {}},
    "explicit_model_wins_openai": {
        "provider": "openai",
        "model": "gpt-5-custom",
        "env": {},
    },
    "openai_no_env_falls_to_default": {
        "provider": "openai",
        "model": None,
        "env": {},
    },
    "openai_provider_specific_env": {
        "provider": "openai",
        "model": None,
        "env": {"LLM_MODEL_OPENAI": "gpt-5-from-env"},
    },
    "openai_generic_llm_model_env": {
        "provider": "openai",
        "model": None,
        "env": {"LLM_MODEL": "generic-model-from-env"},
    },
    "openai_provider_specific_beats_generic": {
        "provider": "openai",
        "model": None,
        "env": {"LLM_MODEL_OPENAI": "specific", "LLM_MODEL": "generic"},
    },
    "local_no_env_falls_to_default": {
        "provider": "local",
        "model": None,
        "env": {},
    },
    "local_provider_specific_env": {
        "provider": "local",
        "model": None,
        "env": {"LLM_MODEL_LOCAL": "llama-from-env"},
    },
}


def run_case(case: dict[str, Any]) -> Any:
    script = (
        "from dev_health_ops.llm.providers import resolve_model_name\n"
        "import json\n"
        f"result = resolve_model_name({case['provider']!r}, {case['model']!r})\n"
        "print(json.dumps(result))\n"
    )
    env = {"PATH": os.environ.get("PATH", "")}
    env.update(case["env"])
    proc = subprocess.run(
        [sys.executable, "-c", script],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout.strip())


def main() -> None:
    for case_name, case in CASES.items():
        result = run_case(case)
        golden = {
            "case": case_name,
            "input": {
                "provider": case["provider"],
                "model": case["model"],
                "env": case["env"],
            },
            "result": result,
        }
        out_path = OUT_DIR / f"resolve_model_name__{case_name}.json"
        out_path.write_text(
            json.dumps(golden, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
        )
        print(f"wrote {out_path}  result={result!r}")


if __name__ == "__main__":
    main()
