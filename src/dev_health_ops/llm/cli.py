import argparse
import os


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def add_llm_arguments(
    parser: argparse.ArgumentParser, *, leaf_mode: bool = False
) -> None:
    """
    Standardize LLM provider and model arguments across the CLI.

    Args:
        parser: The argparse parser or subparser to add arguments to.
        leaf_mode: Use ``default=SUPPRESS`` for subparsers that re-add these
            global options so root-position values are not clobbered.
    """
    provider_default = (
        argparse.SUPPRESS if leaf_mode else os.getenv("LLM_PROVIDER", "auto")
    )
    model_default = argparse.SUPPRESS if leaf_mode else os.getenv("LLM_MODEL")
    api_key_default = argparse.SUPPRESS if leaf_mode else os.getenv("LLM_API_KEY")
    base_url_default = argparse.SUPPRESS if leaf_mode else os.getenv("LLM_BASE_URL")
    concurrency_default = (
        argparse.SUPPRESS if leaf_mode else _env_int("INVESTMENT_LLM_CONCURRENCY", 5)
    )
    parser.add_argument(
        "-l",
        "--llm-provider",
        default=provider_default,
        help="LLM provider (auto, openai, anthropic, local, mock, none). "
        "Use 'none' to compute distributions without LLM explanations.",
    )
    parser.add_argument(
        "-m",
        "--model",
        default=model_default,
        help="LLM model name (overrides provider default)",
    )
    parser.add_argument(
        "--llm-api-key",
        default=api_key_default,
        help="LLM API key for this inline invocation. Env: LLM_API_KEY.",
    )
    parser.add_argument(
        "--llm-base-url",
        default=base_url_default,
        help="LLM provider base URL for this invocation. Env: LLM_BASE_URL.",
    )
    parser.add_argument(
        "--llm-concurrency",
        type=int,
        default=concurrency_default,
        help="Maximum concurrent LLM categorizations. Env: INVESTMENT_LLM_CONCURRENCY (default: 5).",
    )
