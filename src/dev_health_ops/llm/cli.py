import argparse
import os


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
