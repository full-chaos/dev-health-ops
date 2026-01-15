import argparse
import os


def add_llm_arguments(parser: argparse.ArgumentParser) -> None:
    """
    Standardize LLM provider and model arguments across the CLI.

    Args:
        parser: The argparse parser or subparser to add arguments to.
    """
    parser.add_argument(
        "-l",
        "--llm-provider",
        default=os.getenv("LLM_PROVIDER", "auto"),
        help="LLM provider (auto, openai, anthropic, local, mock, etc.)",
    )
    parser.add_argument(
        "-m",
        "--model",
        default=os.getenv("LLM_MODEL"),
        help="LLM model name (overrides provider default)",
    )
