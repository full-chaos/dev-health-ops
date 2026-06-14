from __future__ import annotations

from types import SimpleNamespace

from dev_health_ops.providers.gitlab.normalize import detect_mr_attributions


def _mr(
    *,
    labels: list[str] | None = None,
    author_username: str = "human-author",
    author_bot: bool = False,
    description: str = "normal MR",
    source_branch: str = "feature/human-work",
) -> SimpleNamespace:
    """Build a GitLab merge-request-like object for detection tests."""
    return SimpleNamespace(
        iid=7,
        labels=list(labels or []),
        author=SimpleNamespace(
            username=author_username,
            name=author_username,
            bot=author_bot,
        ),
        description=description,
        source_branch=source_branch,
    )


def test_detect_mr_attributions_from_ai_label() -> None:
    signals = detect_mr_attributions(mr=_mr(labels=["ai-assisted"]))

    assert any(s.source.value == "pr_label" for s in signals)
    label_signal = next(s for s in signals if s.source.value == "pr_label")
    assert label_signal.kind.value == "ai_assisted"
    assert label_signal.evidence["label"] == "ai-assisted"


def test_detect_mr_attributions_from_commit_trailer() -> None:
    signals = detect_mr_attributions(
        mr=_mr(description="Implement feature\n\nAI-Assisted-By: Claude Code"),
    )

    assert any(s.source.value == "commit_trailer" for s in signals)
    trailer_signal = next(s for s in signals if s.source.value == "commit_trailer")
    assert trailer_signal.actor == "Claude Code"


def test_detect_mr_attributions_from_bot_author() -> None:
    signals = detect_mr_attributions(
        mr=_mr(author_username="claude-code[bot]", author_bot=True),
    )

    assert any(s.source.value == "bot_author" for s in signals)
    bot_signal = next(s for s in signals if s.source.value == "bot_author")
    assert bot_signal.kind.value == "agent_created"
    assert bot_signal.actor == "claude-code[bot]"


def test_detect_mr_attributions_from_source_branch() -> None:
    signals = detect_mr_attributions(
        mr=_mr(source_branch="copilot/fix-bug"),
    )

    assert any(s.source.value == "branch_name" for s in signals)


def test_detect_mr_attributions_non_ai_mr_emits_none() -> None:
    signals = detect_mr_attributions(
        mr=_mr(
            labels=["bug", "frontend"],
            author_username="alice",
            author_bot=False,
            description="Refactor the widget rendering pipeline.",
            source_branch="feature/widget-refactor",
        ),
    )

    assert signals == []


def test_detect_mr_attributions_ci_bot_author_emits_none() -> None:
    # CI automation bots are explicitly excluded — they are not AI.
    signals = detect_mr_attributions(
        mr=_mr(author_username="dependabot[bot]", author_bot=True),
    )

    assert not any(s.source.value == "bot_author" for s in signals)
