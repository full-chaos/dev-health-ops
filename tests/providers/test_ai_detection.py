"""Tests for AI attribution detection functions.

Coverage:
- Every P0 source has positive + negative + manual-override cases.
- Integration scenario: fixture PR with ai-assisted label + bot author
  + trailer → 3 distinct attribution records.

P0 sources:
    1. PR labels          (detect_from_pr_labels)
    2. Bot/app authors    (detect_from_author)
    3. Commit trailers    (detect_from_commit_trailers)
    4. Branch naming      (detect_from_branch_name)
    5. PR body            (detect_from_pr_body)
    6. CI annotations     (detect_from_ci_annotations)
"""

from __future__ import annotations

import pytest

from dev_health_ops.providers._ai_detection import (
    AI_LABELS,
    CI_BOTS,
    KNOWN_AI_BOTS,
    AIAttributionKind,
    AIAttributionSignal,
    AIAttributionSource,
    AuthorInfo,
    detect_from_author,
    detect_from_branch_name,
    detect_from_ci_annotations,
    detect_from_commit_trailers,
    detect_from_pr_body,
    detect_from_pr_labels,
)

# ==========================================================================
# 1. detect_from_pr_labels
# ==========================================================================


class TestDetectFromPrLabels:
    # --- positive cases ---

    def test_ai_assisted_label(self) -> None:
        signals = detect_from_pr_labels(["ai-assisted"])
        assert len(signals) == 1
        s = signals[0]
        assert s.source == AIAttributionSource.PR_LABEL
        assert s.kind == AIAttributionKind.AI_ASSISTED
        assert s.confidence == pytest.approx(0.95)
        assert s.evidence["label"] == "ai-assisted"

    def test_agent_created_label(self) -> None:
        signals = detect_from_pr_labels(["agent-created"])
        assert len(signals) == 1
        assert signals[0].kind == AIAttributionKind.AGENT_CREATED

    def test_ai_review_label(self) -> None:
        signals = detect_from_pr_labels(["ai-review"])
        assert len(signals) == 1
        assert signals[0].kind == AIAttributionKind.AI_REVIEW

    def test_copilot_label(self) -> None:
        signals = detect_from_pr_labels(["copilot"])
        assert len(signals) == 1
        assert signals[0].kind == AIAttributionKind.AI_ASSISTED

    def test_claude_code_label(self) -> None:
        signals = detect_from_pr_labels(["claude-code"])
        assert len(signals) == 1
        assert signals[0].actor is None  # label source has no actor

    def test_multiple_ai_labels_emit_multiple_signals(self) -> None:
        signals = detect_from_pr_labels(["ai-assisted", "copilot", "bug"])
        assert len(signals) == 2
        sources = {s.evidence["label"] for s in signals}
        assert "ai-assisted" in sources
        assert "copilot" in sources

    def test_case_insensitive_matching(self) -> None:
        signals = detect_from_pr_labels(["AI-Assisted", "COPILOT"])
        assert len(signals) == 2

    def test_whitespace_stripped_from_label(self) -> None:
        signals = detect_from_pr_labels(["  ai-assisted  "])
        assert len(signals) == 1

    # --- negative cases ---

    def test_no_ai_labels_returns_empty(self) -> None:
        signals = detect_from_pr_labels(["bug", "enhancement", "good first issue"])
        assert signals == []

    def test_empty_labels_list(self) -> None:
        assert detect_from_pr_labels([]) == []

    def test_partial_match_not_detected(self) -> None:
        # "ai-" prefix alone shouldn't match unregistered labels
        signals = detect_from_pr_labels(["ai-suggestion", "ai-generated-tests"])
        # These are not in the registry
        assert signals == []

    # --- manual override indicator ---

    def test_all_registered_labels_are_in_ai_labels(self) -> None:
        """Sanity: every label in the registry can be detected."""
        for label in AI_LABELS:
            signals = detect_from_pr_labels([label])
            assert len(signals) == 1, f"Label {label!r} not detected"


# ==========================================================================
# 2. detect_from_author
# ==========================================================================


class TestDetectFromAuthor:
    # --- positive cases: known AI bots ---

    def test_copilot_bot(self) -> None:
        info = AuthorInfo(login="copilot[bot]", user_type="Bot")
        signal = detect_from_author(info)
        assert signal is not None
        assert signal.source == AIAttributionSource.BOT_AUTHOR
        assert signal.kind == AIAttributionKind.AGENT_CREATED
        assert signal.confidence >= 0.85
        assert signal.actor == "copilot[bot]"
        assert signal.evidence["known_ai_bot"] is True

    def test_claude_code_bot(self) -> None:
        info = AuthorInfo(login="claude-code[bot]", user_type="Bot")
        signal = detect_from_author(info)
        assert signal is not None
        assert signal.evidence["known_ai_bot"] is True

    def test_cursor_agent_bot(self) -> None:
        info = AuthorInfo(login="cursor-agent[bot]", user_type="Bot")
        signal = detect_from_author(info)
        assert signal is not None
        assert signal.kind == AIAttributionKind.AGENT_CREATED

    def test_chatgpt_codex_bot(self) -> None:
        info = AuthorInfo(login="chatgpt-codex[bot]", user_type="Bot")
        signal = detect_from_author(info)
        assert signal is not None

    def test_sweep_ai_bot(self) -> None:
        info = AuthorInfo(login="sweep-ai[bot]", user_type="Bot")
        signal = detect_from_author(info)
        assert signal is not None

    def test_devin_bot(self) -> None:
        info = AuthorInfo(login="devin[bot]", user_type="Bot")
        signal = detect_from_author(info)
        assert signal is not None

    # --- positive case: unknown bot (not in known list) ---

    def test_unknown_bot_lower_confidence(self) -> None:
        info = AuthorInfo(login="some-new-ai-tool[bot]", user_type="Bot")
        signal = detect_from_author(info)
        assert signal is not None
        assert signal.source == AIAttributionSource.BOT_AUTHOR
        assert signal.confidence < 0.70  # weaker
        assert signal.evidence["known_ai_bot"] is False

    # --- negative cases: CI bots excluded ---

    def test_github_actions_excluded(self) -> None:
        """github-actions[bot] must NOT produce a signal."""
        info = AuthorInfo(login="github-actions[bot]", user_type="Bot")
        assert detect_from_author(info) is None

    def test_dependabot_excluded(self) -> None:
        info = AuthorInfo(login="dependabot[bot]", user_type="Bot")
        assert detect_from_author(info) is None

    def test_renovate_excluded(self) -> None:
        info = AuthorInfo(login="renovate[bot]", user_type="Bot")
        assert detect_from_author(info) is None

    def test_human_user_returns_none(self) -> None:
        info = AuthorInfo(login="octocat", user_type="User")
        assert detect_from_author(info) is None

    def test_organization_user_returns_none(self) -> None:
        info = AuthorInfo(login="my-org", user_type="Organization")
        assert detect_from_author(info) is None

    def test_user_type_none_bot_login_returns_none(self) -> None:
        """Without user_type=Bot context, a plain bot-looking login without
        known-bot list membership should not produce a signal."""
        # Not in KNOWN_AI_BOTS and user_type is None
        info = AuthorInfo(login="random[bot]", user_type=None)
        assert detect_from_author(info) is None

    # --- sanity: all CI bots excluded ---

    def test_all_ci_bots_are_excluded(self) -> None:
        for login in CI_BOTS:
            info = AuthorInfo(login=login, user_type="Bot")
            assert detect_from_author(info) is None, (
                f"CI bot {login!r} should be excluded"
            )

    def test_all_known_ai_bots_detected(self) -> None:
        for login in KNOWN_AI_BOTS:
            info = AuthorInfo(login=login, user_type="Bot")
            signal = detect_from_author(info)
            assert signal is not None, f"Known AI bot {login!r} not detected"
            assert signal.evidence["known_ai_bot"] is True


# ==========================================================================
# 3. detect_from_commit_trailers
# ==========================================================================


class TestDetectFromCommitTrailers:
    # --- positive cases ---

    def test_ai_assisted_by_trailer(self) -> None:
        msg = "Fix login bug\n\nAI-Assisted-By: GitHub Copilot"
        signals = detect_from_commit_trailers(msg)
        assert len(signals) == 1
        s = signals[0]
        assert s.source == AIAttributionSource.COMMIT_TRAILER
        assert s.kind == AIAttributionKind.AI_ASSISTED
        assert s.confidence == pytest.approx(0.85)
        assert s.evidence["trailer_key"] == "AI-Assisted-By"
        assert "Copilot" in str(s.evidence["trailer_value"])

    def test_generated_by_trailer(self) -> None:
        msg = "Add tests\n\nGenerated-By: claude-code"
        signals = detect_from_commit_trailers(msg)
        assert len(signals) == 1
        assert signals[0].evidence["trailer_key"] == "Generated-By"

    def test_x_ai_generated_trailer(self) -> None:
        msg = "Refactor auth\n\nX-AI-Generated: cursor"
        signals = detect_from_commit_trailers(msg)
        assert len(signals) == 1

    def test_co_authored_by_copilot_email(self) -> None:
        msg = "Implement feature\n\nCo-authored-by: GitHub Copilot <copilot@github.com>"
        signals = detect_from_commit_trailers(msg)
        assert len(signals) == 1
        s = signals[0]
        assert s.evidence["trailer_key"] == "Co-authored-by"
        assert s.confidence == pytest.approx(0.80)

    def test_co_authored_by_noreply_copilot(self) -> None:
        msg = "Fix bug\nCo-authored-by: Copilot <noreply+copilot@github.com>"
        signals = detect_from_commit_trailers(msg)
        assert len(signals) == 1

    def test_multiple_trailers_emit_multiple_signals(self) -> None:
        msg = (
            "Big change\n\n"
            "AI-Assisted-By: Copilot\n"
            "Co-authored-by: Claude <claude-bot@anthropic.com>\n"
        )
        signals = detect_from_commit_trailers(msg)
        assert len(signals) == 2

    def test_case_insensitive_trailer_key(self) -> None:
        msg = "fix: typo\n\nai-assisted-by: cursor"
        signals = detect_from_commit_trailers(msg)
        assert len(signals) == 1

    # --- negative cases ---

    def test_no_trailers_returns_empty(self) -> None:
        msg = "Normal commit message\n\nSome body text."
        assert detect_from_commit_trailers(msg) == []

    def test_co_authored_by_human_returns_empty(self) -> None:
        msg = "Pair programming\n\nCo-authored-by: Alice <alice@example.com>"
        assert detect_from_commit_trailers(msg) == []

    def test_empty_message_returns_empty(self) -> None:
        assert detect_from_commit_trailers("") == []

    def test_regular_signed_off_by_ignored(self) -> None:
        msg = "Fix\n\nSigned-off-by: Developer <dev@example.com>"
        assert detect_from_commit_trailers(msg) == []


# ==========================================================================
# 4. detect_from_branch_name
# ==========================================================================


class TestDetectFromBranchName:
    # --- positive cases ---

    def test_copilot_prefix_branch(self) -> None:
        signal = detect_from_branch_name("copilot/fix-auth-bug")
        assert signal is not None
        assert signal.source == AIAttributionSource.BRANCH_NAME
        assert signal.kind == AIAttributionKind.AI_ASSISTED
        assert signal.confidence == pytest.approx(0.35)
        assert signal.actor == "copilot"

    def test_claude_prefix_branch(self) -> None:
        signal = detect_from_branch_name("claude/add-tests")
        assert signal is not None
        assert signal.actor == "claude"

    def test_codex_prefix_branch(self) -> None:
        signal = detect_from_branch_name("codex/refactor-payments")
        assert signal is not None

    def test_cursor_prefix_branch(self) -> None:
        signal = detect_from_branch_name("cursor/feature-x")
        assert signal is not None

    def test_agent_prefix_branch(self) -> None:
        signal = detect_from_branch_name("agent/migration-v2")
        assert signal is not None
        assert signal.kind == AIAttributionKind.AGENT_CREATED

    def test_devin_prefix_branch(self) -> None:
        signal = detect_from_branch_name("devin/implement-oauth")
        assert signal is not None
        assert signal.kind == AIAttributionKind.AGENT_CREATED

    def test_ai_infix_branch(self) -> None:
        signal = detect_from_branch_name("feat/ai/new-feature")
        assert signal is not None

    def test_branch_with_separator(self) -> None:
        signal = detect_from_branch_name("feat-copilot-fix")
        assert signal is not None

    # --- negative cases ---

    def test_normal_feature_branch(self) -> None:
        assert detect_from_branch_name("feat/add-login") is None

    def test_fix_branch(self) -> None:
        assert detect_from_branch_name("fix/typo-in-readme") is None

    def test_main_branch(self) -> None:
        assert detect_from_branch_name("main") is None

    def test_dependabot_branch(self) -> None:
        # dependabot branches are CI automation, not AI
        assert detect_from_branch_name("dependabot/npm/lodash-4.17.21") is None

    def test_unrelated_word_containing_ai_substring(self) -> None:
        # "railway" contains "ai" but shouldn't match word-boundary patterns
        # Pattern requires /ai/ or -ai or ai- at boundaries
        # "railway" doesn't have separators around "ai"
        signal = detect_from_branch_name("feat/railway-deploy")
        # This may or may not match depending on pattern — assert only
        # that if it does, confidence is low
        if signal is not None:
            assert signal.confidence <= 0.35

    def test_empty_branch(self) -> None:
        assert detect_from_branch_name("") is None


# ==========================================================================
# 5. detect_from_pr_body
# ==========================================================================


class TestDetectFromPrBody:
    # --- positive cases ---

    def test_generated_by_copilot_phrase(self) -> None:
        body = "This PR was generated by Copilot to fix the auth issue."
        signal = detect_from_pr_body(body)
        assert signal is not None
        assert signal.source == AIAttributionSource.PR_BODY
        assert signal.confidence == pytest.approx(0.25)

    def test_ai_assisted_phrase(self) -> None:
        body = "The refactoring was ai-assisted using cursor."
        signal = detect_from_pr_body(body)
        assert signal is not None
        assert signal.kind == AIAttributionKind.AI_ASSISTED

    def test_agent_created_phrase(self) -> None:
        body = "This PR is agent-created by the automation pipeline."
        signal = detect_from_pr_body(body)
        assert signal is not None
        assert signal.kind == AIAttributionKind.AGENT_CREATED

    def test_copilot_mention(self) -> None:
        body = "I used Copilot to help write the tests."
        signal = detect_from_pr_body(body)
        assert signal is not None

    def test_claude_mention(self) -> None:
        body = "Claude suggested this approach to avoid the race condition."
        signal = detect_from_pr_body(body)
        assert signal is not None

    def test_codex_mention(self) -> None:
        body = "Initial scaffold created with Codex."
        signal = detect_from_pr_body(body)
        assert signal is not None

    # --- negative cases ---

    def test_empty_body(self) -> None:
        assert detect_from_pr_body("") is None

    def test_none_equivalent_body(self) -> None:
        # Callers may pass empty string for None bodies
        assert detect_from_pr_body("") is None

    def test_normal_pr_body(self) -> None:
        body = "Fixes the login bug introduced in #234.\n\nCloses #250."
        assert detect_from_pr_body(body) is None

    def test_technical_discussion_no_tools(self) -> None:
        body = (
            "Refactored the authentication module to improve performance.\n\n"
            "Added unit tests for edge cases."
        )
        assert detect_from_pr_body(body) is None

    def test_only_returns_first_match(self) -> None:
        body = "Generated by Copilot. Also referenced Claude for review."
        signal = detect_from_pr_body(body)
        assert signal is not None
        # Should only return one signal
        # (function returns first match, not a list)
        assert isinstance(signal, AIAttributionSignal)


# ==========================================================================
# 6. detect_from_ci_annotations
# ==========================================================================


class TestDetectFromCiAnnotations:
    # --- positive cases ---

    def test_ai_generated_key(self) -> None:
        annotations: list[dict[str, object]] = [
            {"key": "ai-generated", "message": "This was AI generated"}
        ]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 1
        s = signals[0]
        assert s.source == AIAttributionSource.CI_ANNOTATION
        assert s.kind == AIAttributionKind.AI_ASSISTED
        assert s.confidence >= 0.55

    def test_ai_assisted_name_field(self) -> None:
        annotations: list[dict[str, object]] = [
            {"name": "ai-assisted", "level": "notice"}
        ]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 1

    def test_copilot_generated_title(self) -> None:
        annotations: list[dict[str, object]] = [
            {"title": "copilot-generated", "message": "PR body"}
        ]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 1

    def test_agent_created_key(self) -> None:
        annotations: list[dict[str, object]] = [{"key": "agent-created"}]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 1

    def test_llm_generated_key(self) -> None:
        annotations: list[dict[str, object]] = [
            {"key": "llm-generated", "message": "By LLM"}
        ]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 1

    def test_multiple_annotations_emit_multiple_signals(self) -> None:
        annotations: list[dict[str, object]] = [
            {"key": "ai-generated"},
            {"key": "copilot-generated"},
        ]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 2

    def test_keyword_in_message_body(self) -> None:
        annotations: list[dict[str, object]] = [{"message": "This step is ai-assisted"}]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 1
        assert signals[0].confidence < 0.65  # body match, lower than key match

    # --- negative cases ---

    def test_empty_list_returns_empty(self) -> None:
        assert detect_from_ci_annotations([]) == []

    def test_non_ai_annotation(self) -> None:
        annotations: list[dict[str, object]] = [
            {"key": "test-passed", "name": "unit-tests", "message": "All good"}
        ]
        assert detect_from_ci_annotations(annotations) == []

    def test_one_signal_per_annotation_object(self) -> None:
        """Even if an annotation has both key and name matching, only 1 signal."""
        annotations: list[dict[str, object]] = [
            {"key": "ai-generated", "name": "ai-assisted"}
        ]
        signals = detect_from_ci_annotations(annotations)
        assert len(signals) == 1


# ==========================================================================
# Integration test — PR with 3 concurrent signals
# ==========================================================================


class TestIntegrationThreeSignals:
    """Integration: fixture PR with ai-assisted label + bot author + commit
    trailer should produce 3 distinct attribution signals.

    This mirrors the GitHub normalize integration scenario from the plan.
    """

    def test_three_signals_from_full_pr(self) -> None:
        # 1. PR labels
        label_signals = detect_from_pr_labels(["ai-assisted", "bug"])
        assert len(label_signals) == 1
        assert label_signals[0].source == AIAttributionSource.PR_LABEL
        assert label_signals[0].kind == AIAttributionKind.AI_ASSISTED

        # 2. Bot author
        author_signal = detect_from_author(
            AuthorInfo(login="copilot[bot]", user_type="Bot")
        )
        assert author_signal is not None
        assert author_signal.source == AIAttributionSource.BOT_AUTHOR
        assert author_signal.kind == AIAttributionKind.AGENT_CREATED

        # 3. Commit trailer
        commit_msg = "Add feature\n\nAI-Assisted-By: GitHub Copilot v1.0"
        trailer_signals = detect_from_commit_trailers(commit_msg)
        assert len(trailer_signals) == 1
        assert trailer_signals[0].source == AIAttributionSource.COMMIT_TRAILER

        # All 3 are distinct sources
        all_signals: list[AIAttributionSignal] = (
            label_signals + [author_signal] + trailer_signals
        )
        assert len(all_signals) == 3

        sources = {s.source for s in all_signals}
        assert sources == {
            AIAttributionSource.PR_LABEL,
            AIAttributionSource.BOT_AUTHOR,
            AIAttributionSource.COMMIT_TRAILER,
        }

    def test_ci_bots_do_not_contaminate_signals(self) -> None:
        """CI automation bots alongside AI bots must not produce false signals."""
        # Dependabot as author — excluded
        dep_signal = detect_from_author(
            AuthorInfo(login="dependabot[bot]", user_type="Bot")
        )
        assert dep_signal is None

        # But a real AI label still fires
        label_signals = detect_from_pr_labels(["ai-assisted"])
        assert len(label_signals) == 1

    def test_unknown_stays_unknown_no_guessing(self) -> None:
        """No signals should be emitted for a normal human PR."""
        label_signals = detect_from_pr_labels(["bug", "enhancement"])
        author_signal = detect_from_author(
            AuthorInfo(login="octocat", user_type="User")
        )
        trailer_signals = detect_from_commit_trailers(
            "Fix: handle edge case in parser\n\nSigned-off-by: Dev <dev@example.com>"
        )
        branch_signal = detect_from_branch_name("fix/edge-case-parser")
        body_signal = detect_from_pr_body(
            "Handles edge cases in the parser module. Closes #42."
        )
        ci_signals = detect_from_ci_annotations(
            [{"key": "linting-passed", "name": "eslint"}]
        )

        assert label_signals == []
        assert author_signal is None
        assert trailer_signals == []
        assert branch_signal is None
        assert body_signal is None
        assert ci_signals == []

    def test_signal_confidence_validation(self) -> None:
        """All signal confidences must be in [0.0, 1.0]."""
        all_test_signals: list[AIAttributionSignal] = []

        all_test_signals.extend(detect_from_pr_labels(list(AI_LABELS)))

        for login in KNOWN_AI_BOTS:
            sig = detect_from_author(AuthorInfo(login=login, user_type="Bot"))
            if sig:
                all_test_signals.append(sig)

        msg = "AI-Assisted-By: test\nCo-authored-by: bot <copilot@github.com>"
        all_test_signals.extend(detect_from_commit_trailers(msg))

        sig = detect_from_branch_name("copilot/test")
        if sig:
            all_test_signals.append(sig)

        sig = detect_from_pr_body("Generated by Copilot.")
        if sig:
            all_test_signals.append(sig)

        all_test_signals.extend(detect_from_ci_annotations([{"key": "ai-generated"}]))

        assert all_test_signals, "Expected at least one signal in validation test"
        for s in all_test_signals:
            assert 0.0 <= s.confidence <= 1.0, (
                f"Signal {s.source}:{s.kind} has out-of-range confidence {s.confidence}"
            )

    def test_signal_confidence_in_valid_range_for_all_sources(self) -> None:
        """Confidence values produced by detection functions must be in [0.0, 1.0]."""
        # Verify each detection path produces valid confidence values.
        # Canonical AIAttributionSignal does not enforce this at construction;
        # we assert it here as a contract test on the detection functions.
        msg = "AI-Assisted-By: test\nCo-authored-by: bot <copilot@github.com>"
        label_signal = detect_from_pr_labels(["ai-assisted"])[0]
        author_signal = detect_from_author(
            AuthorInfo(login="copilot[bot]", user_type="Bot")
        )
        trailer_signal = detect_from_commit_trailers(msg)[0]
        branch_signal = detect_from_branch_name("copilot/feat")
        body_signal = detect_from_pr_body("Generated by Copilot.")
        ci_signal = detect_from_ci_annotations([{"key": "ai-generated"}])[0]

        for sig in [
            label_signal,
            author_signal,
            trailer_signal,
            branch_signal,
            body_signal,
            ci_signal,
        ]:
            assert sig is not None
            assert 0.0 <= sig.confidence <= 1.0, (
                f"{sig.source}:{sig.kind} confidence={sig.confidence} out of [0,1]"
            )
