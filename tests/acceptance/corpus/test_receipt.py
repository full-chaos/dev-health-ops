"""Unit coverage for ``scripts.acceptance.corpus.receipt``."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from scripts.acceptance.corpus.receipt import (
    DECLARED_BLOCKED_RECEIPT_STATUS,
    RECEIPT_SCHEMA_VERSION,
    ReceiptValidationError,
    SessionSummaryError,
    Wave4CaseRecorder,
    write_declared_blocked_receipt,
    write_session_summary,
)


def _recorder(**overrides: object) -> Wave4CaseRecorder:
    defaults: dict[str, object] = dict(
        case_id="runner-selftest.basic-exact",
        question="What's the status of the Ask Dev project?",
        subject_class="exact",
        resolution_profile_ref=None,
    )
    defaults.update(overrides)
    return Wave4CaseRecorder(**defaults)  # type: ignore[arg-type]


class TestWave4CaseRecorderWrite:
    def test_zero_assertions_raises(self, tmp_path: Path) -> None:
        recorder = _recorder()
        recorder.set_resolution_path("deterministic-exact")
        recorder.set_world_digest("digest-abc")
        with pytest.raises(ReceiptValidationError, match="zero assertions"):
            recorder.write(tmp_path / "receipt.json")

    def test_unset_resolution_path_raises(self, tmp_path: Path) -> None:
        recorder = _recorder()
        recorder.check(category="c", name="n", condition=True, detail="d")
        recorder.set_world_digest("digest-abc")
        with pytest.raises(ReceiptValidationError, match="resolution_path"):
            recorder.write(tmp_path / "receipt.json")

    def test_unset_world_digest_raises(self, tmp_path: Path) -> None:
        recorder = _recorder()
        recorder.check(category="c", name="n", condition=True, detail="d")
        recorder.set_resolution_path("deterministic-exact")
        with pytest.raises(ReceiptValidationError, match="world_digest"):
            recorder.write(tmp_path / "receipt.json")

    def test_invalid_resolution_path_value_raises(self) -> None:
        recorder = _recorder()
        with pytest.raises(ReceiptValidationError, match="not one of"):
            recorder.set_resolution_path("something-invented")

    def test_explicit_none_resolution_path_is_allowed(self, tmp_path: Path) -> None:
        recorder = _recorder(subject_class="n/a")
        recorder.check(category="c", name="n", condition=True, detail="d")
        recorder.set_resolution_path(None)
        recorder.set_world_digest("digest-abc")
        artifact = recorder.write(tmp_path / "receipt.json")
        assert artifact["resolution_path"] is None

    def test_writes_the_expected_schema_shape(self, tmp_path: Path) -> None:
        recorder = _recorder()
        recorder.check(
            category="subject-resolution",
            name="exact_match",
            condition=True,
            detail="ok",
        )
        recorder.check(
            category="public-outcome", name="answered", condition=True, detail="ok"
        )
        recorder.set_resolution_path("deterministic-exact")
        recorder.set_world_digest("digest-abc")
        path = tmp_path / "receipt.json"
        artifact = recorder.write(path)

        assert artifact["schema_version"] == RECEIPT_SCHEMA_VERSION
        assert artifact["case_id"] == "runner-selftest.basic-exact"
        assert artifact["resolution_path"] == "deterministic-exact"
        assert artifact["world_digest"] == "digest-abc"
        assert artifact["assertion_count"] == 2
        assert artifact["status"] == "passed"
        assert len(artifact["assertions"]) == 2

        on_disk = json.loads(path.read_text())
        assert on_disk == artifact

    def test_any_failed_assertion_makes_status_failed(self, tmp_path: Path) -> None:
        recorder = _recorder()
        recorder.check(category="c", name="ok", condition=True, detail="fine")
        recorder.check(category="c", name="bad", condition=False, detail="broke")
        recorder.set_resolution_path("deterministic-exact")
        recorder.set_world_digest("digest-abc")
        artifact = recorder.write(tmp_path / "receipt.json")
        assert artifact["status"] == "failed"
        assert artifact["assertion_count"] == 2

    def test_check_does_not_raise_on_a_failed_condition(self) -> None:
        """Unlike ScenarioRecorder.check -- a corpus case's remaining
        invariants still matter for diagnosis after one fails."""

        recorder = _recorder()
        recorder.check(category="c", name="bad", condition=False, detail="broke")
        assert len(recorder.assertions) == 1
        assert recorder.assertions[0].passed is False

    def test_secrets_are_redacted_in_assertion_detail(self, tmp_path: Path) -> None:
        # Runtime-constructed so no commit ever contains a JWT-shaped literal
        # (gitleaks scans the full PR commit range).
        header = base64.urlsafe_b64encode(b'{"alg":"HS256"}').rstrip(b"=").decode()
        payload = (
            base64.urlsafe_b64encode(b'{"sub":"1234567890"}').rstrip(b"=").decode()
        )
        jwt = f"{header}.{payload}.abcdefghij1234567890"
        recorder = _recorder()
        recorder.check(category="c", name="n", condition=True, detail=f"token={jwt}")
        recorder.set_resolution_path("deterministic-exact")
        recorder.set_world_digest("digest-abc")
        artifact = recorder.write(tmp_path / "receipt.json")
        assert jwt not in artifact["assertions"][0]["detail"]
        assert "REDACTED_JWT" in artifact["assertions"][0]["detail"]

    def test_extra_fields_are_included(self, tmp_path: Path) -> None:
        recorder = _recorder()
        recorder.check(category="c", name="n", condition=True, detail="d")
        recorder.set_resolution_path("deterministic-exact")
        recorder.set_world_digest("digest-abc")
        recorder.set_extra("provider_script_id_used", "runner-selftest.basic-exact")
        artifact = recorder.write(tmp_path / "receipt.json")
        assert artifact["provider_script_id_used"] == "runner-selftest.basic-exact"

    @pytest.mark.parametrize(
        "key",
        [
            "schema_version",
            "case_id",
            "question",
            "subject_class",
            "resolution_profile_ref",
            "resolution_path",
            "world_digest",
            "started_at",
            "finished_at",
            "status",
            "assertion_count",
            "assertions",
        ],
    )
    def test_set_extra_rejects_every_reserved_field(self, key: str) -> None:
        """Codex round-1, HIGH, confirmed: set_extra had no reserved-key
        check, so `set_extra("status", "passed")` (or resolution_path/
        world_digest/assertion_count/...) silently overwrote a guarded,
        already-computed field -- a caller could record a failed assertion
        then paper over it with one extra-field call."""

        recorder = _recorder()
        with pytest.raises(ReceiptValidationError, match="reserved"):
            recorder.set_extra(key, "attempted-override")

    def test_a_forged_status_override_cannot_survive_write(
        self, tmp_path: Path
    ) -> None:
        """Defense-in-depth proof: even bypassing the public set_extra guard
        by writing directly into the recorder's internal _extra dict cannot
        flip a failed receipt to "passed" -- write() spreads _extra FIRST,
        so the real computed status always wins."""

        recorder = _recorder()
        recorder.check(category="c", name="bad", condition=False, detail="broke")
        recorder.set_resolution_path("deterministic-exact")
        recorder.set_world_digest("digest-abc")
        recorder._extra["status"] = "passed"  # bypassing the public API on purpose
        artifact = recorder.write(tmp_path / "receipt.json")
        assert artifact["status"] == "failed"


class TestWriteDeclaredBlockedReceipt:
    def test_writes_the_expected_shape(self, tmp_path: Path) -> None:
        path = tmp_path / "receipt.json"
        artifact = write_declared_blocked_receipt(
            case_id="portfolio.multi-project.status",
            question="How are the meridian and atlas projects doing together?",
            subject_class="bounded-set",
            resolution_profile_ref=None,
            blocked_by="CHAOS-3393",
            path=path,
        )
        assert artifact["schema_version"] == RECEIPT_SCHEMA_VERSION
        assert artifact["status"] == DECLARED_BLOCKED_RECEIPT_STATUS
        assert artifact["blocked_by"] == "CHAOS-3393"
        assert artifact["resolution_path"] is None
        assert artifact["world_digest"] is None
        assert artifact["assertion_count"] == 0
        assert artifact["assertions"] == []

        on_disk = json.loads(path.read_text())
        assert on_disk == artifact

    def test_empty_blocked_by_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ReceiptValidationError, match="blocked_by"):
            write_declared_blocked_receipt(
                case_id="x",
                question="q",
                subject_class="exact",
                resolution_profile_ref=None,
                blocked_by="",
                path=tmp_path / "receipt.json",
            )

    def test_malformed_ticket_reference_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ReceiptValidationError, match="ticket reference"):
            write_declared_blocked_receipt(
                case_id="x",
                question="q",
                subject_class="exact",
                resolution_profile_ref=None,
                blocked_by="not-a-ticket",
                path=tmp_path / "receipt.json",
            )


class TestWriteSessionSummary:
    def _receipt(self, case_id: str, *, status: str = "passed") -> dict:
        return {"case_id": case_id, "status": status}

    def test_zero_receipts_raises(self, tmp_path: Path) -> None:
        with pytest.raises(SessionSummaryError, match="zero case receipts"):
            write_session_summary(
                [], tmp_path / "summary.json", expected_case_ids=frozenset()
            )

    def test_reports_case_and_pass_counts(self, tmp_path: Path) -> None:
        receipts = [self._receipt("a"), self._receipt("b", status="failed")]
        summary = write_session_summary(
            receipts, tmp_path / "summary.json", expected_case_ids=frozenset({"a", "b"})
        )
        assert summary["case_count"] == 2
        assert summary["passed_count"] == 1
        assert summary["failed_count"] == 1
        assert summary["missing_case_ids"] == []
        assert summary["unexpected_case_ids"] == []

    def test_declared_blocked_receipts_counted_separately_from_pass_and_fail(
        self, tmp_path: Path
    ) -> None:
        receipts = [
            self._receipt("a", status="passed"),
            self._receipt("b", status="failed"),
            self._receipt("c", status=DECLARED_BLOCKED_RECEIPT_STATUS),
        ]
        summary = write_session_summary(
            receipts,
            tmp_path / "summary.json",
            expected_case_ids=frozenset({"a", "b", "c"}),
        )
        assert summary["case_count"] == 3
        assert summary["passed_count"] == 1
        assert summary["failed_count"] == 1
        assert summary["declared_blocked_count"] == 1
        assert summary["declared_blocked_case_ids"] == ["c"]
        # A declared-blocked case IS received, not missing.
        assert summary["missing_case_ids"] == []

    def test_reports_missing_expected_cases(self, tmp_path: Path) -> None:
        receipts = [self._receipt("a")]
        summary = write_session_summary(
            receipts,
            tmp_path / "summary.json",
            expected_case_ids=frozenset({"a", "b", "c"}),
        )
        assert summary["missing_case_ids"] == ["b", "c"]

    def test_reports_unexpected_received_cases(self, tmp_path: Path) -> None:
        receipts = [self._receipt("a"), self._receipt("runner-selftest.extra")]
        summary = write_session_summary(
            receipts, tmp_path / "summary.json", expected_case_ids=frozenset({"a"})
        )
        assert summary["unexpected_case_ids"] == ["runner-selftest.extra"]

    def test_writes_to_disk(self, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "summary.json"
        write_session_summary(
            [self._receipt("a")], path, expected_case_ids=frozenset({"a"})
        )
        assert json.loads(path.read_text())["case_count"] == 1
