"""CHAOS-3620: the ledger's own verification.

``chaos_3620_dispositions.py`` is the artifact a decision-owner reads instead
of forty test files. That makes it the one place in this lane where a
comfortable inaccuracy would do the most damage: a requirement marked
``PROVEN`` on a test that no longer exists is worse than no entry at all,
because a reader who sees "proven" stops checking.

Everything below exists to make that impossible. The tests named by every
entry must resolve. Every non-proven status must state a reason and, where
someone else owns the fix, name a Linear blocker. The ledger must be total
over the issue's own bullets and must not invent any. And the two entries
that carry the lane's hardest news — the conflict requirement blocked on
CHAOS-3612, and the zero-leakage gate blocked on CHAOS-3627 — are asserted by
id, so neither can be quietly upgraded to a pass.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

from tests.context_fabric.chaos_3620_dispositions import (
    _BLOCKER_PATTERN,
    INHERITED_INVARIANTS,
    ISSUE_BULLETS,
    NEEDS_BLOCKER,
    NEEDS_REASON,
    REQUIREMENTS,
    Status,
    Transfer,
    render,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve(node_id: str) -> tuple[bool, str]:
    """Whether ``path::Class::test`` names something that exists.

    By import and attribute walk rather than a pytest collection
    subprocess: the same answer, and it cannot be confused by an unrelated
    collection error elsewhere in the suite.
    """

    path, _, selector = node_id.partition("::")
    module_path = _REPO_ROOT / path
    if not module_path.is_file():
        return False, f"no such file: {path}"
    spec = importlib.util.spec_from_file_location(
        f"_ledger_target_{module_path.stem}", module_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    target: object = module
    for part in selector.split("::"):
        if not hasattr(target, part):
            return False, f"{path} has no {part}"
        target = getattr(target, part)
    return True, ""


class TestTheLedgerIsTotalOverTheIssue:
    def test_every_issue_bullet_has_exactly_one_entry(self) -> None:
        bullet_ids = [bullet_id for bullet_id, _ in ISSUE_BULLETS]
        entry_ids = [requirement.requirement_id for requirement in REQUIREMENTS]
        assert sorted(entry_ids) == sorted(bullet_ids), (
            "the ledger and the issue's requirement list disagree; missing "
            f"{sorted(set(bullet_ids) - set(entry_ids))}, extra "
            f"{sorted(set(entry_ids) - set(bullet_ids))}"
        )

    def test_no_entry_restates_its_requirement(self) -> None:
        """The ledger quotes the issue; it does not paraphrase it.

        A paraphrase is where a requirement quietly becomes an easier one,
        and the paraphrase is what a reader would then check the tests
        against.
        """

        by_id = dict(ISSUE_BULLETS)
        for requirement in REQUIREMENTS:
            assert requirement.issue_text == by_id[requirement.requirement_id], (
                f"{requirement.requirement_id} states its requirement "
                "differently from the issue"
            )

    def test_requirement_ids_are_unique(self) -> None:
        ids = [requirement.requirement_id for requirement in REQUIREMENTS]
        assert len(set(ids)) == len(ids), sorted(ids)

    def test_all_four_requirement_areas_are_represented(self) -> None:
        """Anti-vacuity for the totality check.

        If the bullet list were ever truncated to one area, the check above
        would still pass against a ledger truncated the same way.
        """

        prefixes = {
            requirement.requirement_id.rstrip("0123456789")
            for requirement in REQUIREMENTS
        }
        assert prefixes == {"A", "P", "X", "S", "D", "O"}, (
            f"the ledger covers only {sorted(prefixes)}"
        )


class TestEveryClaimIsBackedByATestThatExists:
    def test_every_entry_names_at_least_one_test(self) -> None:
        naked = [
            requirement.requirement_id
            for requirement in REQUIREMENTS
            if not requirement.proving_tests
        ]
        assert not naked, (
            f"these entries assert a status with no test behind it at all: {naked}"
        )

    @pytest.mark.parametrize(
        "requirement", REQUIREMENTS, ids=lambda item: item.requirement_id
    )
    def test_every_named_test_resolves(self, requirement) -> None:
        broken = []
        for node_id in requirement.proving_tests:
            ok, why = _resolve(node_id)
            if not ok:
                broken.append((node_id, why))
        assert not broken, (
            f"{requirement.requirement_id} names tests that do not exist, so "
            f"its status is a claim nothing checks: {broken}"
        )

    def test_the_resolver_can_actually_fail(self) -> None:
        """Without this, a resolver that always returned True would make
        every check above vacuous."""

        ok, why = _resolve(
            "tests/context_fabric/test_chaos_3620_authorization.py::"
            "TestThatDoesNotExist::test_nor_does_this"
        )
        assert not ok and "TestThatDoesNotExist" in why


class TestNoStatusIsAssertedWithoutItsExcuse:
    def test_every_non_proven_entry_states_a_reason(self) -> None:
        thin = [
            (requirement.requirement_id, len(requirement.reason))
            for requirement in REQUIREMENTS
            if requirement.status in NEEDS_REASON and len(requirement.reason) < 120
        ]
        assert not thin, (
            "these entries downgrade a requirement without explaining "
            f"themselves in enough detail to be checked: {thin}"
        )

    def test_every_blocked_entry_names_a_linear_issue(self) -> None:
        for requirement in REQUIREMENTS:
            if requirement.status not in NEEDS_BLOCKER:
                continue
            assert _BLOCKER_PATTERN.match(requirement.blocker), (
                f"{requirement.requirement_id} is {requirement.status} but "
                f"names blocker {requirement.blocker!r}, which is not a "
                "Linear issue id"
            )

    def test_a_proven_entry_names_no_blocker(self) -> None:
        """A proven requirement waiting on someone is not proven."""

        confused = [
            requirement.requirement_id
            for requirement in REQUIREMENTS
            if requirement.status is Status.PROVEN and requirement.blocker
        ]
        assert not confused, confused

    def test_every_defect_entry_cites_source_coordinates(self) -> None:
        """A defect without file:line is an opinion.

        The reason is what a reviewer checks against the code, and it is what
        the fixing lane works from.
        """

        for requirement in REQUIREMENTS:
            if requirement.status is not Status.DEFECT:
                continue
            assert ".py:" in requirement.reason, (
                f"{requirement.requirement_id} records a defect without "
                "naming a file and line"
            )


class TestTheHardestNewsCannotBeQuietlyUpgraded:
    def test_the_conflict_requirement_stays_NOT_ACCEPTED_on_CHAOS_3612(self) -> None:
        """The instruction is explicit: never silently passed.

        Asserted by requirement id and blocker id rather than by prose, so a
        future edit that marks it proven has to delete this test to do it.
        """

        conflict = next(
            requirement
            for requirement in REQUIREMENTS
            if requirement.requirement_id == "P4"
        )
        assert conflict.status is Status.NOT_ACCEPTED, (
            "the conflict/provenance requirement is no longer NOT_ACCEPTED; "
            "it may only be accepted once CHAOS-3612 is Done, and that has "
            "to be recorded deliberately rather than by editing a status"
        )
        assert conflict.blocker == "CHAOS-3612", (
            f"the conflict requirement names blocker {conflict.blocker!r}"
        )

    def test_the_zero_leakage_gate_stays_NOT_ACCEPTED_on_CHAOS_3627(self) -> None:
        gate = next(
            requirement
            for requirement in REQUIREMENTS
            if requirement.requirement_id == "A9"
        )
        assert gate.status is Status.NOT_ACCEPTED, (
            "the hard zero-leakage gate is marked accepted; it cannot be "
            "until the oracle that owns the dimension can return clean for a "
            "graph-arm packet (CHAOS-3627)"
        )
        assert gate.blocker == "CHAOS-3627", (
            f"the zero-leakage gate names blocker {gate.blocker!r}"
        )

    def test_the_four_known_defects_are_still_recorded_as_defects(self) -> None:
        """If a fix lands, this test is the reminder to update the ledger.

        The pinning tests in the suites go red when the behaviour changes;
        this goes red when someone updates the behaviour and forgets the
        record, which is the more likely order.
        """

        defects = {
            requirement.requirement_id
            for requirement in REQUIREMENTS
            if requirement.status is Status.DEFECT
        }
        assert defects == {"P1", "P6", "S5"}, (
            f"the recorded defect set changed to {sorted(defects)}; update "
            "the CHAOS-3620 findings record and the lane report together"
        )


class TestInheritedInvariantsCarryATransferDisposition:
    """ "Proven by CHAOS-3617" is not automatically proven here.

    Where a 3617 proof ran only on the synthetic fixtures — whose authorized
    set is a hand-written tuple — the result does not transfer to the corpus
    world under real per-principal grants without saying so. Every 3617
    result this lane leans on instead of re-proving therefore carries an
    explicit disposition, and the one that is genuinely synthetic-only says
    why the corpus cannot reach it.
    """

    def test_the_register_is_not_empty(self) -> None:
        assert INHERITED_INVARIANTS, (
            "no inherited invariant is registered, which would mean this "
            "lane re-proved everything — check before believing it"
        )

    @pytest.mark.parametrize(
        "inherited", INHERITED_INVARIANTS, ids=lambda item: item.transfer.value
    )
    def test_every_inherited_invariant_names_evidence_that_resolves(
        self, inherited
    ) -> None:
        broken = []
        for node_id in inherited.evidence:
            ok, why = _resolve(node_id)
            if not ok:
                broken.append((node_id, why))
        assert not broken, (
            f"{inherited.invariant!r} cites evidence that does not exist: {broken}"
        )

    def test_every_inherited_invariant_states_its_reason(self) -> None:
        thin = [
            (inherited.invariant, len(inherited.reason))
            for inherited in INHERITED_INVARIANTS
            if len(inherited.reason) < 100
        ]
        assert not thin, (
            "these inherited invariants assert a transfer disposition "
            f"without justifying it: {thin}"
        )

    def test_a_synthetic_only_disposition_explains_why_the_corpus_cannot_reach_it(
        self,
    ) -> None:
        """The one that would otherwise be a shrug.

        ``synthetic_only`` is the disposition a reader must be able to argue
        with, so its reason has to name the mechanism that makes the corpus
        path inert — not merely assert that it is.
        """

        synthetic = [
            inherited
            for inherited in INHERITED_INVARIANTS
            if inherited.transfer is Transfer.SYNTHETIC_ONLY
        ]
        assert synthetic, (
            "no invariant is recorded synthetic-only; if that is genuinely "
            "true it is a strong claim and should be checked, not assumed"
        )
        for inherited in synthetic:
            assert "NOT exercised" in inherited.reason, (
                f"{inherited.invariant!r} is synthetic-only without saying so plainly"
            )

    def test_the_synthetic_only_claim_is_true_of_the_corpus_path_today(self) -> None:
        """The disposition, checked against the arm rather than believed.

        The attestation guard is recorded synthetic-only because the corpus
        path never makes a semantic claim. That is an observable property of
        a real corpus readout, so it is observed: no attested embedder, and
        every committed subject resolved by exact identifier.
        """

        from tests.context_fabric import chaos_3620_spine as spine

        readout = spine.readout_for(("proj_acr",))
        assert readout.embedder_model_id is None, (
            "the corpus readout now attests an embedder, so the semantic "
            "claim path is live on this world and the synthetic-only "
            "disposition is stale"
        )
        packet = spine.packet_from(readout)
        signals = {
            str(signal.signal)
            for candidate in packet.subject_discovery.candidates
            for signal in candidate.match_signals
        }
        assert signals <= {"exact_canonical_id"}, (
            "a corpus subject now resolves by something other than an exact "
            f"identifier ({sorted(signals)}), so semantic-claim guards are "
            "reachable on this world and must be re-proved here"
        )


class TestTheArchitecturePageAgreesWithTheLedger:
    """A page that says "33 proven" is a claim, and claims drift.

    The page is what a decision-owner reads. If the ledger's counts move and
    the page does not, the page becomes the most confidently wrong artifact
    in the changeset.

    The first version of this class claimed to check "both directions" and
    checked one: ledger → page. Adversarial review pointed out that a page
    row the ledger never produced — a stale status count, an extra defect —
    stayed green, and that is exactly the drift direction that matters,
    because it is the page a human trusts. Both legs are now real, and the
    page→ledger leg is the one that caught a live contradiction: A9 was
    listed in the page's defect table while the ledger records it
    ``not_accepted``.
    """

    PAGE = (
        _REPO_ROOT
        / "docs"
        / "contribute"
        / "architecture"
        / "ask-dev-graph-safety-proof.md"
    )

    def test_the_page_exists(self) -> None:
        assert self.PAGE.is_file(), f"{self.PAGE} is missing"

    def test_every_status_count_on_the_page_matches_the_ledger(self) -> None:
        from collections import Counter

        page = self.PAGE.read_text(encoding="utf-8")
        counts = Counter(requirement.status.value for requirement in REQUIREMENTS)
        for status, count in counts.items():
            assert f"`{status}` | {count} |" in page, (
                f"the page does not record {count} `{status}` requirements; "
                f"the ledger has {dict(counts)}"
            )

    def test_the_page_names_every_blocker_the_ledger_names(self) -> None:
        page = self.PAGE.read_text(encoding="utf-8")
        for requirement in REQUIREMENTS:
            if not requirement.blocker:
                continue
            assert requirement.blocker in page, (
                f"{requirement.requirement_id} is blocked on "
                f"{requirement.blocker} and the page never mentions it"
            )

    def test_the_page_names_every_recorded_defect(self) -> None:
        page = self.PAGE.read_text(encoding="utf-8")
        for requirement in REQUIREMENTS:
            if requirement.status is not Status.DEFECT:
                continue
            assert f"| {requirement.requirement_id} |" in page, (
                f"the page's defect table omits {requirement.requirement_id}"
            )

    # ---- page -> ledger: the leg that was missing -----------------------

    def _page_defect_rows(self) -> set[str]:
        """Requirement ids the page's defect table claims, parsed from it.

        Anchored on the table's own heading so an unrelated table elsewhere
        on the page cannot contribute rows — and so deleting the heading
        fails loudly instead of silently emptying the comparison.
        """

        page = self.PAGE.read_text(encoding="utf-8")
        marker = "defects in merged code"
        assert marker in page, (
            "the page no longer has a defect-table heading, so the "
            "page-to-ledger comparison would compare against nothing"
        )
        section = page[page.index(marker) :]
        section = section.split("\n## ", 1)[0]
        return {
            row.split("|")[1].strip()
            for row in section.splitlines()
            if row.startswith("| ") and "---" not in row and not row.startswith("| ID ")
        }

    def test_the_page_claims_NO_defect_the_ledger_does_not_record(self) -> None:
        """The direction that was never checked.

        A page listing a requirement as a defect that the ledger records
        otherwise is a contradiction a reader resolves in the page's favour,
        because the page is what they are reading. This caught A9 sitting in
        the defect table while the ledger had it ``not_accepted``.
        """

        ledger_defects = {
            requirement.requirement_id
            for requirement in REQUIREMENTS
            if requirement.status is Status.DEFECT
        }
        page_defects = self._page_defect_rows()
        assert page_defects == ledger_defects, (
            "the page's defect table and the ledger disagree; page has "
            f"{sorted(page_defects - ledger_defects)} extra, missing "
            f"{sorted(ledger_defects - page_defects)}"
        )

    def test_the_page_claims_no_status_count_the_ledger_does_not_produce(
        self,
    ) -> None:
        """Every status row on the page must be one the ledger actually has.

        A stale row left behind after a status moves — "2 not_accepted" when
        there are now three — reads as current. Parsed from the page rather
        than assumed.
        """

        from collections import Counter

        page = self.PAGE.read_text(encoding="utf-8")
        counts = Counter(requirement.status.value for requirement in REQUIREMENTS)
        rows = dict(re.findall(r"\|\s*`(\w+)`\s*\|\s*(\d+)\s*\|", page))
        assert rows, "no status-count rows found on the page"
        for status, claimed in rows.items():
            assert status in counts, (
                f"the page claims a `{status}` row that the ledger does not "
                f"produce; ledger statuses are {sorted(counts)}"
            )
            assert int(claimed) == counts[status], (
                f"the page says {claimed} `{status}` requirements; the ledger "
                f"has {counts[status]}"
            )
        assert set(rows) == set(counts), (
            "the page omits status rows the ledger produces: "
            f"{sorted(set(counts) - set(rows))}"
        )


class TestTheRenderedReportIsUsable:
    def test_it_names_every_requirement(self) -> None:
        report = render()
        for requirement in REQUIREMENTS:
            assert f"`{requirement.requirement_id}`" in report, (
                f"{requirement.requirement_id} is missing from the rendered report"
            )

    def test_it_carries_the_reason_for_every_non_proven_requirement(self) -> None:
        report = render()
        for requirement in REQUIREMENTS:
            if requirement.status is Status.PROVEN:
                continue
            assert requirement.reason[:60] in report, (
                f"the rendered report omits why {requirement.requirement_id} "
                "is not proven"
            )

    def test_it_does_not_claim_everything_passed(self) -> None:
        """The report's whole purpose is that green does not mean all of it."""

        report = render()
        assert "not_accepted" in report and "defect" in report, (
            "the rendered report shows no non-proven status, which would "
            "misrepresent this lane's result"
        )
