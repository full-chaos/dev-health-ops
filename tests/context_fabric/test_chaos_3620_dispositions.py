"""CHAOS-3620: the ledger's own verification.

``chaos_3620_dispositions.py`` is the artifact a decision-owner reads instead
of forty test files. That makes it the one place in this lane where a
comfortable inaccuracy would do the most damage: a requirement marked
``PROVEN`` on a test that no longer exists is worse than no entry at all,
because a reader who sees "proven" stops checking.

Everything below exists to make that impossible. The tests named by every
entry must resolve. Every non-proven status must state a reason and, where
someone else owns the fix, name a Linear blocker. The ledger must be total
over the issue's own bullets and must not invent any. Requirements that
carry the lane's hardest news are asserted by requirement id and blocker id
rather than by prose, so none of them can be quietly upgraded to a pass by
editing a status field — ``P4``'s CHAOS-3612 pin is an example, checked
below by id rather than restated here by count. A prose count in this
paragraph is what went stale last time: this docstring used to name
``A9`` as a second pinned entry, blocked on CHAOS-3627, until that issue
landed and the row it pinned flipped to ``PROVEN``.

CHAOS-3620 is completed discovery-era verification, not the release gate —
that is CHAOS-3503 (Wave 3.2). This suite exists to keep the record of what
was proved honest, not to decide whether the arm may ship.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

from tests.context_fabric import chaos_3620_dispositions
from tests.context_fabric.chaos_3620_dispositions import (
    _BLOCKER_PATTERN,
    GATE_STATUS_TOKENS,
    INHERITED_INVARIANTS,
    ISSUE_BULLETS,
    NEEDS_BLOCKER,
    NEEDS_REASON,
    REQUIREMENTS,
    Status,
    Transfer,
    blocked_requirements_heading,
    defects_section_heading,
    gate_status_block,
    render,
)
from tests.context_fabric.chaos_3620_spine import _contains_token as _whole_token

_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Captured at true module scope. Referencing bare ``__doc__`` from inside a
#: class body would resolve to *that class's own* docstring instead — Python
#: sets a local ``__doc__`` while executing a class body with a docstring —
#: which is exactly the wrong thing for a check whose point is scanning this
#: file's top-level module docstring.
_THIS_MODULE_DOCSTRING = __doc__ or ""

#: Words a human-readable ledger summary must never spell out — a count
#: belongs to :func:`~tests.context_fabric.chaos_3620_dispositions.gate_status_block`
#: or one of its siblings, generated from :data:`REQUIREMENTS`, never to prose.
_COUNT_WORDS = (
    "zero",
    "one",
    "two",
    "three",
    "four",
    "five",
    "six",
    "seven",
    "eight",
    "nine",
    "ten",
)

#: What immediately precedes a run of digits that makes it an identifier
#: rather than a count — a Linear issue id, a delivery-wave label, a PR
#: number, or a ``chaos_####_*`` module/file name. Matched by prefix, not by
#: an allowlist of specific numbers, so CHAOS-9999 tomorrow needs no update
#: here.
_IDENTIFIER_DIGIT_PREFIXES = ("CHAOS-", "Wave ", "PR #", "PR#", "chaos_")
_DIGIT_RUN = re.compile(r"\d[\d.]*")


#: A requirement id is a single area letter directly touching its digits —
#: ``A9``, ``P4``, ``X1`` — with no separator to anchor a prefix check on,
#: unlike ``CHAOS-####``. :data:`ISSUE_BULLETS` names the closed set of area
#: letters, so this is exhaustive rather than a guess.
_REQUIREMENT_AREA_LETTERS = frozenset(
    requirement_id[0] for requirement_id, _ in ISSUE_BULLETS
)


def _bare_counts(text: str) -> list[str]:
    """Digit runs in ``text`` that are not part of an id: a CHAOS-####,
    Wave #.#, PR ##### or requirement id (``A9``, ``P4``, ...)."""

    offenders = []
    for match in _DIGIT_RUN.finditer(text):
        prefix = text[max(0, match.start() - 8) : match.start()]
        if any(prefix.endswith(marker) for marker in _IDENTIFIER_DIGIT_PREFIXES):
            continue
        if prefix and prefix[-1] in _REQUIREMENT_AREA_LETTERS:
            continue
        offenders.append(match.group(0))
    return offenders


#: Status-shaped words that make a count on the same LINE a live claim about
#: :data:`REQUIREMENTS`, rather than an incidental number elsewhere in the
#: same docstring — e.g. "Three properties make this a ledger rather than a
#: README" a few paragraphs down, which is a fixed structural description,
#: not a status summary that can drift when a requirement's status moves.
_STATUS_CLAIM_WORDS = (
    "defect",
    "defects",
    "violated",
    "violate",
    "blocked",
    "blocker",
    "unmeasured",
    "accepted",
)


def _sentences(text: str) -> list[str]:
    """Split hand-wrapped prose into sentences rather than lines.

    A docstring hard-wraps at ~79 columns, so a count word and the status
    word it modifies routinely land on different LINES of the same
    sentence — exactly the shape this file's own stale docstring had:
    "two entries" on one line, "blocked on CHAOS-3612" on the next.
    Collapsing whitespace and splitting on sentence punctuation reassembles
    what the wrap tore apart.
    """

    collapsed = re.sub(r"\s+", " ", text)
    return [s.strip() for s in re.split(r"(?<=[.!?])\s+", collapsed) if s.strip()]


def _count_status_claims(text: str) -> list[str]:
    """Sentences pairing a count word with a status-claim word."""

    offenders = []
    for sentence in _sentences(text):
        folded = sentence.casefold()
        counts = [word for word in _COUNT_WORDS if _whole_token(folded, word)]
        statuses = [word for word in _STATUS_CLAIM_WORDS if _whole_token(folded, word)]
        if counts and statuses:
            offenders.append(sentence)
    return offenders


#: Checksum of the transcribed CHAOS-3620 requirement text. Pins BOTH sides
#: of the totality check so a bullet and its entry cannot be deleted
#: together. The transcription is verified against Linear by the
#: orchestrator at merge time -- nothing here can do that.
_ISSUE_BULLETS_DIGEST = "de67b3605f2ff81f"


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

    def test_all_requirement_area_prefixes_are_represented(self) -> None:
        """Anti-vacuity for the totality check.

        If the bullet list were ever truncated to one area, the check above
        would still pass against a ledger truncated the same way. The set of
        prefixes below is asserted in full rather than counted, so adding or
        removing an area cannot leave a stale count behind — this test's own
        former name did exactly that (it said "four" areas years after a
        fifth and sixth, ``D`` and ``O``, were added).
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

    def test_a_proven_entry_carries_no_REASON_either(self) -> None:
        """The missing half of the invariant above, and it had already bitten.

        ``reason`` exists to explain a downgrade. A ``proven`` entry carrying
        one means a stale excuse survived an upgrade — which is exactly what
        happened to X5: it was downgraded, then built and upgraded, and its
        old "recorded unmeasured rather than proven by adjacency" text stayed
        behind. The ledger then simultaneously claimed the requirement was
        proven and explained why it was not.

        Scope belongs in ``notes``; ``reason`` is reserved for statuses that
        owe an excuse. That separation is what makes this checkable.
        """

        contradictory = [
            requirement.requirement_id
            for requirement in REQUIREMENTS
            if requirement.status is Status.PROVEN and requirement.reason
        ]
        assert not contradictory, (
            "these entries are marked proven while still carrying a reason "
            f"explaining why they are not: {contradictory}. Move scope into "
            "notes and delete the stale downgrade text"
        )

    def test_the_issue_bullet_list_cannot_shrink_symmetrically(self) -> None:
        """Totality is vacuous if both sides can be deleted together.

        ``test_every_issue_bullet_has_exactly_one_entry`` compares the ledger
        against ``ISSUE_BULLETS`` — so deleting a bullet AND its entry passes.
        The count and a checksum of the transcribed text pin both sides.

        NOTE FOR THE MERGE CHECK: the transcription itself is verified
        against Linear by the orchestrator at merge time. Nothing in this
        repository can check that the text matches the issue; this only
        ensures it has not changed since it was transcribed and reviewed.
        """

        import hashlib

        assert len(ISSUE_BULLETS) == 44, (
            f"the issue bullet list is now {len(ISSUE_BULLETS)} entries, not "
            "44. If CHAOS-3620 genuinely changed, update this pin and say so "
            "in the PR; if not, a requirement has been dropped"
        )
        digest = hashlib.sha256(
            "␟".join(f"{k}␞{v}" for k, v in ISSUE_BULLETS).encode()
        ).hexdigest()[:16]
        assert digest == _ISSUE_BULLETS_DIGEST, (
            "the transcribed requirement text changed. Re-verify it against "
            f"the Linear issue, then update the digest to {digest}"
        )

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

        Asserted by requirement id and blocker id rather than by prose, so
        upgrading it requires visibly editing the pin -- a reviewer sees the
        change in the diff rather than inferring it from a status field.
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

    def test_the_zero_leakage_gate_is_ACCEPTED_now_that_CHAOS_3627_landed(
        self,
    ) -> None:
        """FLIPPED by CHAOS-3627 (PR #1617).

        This pinned A9 at NOT_ACCEPTED with CHAOS-3627 named as its blocker,
        because the oracle that owns the dimension could not return clean for
        any graph-arm packet. It can now: the arm cites source-issued handles,
        the declared set is entity vocabulary, and evidence attribution names
        the entity the record is about. The gate is signed off on a measured
        clean audit, not on the absence of a measurement.

        The blocker must be EMPTY, not merely a different string -- a gate
        that is accepted while still naming a blocker is the shape this
        ledger's own reason/status guard exists to catch.
        """

        gate = next(
            requirement
            for requirement in REQUIREMENTS
            if requirement.requirement_id == "A9"
        )
        assert gate.status is Status.PROVEN, (
            "the hard zero-leakage gate is not accepted; CHAOS-3627 closed "
            "the oracle-vocabulary blocker, so either the fix regressed or "
            "the ledger was not flipped with it"
        )
        assert not gate.blocker, (
            f"the accepted zero-leakage gate still names blocker {gate.blocker!r}"
        )

    def test_the_known_defect_set_is_still_recorded_as_defects(self) -> None:
        """If a fix lands, this test is the reminder to update the ledger.

        The pinning tests in the suites go red when the behaviour changes;
        this goes red when someone updates the behaviour and forgets the
        record, which is the more likely order. The set is asserted in full
        rather than counted in the test's own name, on purpose: this test
        used to be named for a count ("the four known defects") that PR
        #1618 made false by fixing three of them, while the name sat still.
        """

        defects = {
            requirement.requirement_id
            for requirement in REQUIREMENTS
            if requirement.status is Status.DEFECT
        }
        # FLIPPED by PR #1618: P1 (CHAOS-3630), P6 (CHAOS-3628) and S5
        # (CHAOS-3629) are fixed and their proving tests now assert the fixed
        # behaviour.
        # NARROWED by PR #1619 (CHAOS-3637): X1's TITLE carrier is closed --
        # an instruction-shaped label is withheld and the record kept -- but
        # X1 stays a DEFECT, because the canonical-ID carrier is still open
        # and was measured reaching the wire, and the detector is
        # deliberately narrow. A bullet half closed is not closed; the reason
        # and notes on the X1 row carry both residuals.
        assert defects == {"X1"}, (
            f"the recorded defect set changed to {sorted(defects)}; update "
            "the CHAOS-3620 findings record and the lane report together"
        )


class TestTheLedgerProseNamesNoCountThatCanDrift:
    """The ledger's own module docstrings are a "human-readable summary" too.

    This is what actually went stale, and it is why this class exists:
    ``chaos_3620_dispositions.py``'s own docstring said "four... violated by
    merged code" until PR #1618 fixed three of the four, and "one is blocked
    on CHAOS-3627" until PR #1617 landed. This file's docstring made the
    identical claim about "the two entries". Neither count was derived from
    :data:`REQUIREMENTS`; both were typed once and left to rot.

    Banning counts from the two module docstrings entirely — route them
    through :func:`gate_status_block`, :func:`defects_section_heading` or
    :func:`blocked_requirements_heading` instead — makes that drift
    structurally impossible rather than relying on someone remembering to
    edit prose the next time a requirement's status moves.
    """

    _SOURCES = (
        (
            "chaos_3620_dispositions.py module docstring",
            chaos_3620_dispositions.__doc__ or "",
        ),
        (
            "test_chaos_3620_dispositions.py module docstring",
            _THIS_MODULE_DOCSTRING,
        ),
    )

    def test_no_module_docstring_line_pairs_a_count_with_a_status_claim(
        self,
    ) -> None:
        for label, doc in self._SOURCES:
            offenders = _count_status_claims(doc)
            assert not offenders, (
                f"{label} pairs a count word with a status claim on one "
                f"line ({offenders}); that is exactly the restated summary "
                "that drifted before — derive it from REQUIREMENTS via "
                "gate_status_block(), defects_section_heading() or "
                "blocked_requirements_heading() instead"
            )

    def test_no_module_docstring_names_a_bare_digit(self) -> None:
        for label, doc in self._SOURCES:
            offenders = _bare_counts(doc)
            assert not offenders, (
                f"{label} states digits in prose ({offenders}); a "
                "CHAOS-####/Wave #.#/PR ##### identifier is fine, a bare "
                "count is not"
            )

    def test_the_scanners_can_actually_fail(self) -> None:
        """Without this, a scanner that never matched would make both checks
        above vacuous."""

        wrapped = (
            "This lane's green run does not mean that: four of the\n"
            "issue's requirements are violated by merged code."
        )
        assert _count_status_claims(wrapped) == [
            "This lane's green run does not mean that: four of the "
            "issue's requirements are violated by merged code."
        ]
        assert (
            _count_status_claims("Three properties make this a ledger, not a README.")
            == []
        )
        assert _bare_counts("four of the issue's requirements") == []
        assert _bare_counts("12 of 44 requirements, see CHAOS-3612") == [
            "12",
            "44",
        ]


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

    def test_every_inherited_3617_test_appears_in_the_register(self) -> None:
        """F9: the register must be complete, not merely non-empty.

        The register exists so that leaning on a CHAOS-3617 result carries an
        explicit transfer disposition. That guarantee is worth nothing if a
        ledger entry can cite a 3617 test the register never mentions —
        which is exactly the shape of "the check covers what it happens to
        list" this lane has now hit six times.

        Derived from the ledger rather than hand-listed, so adding a 3617
        citation without a disposition is red immediately.
        """

        cited_3617 = {
            node_id
            for requirement in REQUIREMENTS
            for node_id in requirement.proving_tests
            if "test_chaos_3617" in node_id
        }
        registered = {
            node_id
            for inherited in INHERITED_INVARIANTS
            for node_id in inherited.evidence
        }
        undisposed = sorted(cited_3617 - registered)
        assert not undisposed, (
            "these CHAOS-3617 tests are cited as proving a CHAOS-3620 "
            "requirement but carry no transfer disposition, so nobody has "
            "said whether the result transfers to the corpus world under "
            f"true grants: {undisposed}"
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

    def test_the_page_defect_heading_is_DERIVED_from_the_ledger(self) -> None:
        """The heading, not just the table under it.

        The status-count table above and the defect table's row set were
        already checked and already agreed with the ledger — the drift this
        catches lived one level up, in the section heading itself: a literal
        ``## Four defects in merged code`` that outlived three of the four
        being fixed under PR #1618.
        """

        page = self.PAGE.read_text(encoding="utf-8")
        expected = defects_section_heading()
        assert expected in page, (
            "the page's defect-section heading does not match the ledger's "
            f"defect count. Expected {expected!r}; regenerate with "
            "defects_section_heading() rather than editing the page's wording"
        )

    def test_the_page_blocked_heading_is_DERIVED_from_the_ledger(self) -> None:
        """Same drift, the not-accepted section's heading."""

        page = self.PAGE.read_text(encoding="utf-8")
        expected = blocked_requirements_heading()
        assert expected in page, (
            "the page's not-accepted-section heading does not match the "
            f"ledger's not_accepted count. Expected {expected!r}; regenerate "
            "with blocked_requirements_heading() rather than editing the "
            "page's wording"
        )

    # ---- page -> ledger: the leg that was missing -----------------------

    def _page_defect_rows(self) -> set[str]:
        """Requirement ids the page's defect table claims, parsed from it.

        Anchored on the table's own heading so an unrelated table elsewhere
        on the page cannot contribute rows — and so deleting the heading
        fails loudly instead of silently emptying the comparison.
        """

        page = self.PAGE.read_text(encoding="utf-8")
        # Hardcoding "defects" (plural) here was itself the same drift this
        # whole ticket is about: it silently assumed the count could never
        # fall to one. Anchored on the same derived heading the
        # count-agreement test above requires, so it tracks whatever the
        # ledger's actual defect count singularises or pluralises to.
        marker = defects_section_heading().removeprefix("## ")
        assert marker in page, (
            "the page no longer has a defect-table heading, so the "
            "page-to-ledger comparison would compare against nothing"
        )
        section = page[page.index(marker) :]
        section = section.split("\n## ", 1)[0]
        # Any row starting with a pipe, not just "| ". Review found
        # ``|Z9|fabricated defect|`` ignored entirely, so a fabricated row
        # written without spaces was invisible to the comparison — the
        # parser covered the rows it happened to expect.
        rows = set()
        for row in section.splitlines():
            stripped = row.strip()
            if not stripped.startswith("|") or "---" in stripped:
                continue
            cells = [cell.strip() for cell in stripped.strip("|").split("|")]
            if not cells or cells[0] in {"", "ID"}:
                continue
            rows.add(cells[0])
        return rows

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

    #: The page must say this, verbatim, for as long as the ledger holds any
    #: requirement that is not proven. It is the sentence a reader takes away,
    #: and the tables below it are what they check only if it makes them
    #: curious.
    NOT_GREEN_STATEMENT = "**The hard gate is not green.**"

    #: Phrasings that assert a clean result. Forbidden while the ledger
    #: disagrees. A list of phrases is not a complete defence on its own —
    #: which is why the required-statement check above carries the weight —
    #: but it closes the specific rewrite an author reaches for first.
    GREEN_CLAIMS = (
        "hard gate is green",
        "every requirement passed",
        "all requirements passed",
        "all hard safety gates are green",
        "no defects were found",
    )

    def test_the_pages_gate_status_sentence_is_DERIVED_from_the_ledger(
        self,
    ) -> None:
        """The structural end of the paraphrase problem.

        Three rounds of forbidding green-sounding phrasings only moved the
        attack: an exact-sentence check was bypassed by adding a second
        sentence, and a five-phrase blocklist by paraphrasing. The class of
        problem does not close by lengthening a list, so it is closed by
        removing the authorship: the page must contain
        ``gate_status_block()`` **verbatim**, generated from the ledger's own
        statuses. There is nothing left to paraphrase, because the page does
        not get to write this sentence.

        This is the merge-gating check of the three, and it is deliberately
        the last word on the subject — the descent stops here.
        """

        page = self.PAGE.read_text(encoding="utf-8")
        expected = gate_status_block()
        assert expected in page, (
            "the page does not carry the ledger-derived gate-status sentence "
            f"verbatim. Expected:\n\n{expected}\n\nRegenerate it with "
            "`gate_status_block()` rather than editing the page's wording"
        )

    def test_no_OTHER_line_pairs_a_gate_word_with_a_clean_result_word(
        self,
    ) -> None:
        """One whole-token scan, everywhere, comments included.

        The companion to the derived sentence: gate status may be asserted in
        exactly one place, so anywhere else that pairs a gate word with a
        clean-result word is a second, unauthorised claim — whether it sits
        in prose, a table, an HTML comment or a collapsed block.

        Whole-token matched, because a substring scan on ``gate`` hits
        ``mitigate``/``delegate`` and a check that cries wolf is one people
        learn to ignore — the same lesson the disclosure walker's ``Ember``
        false positive taught.
        """

        page = self.PAGE.read_text(encoding="utf-8")
        derived = gate_status_block()
        clean_words = ("green", "passed", "passes", "satisfied", "clear")
        offenders = []
        for number, line in enumerate(page.splitlines(), start=1):
            if line.strip() and line.strip() in derived:
                continue
            folded = line.casefold()
            has_gate = any(_whole_token(folded, token) for token in GATE_STATUS_TOKENS)
            if not has_gate:
                continue
            hit = [word for word in clean_words if _whole_token(folded, word)]
            if hit:
                offenders.append((number, hit, line.strip()[:90]))
        assert not offenders, (
            "gate status is asserted outside the derived sentence; the page "
            f"may state it in exactly one place: {offenders}"
        )

    def test_the_page_cannot_claim_a_clean_result_while_the_ledger_disagrees(
        self,
    ) -> None:
        """The attack the first version of this class did not stop.

        The independent verifier flipped the page's headline to "the gate is
        green", left a fabricated row behind, and the suite stayed green: the
        checks compared TABLES and never the sentence a reader actually takes
        away. A page whose tables are correct and whose headline is a lie is
        worse than no page, because the headline is what gets quoted.

        Keyed on the ledger's own state, so it cannot go stale: the required
        statement is demanded exactly while something is unproven, and is
        released automatically if everything ever becomes proven.
        """

        page = self.PAGE.read_text(encoding="utf-8")
        unproven = [
            requirement
            for requirement in REQUIREMENTS
            if requirement.status is not Status.PROVEN
        ]

        if unproven:
            assert self.NOT_GREEN_STATEMENT in page, (
                f"the ledger holds {len(unproven)} unproven requirements and "
                f"the page does not carry {self.NOT_GREEN_STATEMENT!r}. The "
                "headline is what a reader quotes; it may not be softer than "
                "the ledger"
            )
            folded = page.casefold()
            claimed = [claim for claim in self.GREEN_CLAIMS if claim in folded]
            assert not claimed, (
                f"the page asserts a clean result ({claimed}) while the "
                f"ledger records {len(unproven)} unproven requirements"
            )
        else:
            assert self.NOT_GREEN_STATEMENT not in page, (
                "every requirement is proven but the page still says the gate "
                "is not green — the page is now understating the result"
            )

    def test_the_page_names_every_blocked_requirement_and_its_blocker(self) -> None:
        """A ``not_accepted`` requirement that the page never mentions is a
        gate a reader does not know is closed."""

        page = self.PAGE.read_text(encoding="utf-8")
        for requirement in REQUIREMENTS:
            if requirement.status is not Status.NOT_ACCEPTED:
                continue
            assert requirement.requirement_id in page, (
                f"{requirement.requirement_id} is blocked and the page never names it"
            )
            assert requirement.blocker in page, (
                f"{requirement.requirement_id} is blocked on "
                f"{requirement.blocker} and the page never names the blocker"
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
        found = re.findall(r"\|\s*`(\w+)`\s*\|\s*(\d+)\s*\|", page)
        # DUPLICATES ARE AN ERROR rather than something to collapse. ``dict``
        # kept the LAST occurrence, so a stale row sitting above a corrected
        # one was invisible while the page visibly contradicted itself.
        seen = [status for status, _ in found]
        duplicates = sorted({status for status in seen if seen.count(status) > 1})
        assert not duplicates, (
            f"the page states these statuses more than once: {duplicates}. A "
            "stale row beside a corrected one is a visible contradiction"
        )
        rows = dict(found)
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
