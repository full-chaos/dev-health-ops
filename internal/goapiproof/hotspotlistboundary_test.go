package goapiproof

import (
	"os"
	"testing"
)

// This file exercises HotspotListBoundaryShape (hotspotlistboundary.go)
// both through small, self-contained node/link fixtures (one rule
// isolated per case) and through the REAL captured GET/POST /api/v1/
// sankey?mode=hotspot response pairs (two production deployed-vs-
// deployed prove runs) the declaration in restcorpus.go was derived
// from.

// hotspotBoundaryOptions mirrors sankeyRepoDedupParity's own shape wiring
// (restcorpus.go) at test scale: the sibling SankeyRepoFanoutShape entry
// runs first (ordinary pass) so HotspotListBoundaryShape's own second
// pass can read its covered LinkValuePath findings, exactly as
// production does.
func hotspotBoundaryOptions(limit int) Options {
	return Options{
		OrderInsensitiveLists: []OrderInsensitiveList{
			{Path: "data.nodes", KeyFields: []string{"name"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
			{Path: "data.links", KeyFields: []string{"source", "target"}, Reason: "test fixture", Ticket: "CHAOS-TEST-ORDER"},
		},
		BaselineDefects: []BaselineDefect{
			{
				Ticket: "CHAOS-TEST-FANOUT", Reason: "test fixture",
				Paths:        []string{"data.links"},
				Intermittent: true, IntermittentReason: "test fixture",
				SankeyRepoFanoutShape: &SankeyRepoFanoutShape{
					NodesListPath:       "data.nodes",
					LinksListPath:       "data.links",
					LinkValuePath:       "data.links.value",
					RepoNodeGroups:      []string{"repo"},
					FallbackAnchorNames: []string{"Unknown repo"},
				},
			},
			{
				Ticket: "CHAOS-TEST-BOUNDARY", Reason: "test fixture",
				Paths:        []string{"data.links", "data.nodes"},
				Intermittent: true, IntermittentReason: "test fixture",
				HotspotListBoundaryShape: &HotspotListBoundaryShape{
					NodesListPath:      "data.nodes",
					LinksListPath:      "data.links",
					LinkValuePath:      "data.links.value",
					RepoNodeGroup:      "repo",
					DirectoryNodeGroup: "directory",
					FileNodeGroup:      "file",
					Limit:              limit,
				},
			},
		},
	}
}

// hotspotBoundaryFixtureParams composes one small, closed hotspot graph:
// repoX fans out at k=2 (anchor->pin.go stable on both legs, dir1
// carrying two baseline-only leavers plus one shared file), repoY does
// not fan out at all (stable subtree present unchanged on both legs,
// dir2 carrying two candidate-only entrants that displace the leavers).
// Every numeric field is a parameter so each RED test can break exactly
// ONE of them.
type hotspotBoundaryFixtureParams struct {
	anchorBase, anchorCand float64 // repoX -> repoX/anchor (and its file/changetype echoes)
	dir1BaseTotal          float64 // repoX -> repoX/dir1 (the printed parent value on baseline)
	sharedBase, sharedCand float64 // repoX/dir1/shared.go
	leaver1, leaver2       float64 // baseline-only files under dir1
	entrant1, entrant2     float64 // candidate-only files under repoY/dir2
}

func defaultHotspotBoundaryFixtureParams() hotspotBoundaryFixtureParams {
	return hotspotBoundaryFixtureParams{
		anchorBase: 140, anchorCand: 70,
		dir1BaseTotal: 240, // 100 (shared) + 60 (leaver1) + 80 (leaver2)
		sharedBase:    100, sharedCand: 50,
		leaver1: 60, leaver2: 80,
		entrant1: 40, entrant2: 45,
	}
}

func buildHotspotBoundaryFixture(p hotspotBoundaryFixtureParams, includeLeaver2, includeEntrant2 bool) (baseline, candidate string) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / anchor", "directory") + "," +
		sankeyNode("repoX / anchor/pin.go", "file") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	if includeLeaver2 {
		baseNodes += "," + sankeyNode("repoX / dir1/leaver2.go", "file")
	}

	baseLinks := sankeyLink("repoX", "repoX / anchor", p.anchorBase) + "," +
		sankeyLink("repoX / anchor", "repoX / anchor/pin.go", p.anchorBase) + "," +
		sankeyLink("repoX / anchor/pin.go", "refactor", p.anchorBase) + "," +
		sankeyLink("repoX", "repoX / dir1", p.dir1BaseTotal) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", p.sharedBase) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", p.sharedBase) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", p.leaver1) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", p.leaver1) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900)
	if includeLeaver2 {
		baseLinks += "," + sankeyLink("repoX / dir1", "repoX / dir1/leaver2.go", p.leaver2) + "," +
			sankeyLink("repoX / dir1/leaver2.go", "refactor", p.leaver2)
	}

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / anchor", "directory") + "," +
		sankeyNode("repoX / anchor/pin.go", "file") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	if includeEntrant2 {
		candNodes += "," + sankeyNode("repoY / dir2/entrant2.go", "file")
	}

	candLinks := sankeyLink("repoX", "repoX / anchor", p.anchorCand) + "," +
		sankeyLink("repoX / anchor", "repoX / anchor/pin.go", p.anchorCand) + "," +
		sankeyLink("repoX / anchor/pin.go", "refactor", p.anchorCand) + "," +
		sankeyLink("repoX", "repoX / dir1", p.sharedCand) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", p.sharedCand) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", p.sharedCand) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", p.entrant1) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", p.entrant1)
	dir2Total := p.entrant1
	if includeEntrant2 {
		dir2Total += p.entrant2
		candLinks += "," + sankeyLink("repoY / dir2", "repoY / dir2/entrant2.go", p.entrant2) + "," +
			sankeyLink("repoY / dir2/entrant2.go", "refactor", p.entrant2)
	}
	candLinks += "," + sankeyLink("repoY", "repoY / dir2", dir2Total)

	return sankeyBody(baseNodes, baseLinks), sankeyBody(candNodes, candLinks)
}

// TestHotspotListBoundaryShape_ClosedFixtureIsFullyAdmitted is the
// positive case every RED test below breaks exactly one piece of: rule 3
// admits both leavers (their reduced value sits under candidate's
// minimum) and both entrants (one-for-one, under baseline's stable
// floor), rule 4 admits every file's own node+links plus repoY/dir2's
// own vanished node+link (every one of its children is an admitted
// entrant), and rule 4's parent identity closes repoX/dir1's own
// non-clean value mismatch exactly: 50 + (60+80) - 0 + (2-1)*50 = 240.
func TestHotspotListBoundaryShape_ClosedFixtureIsFullyAdmitted(t *testing.T) {
	p := defaultHotspotBoundaryFixtureParams()
	baseline, candidate := buildHotspotBoundaryFixture(p, true, true)
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(5))

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_ExcessEntrantsBeyondAdmittedLeaversStayOutside
// is RED for rule 3's one-entrant-per-admitted-leaver CAP specifically --
// distinct from TestReducedValueStillRanksInsideStaysOutside, which
// isolates the FLOOR check on a single entrant. Here THREE candidate-only
// files ALL individually satisfy baseline's own stable floor (100): only
// TWO leavers admit (leaver3's own reduced value fails the ceiling, the
// SAME already-tested mechanism), so only the two LOWEST-valued entrants
// (40, 45) may be admitted; the third (48), despite qualifying under the
// floor exactly like its two siblings, must stay outside purely because
// it is the third row against only two admitted leavers.
func TestHotspotListBoundaryShape_ExcessEntrantsBeyondAdmittedLeaversStayOutside(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / anchor", "directory") + "," +
		sankeyNode("repoX / anchor/pin.go", "file") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver2.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver3.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / anchor", 140) + "," +
		sankeyLink("repoX / anchor", "repoX / anchor/pin.go", 140) + "," +
		sankeyLink("repoX / anchor/pin.go", "refactor", 140) + "," +
		sankeyLink("repoX", "repoX / dir1", 100+60+80+5000) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 100) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 100) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", 60) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", 60) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver2.go", 80) + "," +
		sankeyLink("repoX / dir1/leaver2.go", "refactor", 80) + "," +
		// leaver3: reduced value (5000/2=2500) is far above candidate's own
		// minimum (40) -- the same ceiling check the floor-half test above
		// already isolates; here it exists only to make THREE candidate-only
		// rows outnumber the TWO leavers that actually admit.
		sankeyLink("repoX / dir1", "repoX / dir1/leaver3.go", 5000) + "," +
		sankeyLink("repoX / dir1/leaver3.go", "refactor", 5000) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / anchor", "directory") + "," +
		sankeyNode("repoX / anchor/pin.go", "file") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("repoY / dir2/entrant2.go", "file") + "," +
		sankeyNode("repoY / dir2/entrant3.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / anchor", 70) + "," +
		sankeyLink("repoX / anchor", "repoX / anchor/pin.go", 70) + "," +
		sankeyLink("repoX / anchor/pin.go", "refactor", 70) + "," +
		sankeyLink("repoX", "repoX / dir1", 50) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 50) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 50) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900) + "," +
		// entrant1/2/3: 40, 45, 48 -- ALL three sit at or under baseline's
		// own stable floor (100). Only the two lowest may be admitted.
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", 40) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", 40) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant2.go", 45) + "," +
		sankeyLink("repoY / dir2/entrant2.go", "refactor", 45) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant3.go", 48) + "," +
		sankeyLink("repoY / dir2/entrant3.go", "refactor", 48) + "," +
		sankeyLink("repoY", "repoY / dir2", 40+45+48)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(6))

	// Exactly 9 findings stay outside: leaver3's own node+2 links (fails
	// the ceiling), entrant3's own node+2 links (excluded by the cap
	// alone -- it satisfies the floor exactly like entrant1/entrant2,
	// which DO admit), repoY/dir2's own node+link (one unexplained child
	// voids the whole vanished directory), and repoX/dir1's own parent
	// value (its identity cannot close with leaver3 unadmitted).
	if result.DifferencesOutsideBaselineDefect != 9 {
		t.Fatalf("outside = %d, want 9: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawEntrant3Outside, sawEntrant1Node, sawEntrant2Node := false, false, false
	for _, f := range result.Findings {
		if f.Shape != ShapePresence {
			continue
		}
		switch f.Detail {
		case `key "repoY / dir2/entrant3.go" present in candidate, absent in baseline`:
			sawEntrant3Outside = true
		case `key "repoY / dir2/entrant1.go" present in candidate, absent in baseline`:
			sawEntrant1Node = true
		case `key "repoY / dir2/entrant2.go" present in candidate, absent in baseline`:
			sawEntrant2Node = true
		}
	}
	if !sawEntrant3Outside || !sawEntrant1Node || !sawEntrant2Node {
		t.Fatalf("expected all three entrants' own node-presence findings in the findings set: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_WrongDeclaredLimitStaysOutside is RED for
// rule 1: the identical closed fixture, but the declared Limit (10)
// never matches either leg's own reconstructed file count (5) -- the
// whole plan must refuse, leaving every leaver/entrant/parent finding
// that TestClosedFixtureIsFullyAdmitted shows CAN close, closed or not.
// A team-scoped request bounded short of Limit never reaches this shape
// at all (every production capture this shape is verified against sits
// at Limit on both legs, team-scoped or not); this Limit mismatch is
// purely a wrong declaration, and stays a hard refusal.
func TestHotspotListBoundaryShape_WrongDeclaredLimitStaysOutside(t *testing.T) {
	p := defaultHotspotBoundaryFixtureParams()
	baseline, candidate := buildHotspotBoundaryFixture(p, true, true)
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(10))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- a Limit that matches neither leg's own file count must refuse the whole plan: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_NoCoveredMultiplierStaysOutside is RED
// for rule 2: repoX's own direct edge (140/70 -> 141/70) no longer
// rounds to a clean integer k, so the SIBLING SankeyRepoFanoutShape never
// establishes repoX as a verified anchor and covers none of its edges --
// this shape's own rule 2 then finds no ALREADY-COVERED LinkValuePath
// finding to read a multiplier from, so it never independently
// re-derives one. Both leavers stay unadmitted (no k to divide by), and
// since rule 3's entrant count is gated on the number of ADMITTED
// leavers, the otherwise-unrelated repoY entrants lose their own
// admission too -- the same one-leaver-per-entrant coupling
// LimitDisplacementShape's own tests already pin.
func TestHotspotListBoundaryShape_NoCoveredMultiplierStaysOutside(t *testing.T) {
	p := defaultHotspotBoundaryFixtureParams()
	p.anchorBase = 141 // was a clean 140 (2x of 70); now no integer k validates.
	baseline, candidate := buildHotspotBoundaryFixture(p, true, true)
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(5))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- with no covered multiplier for repoX, nothing under it may be admitted: findings %+v", result.Findings)
	}
	sawLeaver1Outside := false
	for _, f := range result.Findings {
		if f.Shape == ShapePresence && f.Detail == `key "repoX / dir1/leaver1.go" present in baseline, absent in candidate` {
			sawLeaver1Outside = true
		}
	}
	if !sawLeaver1Outside {
		t.Fatalf("expected leaver1.go's own node-presence finding in the findings set: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_ReducedValueStillRanksInsideStaysOutside
// is RED for rule 3's leaver half: leaver2's own value is inflated well
// past what repoX's verified k=2 can explain (halved, it would still
// outrank candidate's own minimum), so it must never be admitted even
// though repoX's multiplier is genuinely verified this time. With only
// ONE leaver (leaver1) admitted, only ONE entrant may be admitted too --
// entrant2 (the higher of the two) must stay outside as well.
func TestHotspotListBoundaryShape_ReducedValueStillRanksInsideStaysOutside(t *testing.T) {
	p := defaultHotspotBoundaryFixtureParams()
	p.leaver2 = 5000 // 5000/2=2500, far above candidate's own minimum.
	p.dir1BaseTotal = 100 + 60 + 5000
	baseline, candidate := buildHotspotBoundaryFixture(p, true, true)
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(5))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0: findings %+v", result.Findings)
	}
	sawLeaver2Outside, sawEntrant2Outside := false, false
	for _, f := range result.Findings {
		if f.Shape != ShapePresence {
			continue
		}
		switch f.Detail {
		case `key "repoX / dir1/leaver2.go" present in baseline, absent in candidate`:
			sawLeaver2Outside = true
		case `key "repoY / dir2/entrant2.go" present in candidate, absent in baseline`:
			sawEntrant2Outside = true
		}
	}
	if !sawLeaver2Outside {
		t.Fatalf("expected leaver2.go's own node-presence finding in the findings set: findings %+v", result.Findings)
	}
	if !sawEntrant2Outside {
		t.Fatalf("expected entrant2.go's own node-presence finding in the findings set (no second admitted leaver to displace it): findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_VanishedDirectoryWithUnexplainedSiblingStaysOutside
// is RED for rule 4's directory half: entrant2's own value (5000) is far
// above baseline's stable floor, so it never admits -- but entrant1
// still does. repoY/dir2 then has ONE admitted child and ONE unexplained
// one, so the DIRECTORY's own node and repoY->dir2 link must stay
// outside even though entrant1's OWN node+links still individually
// admit.
func TestHotspotListBoundaryShape_VanishedDirectoryWithUnexplainedSiblingStaysOutside(t *testing.T) {
	p := defaultHotspotBoundaryFixtureParams()
	p.entrant2 = 5000 // far above baseline's stable floor (100).
	baseline, candidate := buildHotspotBoundaryFixture(p, true, true)
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(5))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0: findings %+v", result.Findings)
	}
	sawDir2NodeOutside, sawEntrant1NodeOutside := false, false
	for _, f := range result.Findings {
		if f.Shape != ShapePresence {
			continue
		}
		switch f.Detail {
		case `key "repoY / dir2" present in candidate, absent in baseline`:
			sawDir2NodeOutside = true
		case `key "repoY / dir2/entrant1.go" present in candidate, absent in baseline`:
			sawEntrant1NodeOutside = true
		}
	}
	if !sawDir2NodeOutside {
		t.Fatalf("expected dir2's own node-presence finding in the findings set (one unexplained sibling voids the whole directory): findings %+v", result.Findings)
	}
	_ = sawEntrant1NodeOutside // entrant1's own node still admits individually; presence in Findings alone does not distinguish covered from outside, see the outside-count assertion above.
}

// TestHotspotListBoundaryShape_ParentIdentityDoesNotCloseStaysOutside is
// RED for rule 4's parent-value half: repoX/dir1's own printed baseline
// total (241) no longer equals the sum any real mechanism could have
// produced (100 shared + 60 + 80 = 240, not 241) -- both leavers still
// individually admit (their own reduced values still rank correctly),
// but the PARENT link's own value finding must stay outside since the
// identity does not close to the printed digit.
func TestHotspotListBoundaryShape_ParentIdentityDoesNotCloseStaysOutside(t *testing.T) {
	p := defaultHotspotBoundaryFixtureParams()
	p.dir1BaseTotal = 241 // true sum is 240; off by one on purpose.
	baseline, candidate := buildHotspotBoundaryFixture(p, true, true)
	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(5))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- an identity that does not close to the printed digit must stay outside: findings %+v", result.Findings)
	}
	sawParentOutside := false
	for _, f := range result.Findings {
		if f.Path == "$.data.links[3].value" || (f.Shape == ShapeValue) {
			if key, ok := parseOrderInsensitiveDetailKey(f.Detail); ok && key == "repoX\x1frepoX / dir1" {
				sawParentOutside = true
			}
		}
	}
	if !sawParentOutside {
		t.Fatalf("expected repoX -> repoX/dir1's own value finding in the findings set: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_TiedBoundarySwapStaysOutside is RED for
// rule 5's documented residual gap: two files of EQUAL value swap
// presence with NO repository fan-out involved anywhere in the response
// (repoZ carries no value difference at all, so the sibling shape
// establishes no k for it) -- ORDER BY churn DESC has no secondary sort
// on either plane (services/sankey.py:202, sankey/queries.go:449), so
// this genuinely can happen on a real tie, and it must never be admitted
// by coincidence.
func TestHotspotListBoundaryShape_TiedBoundarySwapStaysOutside(t *testing.T) {
	nodes := func(fileName string) string {
		return sankeyNode("repoZ", "repo") + "," +
			sankeyNode("repoZ / dir", "directory") + "," +
			sankeyNode("repoZ / dir/"+fileName, "file") + "," +
			sankeyNode("refactor", "change_type")
	}
	links := func(fileName string) string {
		return sankeyLink("repoZ", "repoZ / dir", 100) + "," +
			sankeyLink("repoZ / dir", "repoZ / dir/"+fileName, 100) + "," +
			sankeyLink("repoZ / dir/"+fileName, "refactor", 100)
	}
	baseline := sankeyBody(nodes("tied_a.go"), links("tied_a.go"))
	candidate := sankeyBody(nodes("tied_b.go"), links("tied_b.go"))

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(1))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- a tied-value swap with no repository fan-out must never be admitted: findings %+v", result.Findings)
	}
}

// --- Real captured GET/POST /api/v1/sankey?mode=hotspot response pairs ---

func hotspotBoundarySnapshotFromFile(t *testing.T, path string) Snapshot {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	snapshot, err := DecodeRESTSnapshot(body)
	if err != nil {
		t.Fatalf("decode fixture %s: %v", path, err)
	}
	return snapshot
}

// TestHotspotListBoundaryShape_RealCapturedGET_LaterRunFullyAdmitted pins
// the production capture the declaration in restcorpus.go was derived
// from: GET /api/v1/sankey?mode=hotspot, the later of two runs against
// the same org, where full-chaos/dev-health-web's own unmerged repos row
// crosses the file-list boundary on three files and shifts its own
// repo->directory (src) parent link.
func TestHotspotListBoundaryShape_RealCapturedGET_LaterRunFullyAdmitted(t *testing.T) {
	baseline := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_get_boundary_baseline_bfcf8134.json")
	candidate := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_get_boundary_candidate_1fac62b6.json")

	result := Compare(baseline, candidate, sankeyHotspotParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_RealCapturedPOST_LaterRunFullyAdmitted is
// the POST sibling of the GET case above: the SAME repository, the SAME
// run, but two of the affected directories ((root) and src) each lose
// their ONLY listed file on the candidate leg and vanish entirely --
// rule 4's directory half, not its parent-value half.
func TestHotspotListBoundaryShape_RealCapturedPOST_LaterRunFullyAdmitted(t *testing.T) {
	baseline := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_post_boundary_baseline_fd651d42.json")
	candidate := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_post_boundary_candidate_7463cdf6.json")

	result := Compare(baseline, candidate, sankeyHotspotParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_RealCapturedGET_EarlierRunStillMatches
// pins the EARLIER of the two production runs, taken a few hours before
// the boundary-crossing one above against the same org: repos held no
// unmerged version at capture time, and the two planes agree outright --
// this shape's own reconstruction must never manufacture a difference
// where the real response carries none.
func TestHotspotListBoundaryShape_RealCapturedGET_EarlierRunStillMatches(t *testing.T) {
	baseline := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_get_match_baseline_79c98052.json")
	candidate := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_get_match_candidate_245ccbb2.json")

	result := Compare(baseline, candidate, sankeyHotspotParity)

	if !result.IsMatch() {
		t.Fatalf("terminal = %q, want match: findings %+v", result.TerminalState, result.Findings)
	}
}

// TestHotspotListBoundaryShape_RealCapturedPOST_EarlierRunStillMatches is
// the POST sibling of the earlier-run match case above.
func TestHotspotListBoundaryShape_RealCapturedPOST_EarlierRunStillMatches(t *testing.T) {
	baseline := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_post_match_baseline_cdbf09ee.json")
	candidate := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_post_match_candidate_59e5527f.json")

	result := Compare(baseline, candidate, sankeyHotspotParity)

	if !result.IsMatch() {
		t.Fatalf("terminal = %q, want match: findings %+v", result.TerminalState, result.Findings)
	}
}

// TestHotspotListBoundaryShape_RealCapturedPOST_TwoLevelBoundaryFullyAdmitted
// pins a THIRD production capture, distinct from the two pairs above: a
// repository (full-chaos/ask-dev) whose OWN two directly-owned directory
// edges read 2.8x and 3.1x -- neither a clean integer ratio, so the
// sibling SankeyRepoFanoutShape entry establishes NO anchor k for it at
// all -- while every one of its four SHARED (non-boundary) files reads an
// identical 2.0x. Twelve of its own files are baseline-only, admitted
// against the response's global candidate minimum, and two of its
// directories ((root) is not one of them; corpus/scripts/tests each lose
// every one of their listed children and vanish outright). Proves rule
// 2's shared-file-ratio source and rule 4's shared-file value admission
// together, not just the parent identity the two pairs above already
// pin.
func TestHotspotListBoundaryShape_RealCapturedPOST_TwoLevelBoundaryFullyAdmitted(t *testing.T) {
	baseline := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_post_step93_baseline_04b49201.json")
	candidate := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_post_step93_candidate_831338a4.json")

	result := Compare(baseline, candidate, sankeyHotspotParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_RealCapturedGET_TeamScopedFullyAdmitted pins
// the SAME mechanism as the two production captures above, but under
// hotspot_team_scoped's OWN Options (sankeyHotspotTeamScopedParity, which
// layers sankeyNodesTeamScopeSubsetDefect/sankeyLinksTeamScopeSubsetDefect
// on top of sankeyHotspotParity's own two entries) -- proving the
// off-Limit relaxation is not needed for THIS production org (team CHAOS
// owns every repository the corpus's own hotspot query can see, so both
// legs still land exactly at maxHotspotRowsCorpus), while the shared-file
// multiplier fix still must fire for the SAME ask-dev fan-out this GET
// capture also carries.
func TestHotspotListBoundaryShape_RealCapturedGET_TeamScopedFullyAdmitted(t *testing.T) {
	baseline := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_team_scoped_get_step93_baseline_3f4d5c05.json")
	candidate := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_get_step93_candidate_afd5dfa5.json")

	result := Compare(baseline, candidate, sankeyHotspotTeamScopedParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_RealCapturedPOST_TeamScopedFullyAdmitted is
// the POST sibling of the team-scoped case above.
func TestHotspotListBoundaryShape_RealCapturedPOST_TeamScopedFullyAdmitted(t *testing.T) {
	baseline := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_org_post_step93_baseline_04b49201.json")
	candidate := hotspotBoundarySnapshotFromFile(t, "testdata/hotspot_team_scoped_post_step93_candidate_4f0a5a7c.json")

	result := Compare(baseline, candidate, sankeyHotspotTeamScopedParity)

	if result.TerminalState != TerminalStateMismatch {
		t.Fatalf("terminal = %q, want mismatch", result.TerminalState)
	}
	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_SharedFileValueAdmittedFromTwoAgreeingFiles is
// RED for the OLD rule 2 (sibling-covered-anchor-edge-only source) AND
// the old admits() (parent-value only): repoX's ONLY directory is dir1,
// and dir1's own repo->directory edge is ITSELF contaminated by a leaver
// (200 vs 80 = 2.5x, not a clean integer ratio) -- exactly the production
// shape (a repository's own directly-owned directory sums a mix of
// doubled shared files plus leavers) -- so the sibling SankeyRepoFanoutShape
// entry establishes NO anchor k for repoX at all; nothing under it is its
// own to admit. repoX's own k=2 is derivable ONLY from TWO independently
// agreeing shared files (dir1/shared.go, 100 vs 50; dir1/shared2.go, 60
// vs 30) -- neither alone, nor any single uncorroborated ratio, may ever
// derive k (see TestSingleUncorroboratedFileRatioNeverDerivesK for the
// negative twin) -- and each shared file's own two link values must be
// admitted directly by rule 4's shared-file half, not merely folded into
// dir1's own parent re-sum (which this fixture also exercises, closing
// at 200).
func TestHotspotListBoundaryShape_SharedFileValueAdmittedFromTwoAgreeingFiles(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoX / dir1/shared2.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / dir1", 200) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 100) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 100) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared2.go", 60) + "," +
		sankeyLink("repoX / dir1/shared2.go", "refactor", 60) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", 40) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", 40) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoX / dir1/shared2.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / dir1", 80) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 50) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 50) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared2.go", 30) + "," +
		sankeyLink("repoX / dir1/shared2.go", "refactor", 30) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900) + "," +
		sankeyLink("repoY", "repoY / dir2", 25) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", 25) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", 25)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(4))

	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- two independently agreeing shared files are the ONLY source available for repoX's multiplier (its own dir1 parent edge is contaminated by leaver1 and never validates alone), and each shared file's own two link values must be admitted directly: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_SingleUncorroboratedFileRatioNeverDerivesK
// is RED for rule 2's own single-file discipline: the IDENTICAL fixture
// to the two-agreeing-files case above, but shared2.go is dropped (only
// dir1/shared.go's own 2.0x ratio remains, uncorroborated by any second
// shared file or by the sibling SankeyRepoFanoutShape, whose own anchor
// edge -- dir1's parent, 140 vs 50 -- is not a clean integer ratio
// either). A single, self-consistent ratio must never derive k on its
// own: leaver1 stays unadmitted, and the otherwise-unrelated repoY
// entrant loses its own one-leaver-per-entrant admission with it.
func TestHotspotListBoundaryShape_SingleUncorroboratedFileRatioNeverDerivesK(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / dir1", 140) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 100) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 100) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", 40) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", 40) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / stable", "directory") + "," +
		sankeyNode("repoY / stable/stable.go", "file") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / dir1", 50) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 50) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 50) + "," +
		sankeyLink("repoY", "repoY / stable", 900) + "," +
		sankeyLink("repoY / stable", "repoY / stable/stable.go", 900) + "," +
		sankeyLink("repoY / stable/stable.go", "refactor", 900) + "," +
		sankeyLink("repoY", "repoY / dir2", 25) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", 25) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", 25)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(3))

	if result.DifferencesOutsideBaselineDefect == 0 {
		t.Fatalf("outside = 0, want > 0 -- a single, uncorroborated shared-file ratio must never derive a repository's multiplier on its own: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_SingleFileCorroboratedBySiblingDerivesK
// pins rule 2's own remaining source-(b) path: dir1's parent edge (140
// vs 70 = clean 2.0x this time) lets the sibling SankeyRepoFanoutShape
// establish repoX's anchor k directly, and dir1's single shared file
// (100 vs 50) independently agrees with that SAME integer -- one shared
// file, corroborated by the sibling, still derives k and admits the
// file's own two link values.
func TestHotspotListBoundaryShape_SingleFileCorroboratedBySiblingDerivesK(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / dir1", 140) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 100) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 100)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / dir1", 70) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 50) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 50)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(1))

	if result.DifferencesOutsideBaselineDefect != 0 {
		t.Fatalf("outside = %d, want 0 -- one shared file corroborated by the sibling's own anchor k must still derive the repository's multiplier: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
}

// TestHotspotListBoundaryShape_DirToFileOwnCheckRejectsMismatch is RED
// for mutation (a): deleting the directory->file link's own k*candidate
// check (hotspotlistboundary.go, the repoFanoutFloatsWithinTolerance call
// admitting cf.dirToFileLinkKey) and admitting it unconditionally once a
// repository multiplier exists. repoX's k=2 is derived from TWO clean
// shared files (a.go, b.go, dir1's own parent edge deliberately
// contaminated by leaver1 so the sibling SankeyRepoFanoutShape
// contributes nothing); t.go is a THIRD shared file under the SAME
// directory whose own directory->file value does NOT satisfy k*candidate
// (90 vs 60*2=120) even though its own file->change_type value DOES
// (200 vs 100*2=200, admitted independently -- proving this test isolates
// the directory->file check alone, not the change_type one). Exactly two
// findings stay outside: t.go's own directory->file value, and dir1's own
// parent value (unable to close, since t.go's own contamination breaks
// the uniform-k re-sum identity too) -- an EXACT count, not merely > 0.
func TestHotspotListBoundaryShape_DirToFileOwnCheckRejectsMismatch(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/a.go", "file") + "," +
		sankeyNode("repoX / dir1/b.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoX / dir1/t.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / dir1", 314) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/a.go", 100) + "," +
		sankeyLink("repoX / dir1/a.go", "refactor", 100) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/b.go", 80) + "," +
		sankeyLink("repoX / dir1/b.go", "refactor", 80) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", 44) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", 44) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/t.go", 90) + "," +
		sankeyLink("repoX / dir1/t.go", "refactor", 200)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/a.go", "file") + "," +
		sankeyNode("repoX / dir1/b.go", "file") + "," +
		sankeyNode("repoX / dir1/t.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / dir1", 150) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/a.go", 50) + "," +
		sankeyLink("repoX / dir1/a.go", "refactor", 50) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/b.go", 40) + "," +
		sankeyLink("repoX / dir1/b.go", "refactor", 40) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/t.go", 60) + "," +
		sankeyLink("repoX / dir1/t.go", "refactor", 100) + "," +
		sankeyLink("repoY", "repoY / dir2", 25) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", 25) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", 25)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(4))

	if result.DifferencesOutsideBaselineDefect != 2 {
		t.Fatalf("outside = %d, want 2 (t.go's own directory->file value, and dir1's own parent value): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawTGoDirToFile := false
	for _, f := range result.Findings {
		if f.Shape == ShapeValue {
			if key, ok := parseOrderInsensitiveDetailKey(f.Detail); ok && key == "repoX / dir1\x1frepoX / dir1/t.go" {
				sawTGoDirToFile = true
			}
		}
	}
	if !sawTGoDirToFile {
		t.Fatalf("expected t.go's own directory->file value finding in the findings set: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_FileToChangeTypeValueCheckedIndependently
// is RED for mutation (b): deleting the file->change_type link's own
// SEPARATE check and admitting it unconditionally once the
// directory->file link admits. dir1's TWO shared files (shared.go,
// shared2.go) both derive repoX's k=2 via rule 2 (dir1's own parent edge,
// 200 vs 80 = 2.5x, is contaminated by leaver1 and never validates
// alone), and shared.go's own directory->file value matches that k
// exactly -- but shared.go's own file->change_type edge carries a
// DIFFERENT, unrelated value pair (970 vs 500) that does NOT satisfy
// k*candidate at all (500*2=1000, not 970). Exactly ONE finding stays
// outside: shared.go's own file->change_type value -- an EXACT count.
func TestHotspotListBoundaryShape_FileToChangeTypeValueCheckedIndependently(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoX / dir1/shared2.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / dir1", 200) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 100) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 970) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared2.go", 60) + "," +
		sankeyLink("repoX / dir1/shared2.go", "refactor", 30) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", 40) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", 40)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/shared.go", "file") + "," +
		sankeyNode("repoX / dir1/shared2.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / dir1", 80) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared.go", 50) + "," +
		sankeyLink("repoX / dir1/shared.go", "refactor", 500) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/shared2.go", 30) + "," +
		sankeyLink("repoX / dir1/shared2.go", "refactor", 15) + "," +
		sankeyLink("repoY", "repoY / dir2", 25) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", 25) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", 25)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(3))

	if result.DifferencesOutsideBaselineDefect != 1 {
		t.Fatalf("outside = %d, want 1 (shared.go's own file->change_type value, 970 vs 500): findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawSharedGoChangeType := false
	for _, f := range result.Findings {
		if f.Shape == ShapeValue {
			if key, ok := parseOrderInsensitiveDetailKey(f.Detail); ok && key == "repoX / dir1/shared.go\x1frefactor" {
				sawSharedGoChangeType = true
			}
		}
	}
	if !sawSharedGoChangeType {
		t.Fatalf("expected shared.go's own file->change_type value finding in the findings set: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_SiblingDisagreementIgnoredStaysOutside is
// RED for mutation (c): deleting the disagreement guard between source
// (a) (the sibling's own covered anchor edge) and source (b) (two
// agreeing shared files) and setting the repository's multiplier from
// source (b) alone. repoX has an "anchor" directory with NO file
// children (so it can never itself pollute fileKs) whose own direct edge
// reads a clean 210 vs 70 = 3.0x -- the sibling SankeyRepoFanoutShape
// establishes k=3 and admits that ONE edge. dir1's own two shared files
// (a.go, b.go) independently agree on a DIFFERENT k=2. The two sources
// disagree, so repoX's multiplier must never be established at all:
// EXACTLY 13 findings stay outside (every leaver/entrant/parent/file
// consequence under dir1, none admitted), and a.go's own directory->file
// value is among them.
func TestHotspotListBoundaryShape_SiblingDisagreementIgnoredStaysOutside(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / anchor", "directory") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/a.go", "file") + "," +
		sankeyNode("repoX / dir1/b.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / anchor", 210) + "," +
		sankeyLink("repoX", "repoX / dir1", 224) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/a.go", 100) + "," +
		sankeyLink("repoX / dir1/a.go", "refactor", 100) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/b.go", 80) + "," +
		sankeyLink("repoX / dir1/b.go", "refactor", 80) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", 44) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", 44)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / anchor", "directory") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/a.go", "file") + "," +
		sankeyNode("repoX / dir1/b.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / anchor", 70) + "," +
		sankeyLink("repoX", "repoX / dir1", 90) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/a.go", 50) + "," +
		sankeyLink("repoX / dir1/a.go", "refactor", 50) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/b.go", 40) + "," +
		sankeyLink("repoX / dir1/b.go", "refactor", 40) + "," +
		sankeyLink("repoY", "repoY / dir2", 25) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", 25) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", 25)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(3))

	if result.DifferencesOutsideBaselineDefect != 13 {
		t.Fatalf("outside = %d, want 13: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawAGoDirToFile := false
	for _, f := range result.Findings {
		if f.Shape == ShapeValue {
			if key, ok := parseOrderInsensitiveDetailKey(f.Detail); ok && key == "repoX / dir1\x1frepoX / dir1/a.go" {
				sawAGoDirToFile = true
			}
		}
	}
	if !sawAGoDirToFile {
		t.Fatalf("expected a.go's own directory->file value finding in the findings set: findings %+v", result.Findings)
	}
}

// TestHotspotListBoundaryShape_NonIntegerRatioNeverDerivesKEvenWithAgreement
// is RED for mutation (d): computing a file's own ratio as the NEAREST
// integer with no tolerance check at all, in place of
// repoFanoutIntegerMultiplier. TWO shared files both read the SAME
// non-integer ratio (a.go 160/100, b.go 136/85, both exactly 1.6) --
// naive nearest-integer rounding would round 1.6 to a MEANINGFUL k=2
// (able to admit leaver1 below); repoFanoutIntegerMultiplier's own
// tolerance instead rejects 1.6 outright (nowhere near clean), so this
// repository never derives ANY k, agreeing samples or not. Exactly 11
// findings stay outside (every leaver/entrant/parent/file consequence
// under dir1), and a.go's own directory->file value is among them.
func TestHotspotListBoundaryShape_NonIntegerRatioNeverDerivesKEvenWithAgreement(t *testing.T) {
	baseNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/a.go", "file") + "," +
		sankeyNode("repoX / dir1/b.go", "file") + "," +
		sankeyNode("repoX / dir1/leaver1.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	baseLinks := sankeyLink("repoX", "repoX / dir1", 336) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/a.go", 160) + "," +
		sankeyLink("repoX / dir1/a.go", "refactor", 160) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/b.go", 136) + "," +
		sankeyLink("repoX / dir1/b.go", "refactor", 136) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/leaver1.go", 40) + "," +
		sankeyLink("repoX / dir1/leaver1.go", "refactor", 40)

	candNodes := sankeyNode("repoX", "repo") + "," +
		sankeyNode("repoX / dir1", "directory") + "," +
		sankeyNode("repoX / dir1/a.go", "file") + "," +
		sankeyNode("repoX / dir1/b.go", "file") + "," +
		sankeyNode("repoY", "repo") + "," +
		sankeyNode("repoY / dir2", "directory") + "," +
		sankeyNode("repoY / dir2/entrant1.go", "file") + "," +
		sankeyNode("refactor", "change_type")
	candLinks := sankeyLink("repoX", "repoX / dir1", 185) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/a.go", 100) + "," +
		sankeyLink("repoX / dir1/a.go", "refactor", 100) + "," +
		sankeyLink("repoX / dir1", "repoX / dir1/b.go", 85) + "," +
		sankeyLink("repoX / dir1/b.go", "refactor", 85) + "," +
		sankeyLink("repoY", "repoY / dir2", 25) + "," +
		sankeyLink("repoY / dir2", "repoY / dir2/entrant1.go", 25) + "," +
		sankeyLink("repoY / dir2/entrant1.go", "refactor", 25)

	baseline := sankeyBody(baseNodes, baseLinks)
	candidate := sankeyBody(candNodes, candLinks)

	result := Compare(snapshotFromJSON(t, baseline), snapshotFromJSON(t, candidate), hotspotBoundaryOptions(3))

	if result.DifferencesOutsideBaselineDefect != 11 {
		t.Fatalf("outside = %d, want 11: findings %+v", result.DifferencesOutsideBaselineDefect, result.Findings)
	}
	sawAGoDirToFile := false
	for _, f := range result.Findings {
		if f.Shape == ShapeValue {
			if key, ok := parseOrderInsensitiveDetailKey(f.Detail); ok && key == "repoX / dir1\x1frepoX / dir1/a.go" {
				sawAGoDirToFile = true
			}
		}
	}
	if !sawAGoDirToFile {
		t.Fatalf("expected a.go's own directory->file value finding in the findings set: findings %+v", result.Findings)
	}
}
