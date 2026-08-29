package providersync

import "testing"

// This file pins the codex review finding (mirrored from
// GitHubTeamCatalogCollector's CHAOS-4461-class fix, team-lead relay
// 2026-08-28): writeTeams' roster-preservation read must be SCOPED to only
// the rows that actually need it (MembersAuthoritative == false), and a
// failure of that read must exclude only THOSE rows from the write, never
// every other, self-sufficient team in the same batch.

// TestGitLabTeamsNeedingRosterPreservationOnlyIncludesNonAuthoritativeRows
// proves the scoping helper: only MembersAuthoritative=false rows are
// included -- a self-sufficient (MembersAuthoritative=true) row never
// triggers or depends on the preservation read at all.
func TestGitLabTeamsNeedingRosterPreservationOnlyIncludesNonAuthoritativeRows(t *testing.T) {
	rows := []gitlabTeamCatalogTeamRow{
		{ID: "gl:org", MembersAuthoritative: true},
		{ID: "gl:org/team-a", MembersAuthoritative: false},
		{ID: "gl:org/team-b", MembersAuthoritative: false},
	}
	ids := gitlabTeamsNeedingRosterPreservation(rows)
	if len(ids) != 2 || ids[0] != "gl:org/team-a" || ids[1] != "gl:org/team-b" {
		t.Fatalf("ids=%v, want [gl:org/team-a gl:org/team-b]", ids)
	}
}

// TestGitLabTeamsNeedingRosterPreservationEmptyWhenAllAuthoritative proves
// the zero case: a fully-authoritative batch needs no preservation read at
// all.
func TestGitLabTeamsNeedingRosterPreservationEmptyWhenAllAuthoritative(t *testing.T) {
	rows := []gitlabTeamCatalogTeamRow{
		{ID: "gl:org", MembersAuthoritative: true},
		{ID: "gl:org/team-a", MembersAuthoritative: true},
	}
	if ids := gitlabTeamsNeedingRosterPreservation(rows); len(ids) != 0 {
		t.Fatalf("ids=%v, want empty", ids)
	}
}

// TestGitLabTeamsSafeToWriteAfterRosterPreservationFailureExcludesNonAuthoritativeRows
// proves the failure-path filter: a preservation-read failure must exclude
// ONLY the MembersAuthoritative=false rows from the write -- every
// self-sufficient row survives.
func TestGitLabTeamsSafeToWriteAfterRosterPreservationFailureExcludesNonAuthoritativeRows(t *testing.T) {
	rows := []gitlabTeamCatalogTeamRow{
		{ID: "gl:org", MembersAuthoritative: true},
		{ID: "gl:org/team-a", MembersAuthoritative: false},
	}
	kept := gitlabTeamsSafeToWriteAfterRosterPreservationFailure(rows)
	if len(kept) != 1 || kept[0].ID != "gl:org" {
		t.Fatalf("kept=%+v, want only gl:org", kept)
	}
}
