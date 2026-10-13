//go:build integration

package providersync

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ownershipRowSpan is one stored inferred row of a repository: its team, role
// and interval.
type ownershipRowSpan struct {
	team      string
	primary   uint8
	validFrom time.Time
	validTo   *time.Time
}

func ownershipRowSpans(t *testing.T, ctx context.Context, conn driver.Conn, org string) []ownershipRowSpan {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT team_id, is_primary, valid_from, valid_to FROM team_repo_ownership FINAL
WHERE org_id = ? AND source = 'inferred' ORDER BY valid_from, team_id`, org)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []ownershipRowSpan
	for rows.Next() {
		var span ownershipRowSpan
		if err := rows.Scan(&span.team, &span.primary, &span.validFrom, &span.validTo); err != nil {
			t.Fatal(err)
		}
		out = append(out, span)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// mainOwnersAt is the teams whose main row (is_primary = 1) covers the instant.
func mainOwnersAt(spans []ownershipRowSpan, at time.Time) []string {
	var teams []string
	for _, span := range spans {
		if span.primary != teamRepoOwnershipMainOwner || at.Before(span.validFrom) || (span.validTo != nil && !at.Before(*span.validTo)) {
			continue
		}
		teams = append(teams, span.team)
	}
	return teams
}

// TestAWinnerChangeIsDatedAtTheRunWithOneMainOwnerAtEveryInstant: team A is
// the main owner from the repository's earliest evidence and team B a
// co-owner; B then takes the lead. The change is dated at the run that
// observes it: A's main row ends at that run, B's main row starts there, A
// continues as a co-owner from there with its share, and B's co-owner row ends
// there. At every instant of the history the repository has exactly one main
// row, the shared one-owner read names A before the change and B after it, and
// a rerun writes nothing.
func TestAWinnerChangeIsDatedAtTheRunWithOneMainOwnerAtEveryInstant(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	old := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	mid := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)

	for _, provider := range evidenceSettleProviders {
		t.Run(provider, func(t *testing.T) {
			org := "org-winner-change-" + provider
			teamA, teamB := provider+"-team-a", provider+"-team-b"
			repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-a", teamA, true, time.Now().UTC().Add(-time.Hour))
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-b", teamB, true, time.Now().UTC().Add(-time.Hour))
			for n := uint32(1); n <= 3; n++ {
				seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", n, old.Add(time.Duration(n-1)*time.Hour))
			}
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 4, mid)

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("first derive: ready %v err %v", ready, err)
			}

			for n := uint32(5); n <= 7; n++ {
				seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", n, mid.Add(time.Duration(n-4)*time.Hour))
			}
			before := time.Now().UTC().Add(-time.Second)
			_, retracted, _, _, stats, err := service.DeriveWithStats(ctx, org)
			if err != nil {
				t.Fatal(err)
			}
			after := time.Now().UTC().Add(time.Second)
			if retracted != 0 || stats.RoleChanges != 2 {
				t.Fatalf("winner change: retracted %d role changes %d, want 0 and 2 (A main -> co-owner, B co-owner -> main)", retracted, stats.RoleChanges)
			}

			spans := ownershipRowSpans(t, ctx, conn, org)
			var change time.Time
			for _, span := range spans {
				if span.team == teamB && span.primary == teamRepoOwnershipMainOwner && span.validTo == nil {
					change = span.validFrom
				}
			}
			if change.Before(before) || change.After(after) {
				t.Fatalf("B's main row starts at %v, want the run (%v..%v); rows %+v", change, before, after, spans)
			}
			want := map[string]bool{
				fmt.Sprintf("%s:1:%s:%s", teamA, old, change): true,
				fmt.Sprintf("%s:2:%s:-", teamA, change):       true,
				fmt.Sprintf("%s:2:%s:%s", teamB, mid, change): true,
				fmt.Sprintf("%s:1:%s:-", teamB, change):       true,
			}
			for _, span := range spans {
				to := "-"
				if span.validTo != nil {
					to = span.validTo.UTC().String()
				}
				key := fmt.Sprintf("%s:%d:%s:%s", span.team, span.primary, span.validFrom.UTC(), to)
				if !want[key] {
					t.Errorf("unexpected row %s (rows %+v)", key, spans)
				}
				delete(want, key)
			}
			for key := range want {
				t.Errorf("missing row %s", key)
			}

			for _, at := range []time.Time{old, old.Add(time.Minute), mid, mid.Add(time.Minute), change.Add(-time.Millisecond), change, change.Add(time.Millisecond), after} {
				if mains := mainOwnersAt(spans, at); len(mains) != 1 {
					t.Errorf("main owners at %v = %v, want exactly one", at, mains)
				}
			}
			for at, wantTeam := range map[time.Time]string{mid.Add(time.Minute): teamA, change.Add(-time.Millisecond): teamA, change.Add(time.Millisecond): teamB} {
				owners, err := joinedOwnersByRepo(ctx, conn, org, at)
				if err != nil {
					t.Fatal(err)
				}
				if got := owners[repo.String()]; got != wantTeam {
					t.Errorf("one owner at %v = %q, want %q", at, got, wantTeam)
				}
			}

			for run := 1; run <= 2; run++ {
				written, retracted, _, _, stats, err := service.DeriveWithStats(ctx, org)
				if err != nil {
					t.Fatal(err)
				}
				if written != 0 || retracted != 0 || stats.Superseded != 0 || stats.RoleChanges != 0 {
					t.Errorf("rerun %d: written %d retracted %d superseded %d role changes %d, want all 0", run, written, retracted, stats.Superseded, stats.RoleChanges)
				}
			}
		})
	}
}

// TestACoOwnerRetractionNeverMovesTheMainOwner: a co-owner owns the repository
// beside the main owner, so the end of a co-owner's row is no end of a main
// tenure. When co-owner B loses its links and is retracted, main owner A keeps
// its start at the repository's earliest evidence on every later run.
func TestACoOwnerRetractionNeverMovesTheMainOwner(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	old := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	mid := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)

	for _, provider := range evidenceSettleProviders {
		t.Run(provider, func(t *testing.T) {
			org := "org-coowner-retraction-" + provider
			teamA, teamB := provider+"-team-a", provider+"-team-b"
			repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-a", teamA, true, time.Now().UTC().Add(-time.Hour))
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-b", teamB, true, time.Now().UTC().Add(-time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 1, old)
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 2, old.Add(time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 3, mid)

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			if _, _, ready, _, err := service.Derive(ctx, org); err != nil || !ready {
				t.Fatalf("first derive: ready %v err %v", ready, err)
			}
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 3, mid)
			if _, retracted, _, _, err := service.Derive(ctx, org); err != nil || retracted != 1 {
				t.Fatalf("B loses its links: retracted %d err %v, want B's row retracted", retracted, err)
			}
			for run := 1; run <= 2; run++ {
				written, retracted, _, _, stats, err := service.DeriveWithStats(ctx, org)
				if err != nil {
					t.Fatal(err)
				}
				if written != 0 || retracted != 0 || stats.Superseded != 0 || stats.RoleChanges != 0 {
					t.Errorf("run %d after the retraction: written %d retracted %d superseded %d role changes %d, want all 0", run, written, retracted, stats.Superseded, stats.RoleChanges)
				}
			}
			if starts := openOwnershipStarts(t, ctx, conn, org, teamA); len(starts) != 1 || !starts[0].Equal(old) {
				t.Errorf("A open rows start at %v, want one at the repository's earliest evidence %v", starts, old)
			}
		})
	}
}

// TestAFullTieThatFormsAndEndsIsDatedAtTheRuns: main owner A holds the
// repository from its earliest evidence. Co-owner B draws level, so both are
// main owners (D5990): B's main row starts at that run. A then takes the lead
// again, so B continues as a co-owner from that run. A's main row is never
// moved (B's main tenure ends while B still owns the repository), every
// instant's main rows are the leaders of that instant, and reruns write
// nothing.
func TestAFullTieThatFormsAndEndsIsDatedAtTheRuns(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	old := time.Date(2025, 3, 4, 5, 6, 7, 0, time.UTC)
	mid := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)

	for _, provider := range evidenceSettleProviders {
		t.Run(provider, func(t *testing.T) {
			org := "org-tie-forms-ends-" + provider
			teamA, teamB := provider+"-team-a", provider+"-team-b"
			repo := seedEvidenceSettleRepo(t, ctx, conn, org, provider)
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-a", teamA, true, time.Now().UTC().Add(-time.Hour))
			seedTeamProjectOwnership(t, ctx, conn, org, provider, "proj-b", teamB, true, time.Now().UTC().Add(-time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 1, old)
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 2, old.Add(time.Hour))
			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 3, mid)

			service := TeamRepoOwnershipDerivationService{Conn: conn}
			derive := func(step string) TeamRepoOwnershipDerivationStats {
				t.Helper()
				_, _, ready, _, stats, err := service.DeriveWithStats(ctx, org)
				if err != nil || !ready {
					t.Fatalf("%s: ready %v err %v", step, ready, err)
				}
				return stats
			}
			derive("first derive")

			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-b", 4, mid.Add(time.Hour))
			tieFrom := time.Now().UTC().Add(-time.Second)
			if stats := derive("the tie forms"); stats.RoleChanges != 1 || len(stats.Ties) != 1 {
				t.Fatalf("the tie forms: role changes %d ties %+v, want B's co-owner row closed and one tie", stats.RoleChanges, stats.Ties)
			}

			seedOwnershipEvidence(t, ctx, conn, org, provider, repo, "proj-a", 5, mid.Add(2*time.Hour))
			tieUntil := time.Now().UTC().Add(-time.Second)
			if stats := derive("the tie ends"); stats.RoleChanges != 1 || len(stats.Ties) != 0 {
				t.Fatalf("the tie ends: role changes %d ties %+v, want B's main row closed and no tie", stats.RoleChanges, stats.Ties)
			}

			spans := ownershipRowSpans(t, ctx, conn, org)
			var bMainFrom, bMainTo time.Time
			for _, span := range spans {
				if span.team == teamA && (!span.validFrom.Equal(old) || span.primary != teamRepoOwnershipMainOwner || span.validTo != nil) {
					t.Errorf("A row %+v, want only its one open main row from %v", span, old)
				}
				if span.team == teamB && span.primary == teamRepoOwnershipMainOwner && span.validTo != nil {
					bMainFrom, bMainTo = span.validFrom, *span.validTo
				}
			}
			if bMainFrom.Before(tieFrom) || !bMainTo.After(bMainFrom) || bMainTo.Before(tieUntil) {
				t.Fatalf("B main row %v..%v, want from the run of the tie (after %v) to the run that ended it (after %v); rows %+v", bMainFrom, bMainTo, tieFrom, tieUntil, spans)
			}
			for at, want := range map[time.Time][]string{
				mid:                              {teamA},
				bMainFrom.Add(-time.Millisecond): {teamA},
				bMainFrom:                        {teamA, teamB},
				bMainTo.Add(-time.Millisecond):   {teamA, teamB},
				bMainTo:                          {teamA},
			} {
				if got := mainOwnersAt(spans, at); fmt.Sprint(sortedStrings(got)) != fmt.Sprint(sortedStrings(want)) {
					t.Errorf("main owners at %v = %v, want %v", at, got, want)
				}
			}
			for run := 1; run <= 2; run++ {
				written, retracted, _, _, stats, err := service.DeriveWithStats(ctx, org)
				if err != nil {
					t.Fatal(err)
				}
				if written != 0 || retracted != 0 || stats.Superseded != 0 || stats.RoleChanges != 0 {
					t.Errorf("rerun %d: written %d retracted %d superseded %d role changes %d, want all 0", run, written, retracted, stats.Superseded, stats.RoleChanges)
				}
			}
		})
	}
}
