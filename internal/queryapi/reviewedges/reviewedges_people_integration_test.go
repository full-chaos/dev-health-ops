//go:build integration

package reviewedges

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-8485 on a real ClickHouse and the migrated schema. The unit tests pin
// the rules with scripted rows; only an executed statement shows that the two
// reads of people.go run against the real tables (identities FINAL with its
// JSON column, git_pull_requests FINAL with the tuple argMax) and scan into
// the Go types.
//
// The fixture's edges are rev-N > auth-N strings. This test adds, in the
// fixture's org:
//   - an identity for rev-1 (a provider login) with a display name;
//   - one edge whose author is stored by e-mail address, with an identity
//     that has NO display name, and two pull requests of that address (the
//     newer one names the author);
//   - one edge whose author is an e-mail address nobody knows;
//   - an inactive identity and another org's identity for rev-2, which must
//     not name anybody.
func TestPeople_NamesAndKeysOnARealStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f := newTeamScopeFixture(ctx, t)

	updated := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	identityOf := func(org, canonicalID string, displayName, email *string, providerIdentities string, active uint8) {
		t.Helper()
		if err := f.admin.Exec(ctx, `INSERT INTO identities (org_id, canonical_id, identity_uuid, display_name, email, provider_identities, team_ids, is_active, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			org, canonicalID, uuid.New(), displayName, email, providerIdentities, []string{}, active, updated); err != nil {
			t.Fatalf("seed identity %s: %v", canonicalID, err)
		}
	}
	text := func(value string) *string { return &value }
	identityOf(teamScopeOrg, "rev-one", text("Rev One"), nil, `{"github": ["rev-1"]}`, 1)
	identityOf(teamScopeOrg, "pat", nil, text("Pat@Corp.Example"), `{"linear": ["usr_7"]}`, 1)
	identityOf(teamScopeOrg, "rev-two-gone", text("Gone Person"), nil, `{"github": ["rev-2"]}`, 0)
	identityOf(teamScopeOther, "rev-two-elsewhere", text("Elsewhere Person"), nil, `{"github": ["rev-2"]}`, 1)

	computed := time.Date(2026, 9, 1, 6, 0, 0, 0, time.UTC)
	edge := func(day int, reviewer, author string, count uint32) {
		t.Helper()
		if err := f.admin.Exec(ctx, `INSERT INTO review_edges_daily (repo_id, day, reviewer, author, reviews_count, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			f.repoA, time.Date(2026, 8, day, 0, 0, 0, 0, time.UTC), reviewer, author, count, computed, teamScopeOrg); err != nil {
			t.Fatalf("seed edge: %v", err)
		}
	}
	edge(10, "rev-1", "pat@corp.example", 500)
	edge(11, "rev-1", "ghost@nowhere.example", 400)

	pullRequest := func(number uint32, authorName, authorEmail string, created time.Time) {
		t.Helper()
		if err := f.admin.Exec(ctx, `INSERT INTO git_pull_requests (repo_id, number, author_name, author_email, created_at, last_synced, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			f.repoA, number, authorName, authorEmail, created, created, teamScopeOrg); err != nil {
			t.Fatalf("seed pull request %d: %v", number, err)
		}
	}
	pullRequest(1, "Old Name", "pat@corp.example", time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
	pullRequest(2, "Pat Rivers", "PAT@corp.example", time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC))

	got, err := ResolveScoped(ctx, f.client, teamScopeOrg, mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), Scope{AsOf: teamScopeAsOf}, 100)
	if err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	name := func(value *string) string {
		if value == nil {
			return "<nil>"
		}
		return *value
	}
	byPair := map[string]int{}
	for index, row := range got.Edges {
		byPair[row.Reviewer+">"+row.Author] = index
		for _, served := range []string{name(row.ReviewerName), name(row.AuthorName), row.ReviewerKey, row.AuthorKey} {
			if strings.Contains(served, "@") {
				t.Errorf("%s > %s: a new field holds an e-mail address: %q", row.Reviewer, row.Author, served)
			}
		}
		if !strings.HasPrefix(row.ReviewerKey, "p_") || !strings.HasPrefix(row.AuthorKey, "p_") {
			t.Errorf("%s > %s: keys %q / %q are not opaque keys", row.Reviewer, row.Author, row.ReviewerKey, row.AuthorKey)
		}
	}
	row := func(pair string) (index int) {
		index, ok := byPair[pair]
		if !ok {
			t.Fatalf("no row %s in %v", pair, byPair)
		}
		return index
	}

	pat := got.Edges[row("rev-1>pat@corp.example")]
	if name(pat.ReviewerName) != "Rev One" {
		t.Errorf("reviewer name = %s, want the identity's display name Rev One", name(pat.ReviewerName))
	}
	// The identity has no display name; the newest pull request of the address
	// (matched without regard to case) names the author.
	if name(pat.AuthorName) != "Pat Rivers" {
		t.Errorf("author name = %s, want the newest pull request author name Pat Rivers", name(pat.AuthorName))
	}

	ghost := got.Edges[row("rev-1>ghost@nowhere.example")]
	if ghost.AuthorName != nil {
		t.Errorf("author name = %s for an address nobody knows, want none", name(ghost.AuthorName))
	}
	if ghost.ReviewerKey != pat.ReviewerKey {
		t.Error("one reviewer has two keys in two rows")
	}
	if ghost.AuthorKey == pat.AuthorKey {
		t.Error("two authors have one key")
	}

	// rev-2 has an INACTIVE identity in this org and an identity in another
	// org: neither names it. Its name is the stored login.
	other := got.Edges[row("rev-2>auth-2")]
	if name(other.ReviewerName) != "rev-2" || name(other.AuthorName) != "auth-2" {
		t.Errorf("names = %s / %s, want the stored logins rev-2 / auth-2", name(other.ReviewerName), name(other.AuthorName))
	}
}
