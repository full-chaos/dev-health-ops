package reviewedges

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-8485: each row serves a display name and an opaque key for the
// reviewer and for the author, and no e-mail address in either.

var peopleDay = time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)

func edgeRow(reviewer, author string) []any {
	return []any{reviewer, author, uint32(1), peopleDay, "repo-a"}
}

// identityRow is one row of the identities read: identity_uuid, canonical_id,
// email, display_name, provider_identities (JSON text).
func identityRow(uuid, canonicalID, email, displayName, providerIdentities string) []any {
	return []any{uuid, canonicalID, email, displayName, providerIdentities}
}

func resolvePeople(t *testing.T, client *fakeClient, orgID string) []model.ReviewEdgeRow {
	t.Helper()
	got, err := ResolveScoped(context.Background(), client, orgID, mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), Scope{}, 100)
	if err != nil {
		t.Fatalf("ResolveScoped: %v", err)
	}
	return got.Edges
}

func nameOf(name *string) string {
	if name == nil {
		return "<nil>"
	}
	return *name
}

var personKeyForm = regexp.MustCompile(`^p_[0-9a-f]{20}$`)

// One identity per provider, each reached the way that provider's data is
// stored: a login in provider_identities (github, gitlab), the identity's
// e-mail address for an author stored by e-mail (jira-linked), and a pull
// request author name when the identity has no display name (linear).
func TestPeople_NamesAndKeysAcrossTheProviders(t *testing.T) {
	client := &fakeClient{
		response: &fakeRowScanner{rows: [][]any{
			edgeRow("octo-jane", "jane@example.com"),     // github: login reviewer, the SAME person as author by e-mail
			edgeRow("gl.sam", "octo-jane"),               // gitlab login reviewer; author stored by name (no e-mail on the PR)
			edgeRow("octo-jane", "ravi@corp.example"),    // jira-linked: author by the identity's e-mail
			edgeRow("gl.sam", "li@corp.example"),         // linear: identity with no display name, name from the pull request
			edgeRow("drive-by", "ghost@nowhere.example"), // nobody knows these two
			edgeRow("gl.sam", "unknown"),                 // the writer's placeholder
		}},
		identities: [][]any{
			identityRow("00000000-0000-0000-0000-000000000001", "jane", "jane@example.com", "Jane Doe", `{"github": ["octo-jane"]}`),
			identityRow("00000000-0000-0000-0000-000000000002", "sam", "", "Sam Lee", `{"gitlab": ["gl.sam"]}`),
			identityRow("00000000-0000-0000-0000-000000000003", "ravi", "Ravi@Corp.Example", "Ravi K", `{"jira": ["acc-123"]}`),
			identityRow("00000000-0000-0000-0000-000000000004", "li", "li@corp.example", "", `{"linear": ["usr_9"]}`),
		},
		authorNames: [][]any{{"li@corp.example", "Li Wei"}},
	}
	edges := resolvePeople(t, client, "org-1")
	if len(edges) != 6 {
		t.Fatalf("edges = %d, want 6", len(edges))
	}

	want := []struct{ reviewerName, authorName string }{
		{"Jane Doe", "Jane Doe"},
		{"Sam Lee", "Jane Doe"},
		{"Jane Doe", "Ravi K"},
		{"Sam Lee", "Li Wei"},
		{"drive-by", "<nil>"},
		{"Sam Lee", "<nil>"},
	}
	for i, edge := range edges {
		if nameOf(edge.ReviewerName) != want[i].reviewerName || nameOf(edge.AuthorName) != want[i].authorName {
			t.Errorf("edge %d (%s > %s): names = %s / %s, want %s / %s", i, edge.Reviewer, edge.Author,
				nameOf(edge.ReviewerName), nameOf(edge.AuthorName), want[i].reviewerName, want[i].authorName)
		}
		for _, served := range []string{nameOf(edge.ReviewerName), nameOf(edge.AuthorName), edge.ReviewerKey, edge.AuthorKey} {
			if strings.Contains(served, "@") {
				t.Errorf("edge %d: a new field holds an e-mail address: %q", i, served)
			}
		}
		for _, key := range []string{edge.ReviewerKey, edge.AuthorKey} {
			if !personKeyForm.MatchString(key) {
				t.Errorf("edge %d: key %q is not an opaque key", i, key)
			}
		}
	}

	// One person, one key: Jane as reviewer (by login), as author by e-mail
	// and as author by login.
	jane := edges[0].ReviewerKey
	if edges[0].AuthorKey != jane || edges[1].AuthorKey != jane || edges[2].ReviewerKey != jane {
		t.Errorf("Jane has more than one key: %s, %s, %s, %s", jane, edges[0].AuthorKey, edges[1].AuthorKey, edges[2].ReviewerKey)
	}
	// Different people, different keys.
	keys := map[string]string{
		"jane": jane, "sam": edges[1].ReviewerKey, "ravi": edges[2].AuthorKey, "li": edges[3].AuthorKey,
		"drive-by": edges[4].ReviewerKey, "ghost": edges[4].AuthorKey, "unknown": edges[5].AuthorKey,
	}
	seen := map[string]string{}
	for person, key := range keys {
		if other, ok := seen[key]; ok {
			t.Errorf("%s and %s have the same key %s", person, other, key)
		}
		seen[key] = person
	}

	// Two reads for the people, whatever the number of rows; the pull request
	// read asks only for the e-mail authors that had no name: Li (no display
	// name) and the ghost (no identity). Jane and Ravi have one already.
	if client.identitiesCalls != 1 || client.authorNamesCalls != 1 {
		t.Errorf("identity reads = %d, author name reads = %d, want 1 and 1", client.identitiesCalls, client.authorNamesCalls)
	}
	emails := bindingValues(client.authorNamesBindings)["emails"]
	if !reflect.DeepEqual(emails, []string{"ghost@nowhere.example", "li@corp.example"}) {
		t.Errorf("author name read asked for %v, want the two e-mail authors without a name", emails)
	}
	// The old fields keep their meaning (CHAOS-8485 is additive).
	if edges[0].Reviewer != "octo-jane" || edges[0].Author != "jane@example.com" {
		t.Errorf("the stored identities changed: %q / %q", edges[0].Reviewer, edges[0].Author)
	}
}

func TestPeople_AValueThatCouldBeAnAddressIsNeverAName(t *testing.T) {
	client := &fakeClient{
		response: &fakeRowScanner{rows: [][]any{
			edgeRow("mallory@evil.example", "a@x.example"), // a reviewer stored as an address; an author whose only names are addresses
			edgeRow("octo-jane", "b@x.example"),            // an identity whose display name is an address, with a login
		}},
		identities: [][]any{
			identityRow("00000000-0000-0000-0000-000000000001", "a@x.example", "a@x.example", "a@x.example", `{}`),
			identityRow("00000000-0000-0000-0000-000000000002", "jane", "", "jane@example.com", `{"github": ["octo-jane"]}`),
		},
		authorNames: [][]any{{"a@x.example", "also@an.address"}},
	}
	edges := resolvePeople(t, client, "org-1")
	if got := nameOf(edges[0].ReviewerName); got != "<nil>" {
		t.Errorf("a reviewer stored as an address has the name %q, want none", got)
	}
	if got := nameOf(edges[0].AuthorName); got != "<nil>" {
		t.Errorf("an author whose display name and pull request name are addresses has the name %q, want none", got)
	}
	// The identity's display name is an address, so the next candidate is
	// taken: the stored login.
	if got := nameOf(edges[1].ReviewerName); got != "octo-jane" {
		t.Errorf("reviewer name = %q, want the stored login octo-jane", got)
	}
}

// Two identities with one display name: the name alone belongs to neither, so
// a row stored under it resolves to no identity (its key is the row's own).
func TestPeople_AnAmbiguousValueResolvesToNoIdentity(t *testing.T) {
	client := &fakeClient{
		response: &fakeRowScanner{rows: [][]any{edgeRow("Alex", "alex.one"), edgeRow("drive-by", "alex.two")}},
		identities: [][]any{
			identityRow("00000000-0000-0000-0000-000000000001", "alex1", "", "Alex", `{"github": ["alex.one"]}`),
			identityRow("00000000-0000-0000-0000-000000000002", "alex2", "", "Alex", `{"github": ["alex.two"]}`),
		},
	}
	edges := resolvePeople(t, client, "org-1")
	if nameOf(edges[0].ReviewerName) != "Alex" {
		t.Errorf("reviewer name = %s, want the stored Alex", nameOf(edges[0].ReviewerName))
	}
	if edges[0].ReviewerKey == edges[0].AuthorKey || edges[0].ReviewerKey == edges[1].AuthorKey {
		t.Errorf("the ambiguous reviewer got the key of one of the two identities: %s", edges[0].ReviewerKey)
	}
	if edges[0].AuthorKey == edges[1].AuthorKey {
		t.Error("two different identities have one key")
	}
}

func TestPeople_AKeyIsStableAndBelongsToItsOrg(t *testing.T) {
	script := func() *fakeClient {
		return &fakeClient{
			response:   &fakeRowScanner{rows: [][]any{edgeRow("octo-jane", "ghost@nowhere.example")}},
			identities: [][]any{identityRow("00000000-0000-0000-0000-000000000001", "jane", "", "Jane Doe", `{"github": ["octo-jane"]}`)},
		}
	}
	first := resolvePeople(t, script(), "org-1")[0]
	again := resolvePeople(t, script(), "org-1")[0]
	other := resolvePeople(t, script(), "org-2")[0]
	if first.ReviewerKey != again.ReviewerKey || first.AuthorKey != again.AuthorKey {
		t.Error("the keys of two answers of one org differ")
	}
	if first.ReviewerKey == other.ReviewerKey || first.AuthorKey == other.AuthorKey {
		t.Error("a key is the same in two orgs")
	}
}

func TestPeople_NoRowsMakeNoPeopleRead(t *testing.T) {
	client := &fakeClient{response: &fakeRowScanner{rows: nil}}
	if edges := resolvePeople(t, client, "org-1"); len(edges) != 0 {
		t.Fatalf("edges = %d, want 0", len(edges))
	}
	if client.identitiesCalls != 0 || client.authorNamesCalls != 0 {
		t.Errorf("identity reads = %d, author name reads = %d for no rows, want 0 and 0", client.identitiesCalls, client.authorNamesCalls)
	}
}

// No author stored by e-mail without a name: the pull request read is not made.
func TestPeople_ThePullRequestReadIsMadeOnlyWhenAnAuthorNeedsIt(t *testing.T) {
	client := &fakeClient{
		response:   &fakeRowScanner{rows: [][]any{edgeRow("octo-jane", "gl.sam"), edgeRow("gl.sam", "jane@example.com")}},
		identities: [][]any{identityRow("00000000-0000-0000-0000-000000000001", "jane", "jane@example.com", "Jane Doe", `{"github": ["octo-jane"]}`)},
	}
	resolvePeople(t, client, "org-1")
	if client.authorNamesCalls != 0 {
		t.Errorf("author name reads = %d, want 0: no e-mail author lacks a name", client.authorNamesCalls)
	}
}

// A failed read of the people is a failed request, not rows with "no name".
func TestPeople_AFailedReadFailsTheRequest(t *testing.T) {
	boom := errors.New("clickhouse: read failed")
	for name, client := range map[string]*fakeClient{
		"identities": {
			response:      &fakeRowScanner{rows: [][]any{edgeRow("octo-jane", "ghost@nowhere.example")}},
			identitiesErr: boom,
		},
		"pull request author names": {
			response:       &fakeRowScanner{rows: [][]any{edgeRow("octo-jane", "ghost@nowhere.example")}},
			authorNamesErr: boom,
		},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := ResolveScoped(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), Scope{}, 100)
			if err == nil {
				t.Fatalf("ResolveScoped answered %d edges and no error after a failed read", len(got.Edges))
			}
			if !errors.Is(err, boom) {
				t.Fatalf("ResolveScoped error = %v, want it to wrap the read failure", err)
			}
		})
	}
}
