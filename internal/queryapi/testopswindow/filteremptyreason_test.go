package testopswindow

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// ownedClient answers the ownership count of the reason apart from the two window reads.
type ownedClient struct {
	*fakeClient
	owned      uint64
	err        error
	ownedCalls int
}

type ownedRows struct {
	value uint64
	read  bool
}

func (r *ownedRows) Next() bool { r.read = !r.read; return r.read }
func (r *ownedRows) Scan(dest ...any) error {
	*(dest[0].(*uint64)) = r.value
	return nil
}
func (r *ownedRows) Err() error   { return nil }
func (r *ownedRows) Close() error { return nil }

func (c *ownedClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if strings.Contains(statement, "owned_repositories") {
		c.ownedCalls++
		if c.err != nil {
			return nil, c.err
		}
		return &ownedRows{value: c.owned}, nil
	}
	return c.fakeClient.Query(ctx, statement, bindings)
}

func emptyWindow() *fakeClient {
	return &fakeClient{coverage: []any{uint64(0), uint64(0)}, cases: casesRow(0, 0, 0, 0, 0, nil, nil, 0)}
}

func reasonOf(t *testing.T, client *ownedClient, scope Scope) (reason *string, rates [3]*float64) {
	t.Helper()
	got, err := Resolve(context.Background(), client, "org-1", date(t, "2026-10-31"), date(t, "2026-11-13"), scope)
	if err != nil {
		t.Fatal(err)
	}
	return got.FilterEmptyReason, [3]*float64{got.PassRatePct, got.FailureRatePct, got.FlakeRatePct}
}

// A team that owns no repository gets the reason, and the window numbers stay as served: counts 0, rates null (never a rate of 0).
func TestResolve_TeamThatOwnsNoRepositoryGetsTheReason(t *testing.T) {
	client := &ownedClient{fakeClient: emptyWindow(), owned: 0}
	reason, rates := reasonOf(t, client, Scope{TeamIDs: []string{"t0"}, FilterEmptyReason: true})
	if reason == nil || *reason != "team_owns_no_repository" {
		t.Fatalf("reason = %v, want team_owns_no_repository", reason)
	}
	for i, r := range rates {
		if r != nil {
			t.Errorf("rate %d = %v, want null", i, *r)
		}
	}
}

func TestResolve_TeamWithOwnedRepositoriesHasNoReasonEvenWithNoRows(t *testing.T) {
	client := &ownedClient{fakeClient: emptyWindow(), owned: 3}
	if reason, _ := reasonOf(t, client, Scope{TeamIDs: []string{"t1"}, FilterEmptyReason: true}); reason != nil {
		t.Fatalf("reason = %v, want null", *reason)
	}
}

// Both filters apply: the reason is about the team scope, whatever repositories are named beside it.
func TestResolve_ReasonDoesNotDependOnNamedRepositories(t *testing.T) {
	client := &ownedClient{fakeClient: emptyWindow(), owned: 0}
	reason, _ := reasonOf(t, client, Scope{TeamIDs: []string{"t0"}, RepoIDs: []string{"acme/web"}, FilterEmptyReason: true})
	if reason == nil {
		t.Fatal("reason = null, want team_owns_no_repository")
	}
}

func TestResolve_NoTeamScopeOrFieldNotSelectedSendsNoOwnershipStatement(t *testing.T) {
	for name, scope := range map[string]Scope{
		"no team ids":            {FilterEmptyReason: true},
		"blank team id":          {TeamIDs: []string{""}, FilterEmptyReason: true},
		"field not selected":     {TeamIDs: []string{"t0"}},
		"neither team nor field": {},
	} {
		client := &ownedClient{fakeClient: emptyWindow(), owned: 0}
		reason, _ := reasonOf(t, client, scope)
		if reason != nil || client.ownedCalls != 0 {
			t.Errorf("%s: reason = %v, ownership statements = %d; want null, 0", name, reason, client.ownedCalls)
		}
	}
}

func TestResolve_OwnershipReadFailureFailsTheField(t *testing.T) {
	boom := errors.New("boom")
	client := &ownedClient{fakeClient: emptyWindow(), err: boom}
	_, err := Resolve(context.Background(), client, "org-1", date(t, "2026-10-31"), date(t, "2026-11-13"), Scope{TeamIDs: []string{"t0"}, FilterEmptyReason: true})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the read error", err)
	}
}
