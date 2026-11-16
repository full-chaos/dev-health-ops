package testopsjobfailures

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

type ownedClient struct {
	*fakeClient
	owned      uint64
	err        error
	ownedCalls int
}

func (c *ownedClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	if strings.Contains(statement, "owned_repositories") {
		c.ownedCalls++
		if c.err != nil {
			return nil, c.err
		}
		return &fakeRows{rows: [][]any{{c.owned}}}, nil
	}
	return c.fakeClient.Query(ctx, statement, bindings)
}

func resolveWith(t *testing.T, client *ownedClient, scope Scope) (*string, int) {
	t.Helper()
	got, err := Resolve(context.Background(), client, "org-1", date(t, "2026-08-01"), date(t, "2026-08-30"), scope, 20)
	if err != nil {
		t.Fatal(err)
	}
	return got.FilterEmptyReason, len(got.Groups)
}

// A team that owns no repository gets the reason with no groups; the list is not made to look like "no failures".
func TestResolve_TeamThatOwnsNoRepositoryGetsTheReason(t *testing.T) {
	reason, groups := resolveWith(t, &ownedClient{fakeClient: &fakeClient{}, owned: 0}, Scope{TeamIDs: []string{"t0"}, FilterEmptyReason: true})
	if reason == nil || *reason != "team_owns_no_repository" || groups != 0 {
		t.Fatalf("reason = %v, groups = %d; want team_owns_no_repository, 0", reason, groups)
	}
}

func TestResolve_TeamWithOwnedRepositoriesAndNoFailuresHasNoReason(t *testing.T) {
	reason, _ := resolveWith(t, &ownedClient{fakeClient: &fakeClient{}, owned: 2}, Scope{TeamIDs: []string{"t1"}, FilterEmptyReason: true})
	if reason != nil {
		t.Fatalf("reason = %v, want null", *reason)
	}
}

func TestResolve_NoTeamScopeOrFieldNotSelectedSendsNoOwnershipStatement(t *testing.T) {
	for name, scope := range map[string]Scope{
		"no team ids":        {FilterEmptyReason: true},
		"blank team id":      {TeamIDs: []string{""}, FilterEmptyReason: true},
		"field not selected": {TeamIDs: []string{"t0"}},
	} {
		client := &ownedClient{fakeClient: &fakeClient{}, owned: 0}
		reason, _ := resolveWith(t, client, scope)
		if reason != nil || client.ownedCalls != 0 {
			t.Errorf("%s: reason = %v, ownership statements = %d; want null, 0", name, reason, client.ownedCalls)
		}
	}
}

func TestResolve_OwnershipReadFailureFailsTheField(t *testing.T) {
	boom := errors.New("boom")
	client := &ownedClient{fakeClient: &fakeClient{}, err: boom}
	_, err := Resolve(context.Background(), client, "org-1", date(t, "2026-08-01"), date(t, "2026-08-30"), Scope{TeamIDs: []string{"t0"}, FilterEmptyReason: true}, 20)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the read error", err)
	}
}
