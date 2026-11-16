package teamscope

import (
	"context"
	"errors"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type ownedCountClient struct {
	owned      uint64
	err        error
	statements []string
	bindings   [][]dhclickhouse.Binding
}

func (c *ownedCountClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, bindings)
	if c.err != nil {
		return nil, c.err
	}
	return &countRows{value: c.owned}, nil
}

type countRows struct {
	value uint64
	read  bool
}

func (r *countRows) Next() bool { r.read = !r.read; return r.read }
func (r *countRows) Scan(dest ...any) error {
	*(dest[0].(*uint64)) = r.value
	return nil
}
func (r *countRows) Err() error   { return nil }
func (r *countRows) Close() error { return nil }

func TestTeamOwnsNoRepositoryIsTheOwnershipRule(t *testing.T) {
	zero := &ownedCountClient{owned: 0}
	got, err := TeamOwnsNoRepository(context.Background(), zero, "org-1", []string{"t0"}, asOf)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || *got != ReasonTeamOwnsNoRepository {
		t.Fatalf("0 owned = %v, want %q", deref(got), ReasonTeamOwnsNoRepository)
	}
	if len(zero.statements) != 1 {
		t.Fatalf("statements = %d, want 1", len(zero.statements))
	}
	// The statement is the one ownership rule (RepoCondition), narrowed to the selected teams and the org, as of the instant.
	condition, _ := RepoCondition("org-1", "toString(id)", []string{"t0"}, asOf)
	if !strings.Contains(zero.statements[0], condition) {
		t.Errorf("statement does not carry RepoCondition:\n%s", zero.statements[0])
	}
	if !strings.Contains(zero.statements[0], "LIMIT 1") {
		t.Errorf("statement is not bounded to one row:\n%s", zero.statements[0])
	}
	names := map[string]any{}
	for _, b := range zero.bindings[0] {
		names[b.Name] = b.Value
	}
	if names[BindingOrgID] != "org-1" || names[BindingAsOf] != asOf {
		t.Errorf("bindings = %v", names)
	}
	if ids, _ := names[BindingTeamIDs].([]string); len(ids) != 1 || ids[0] != "t0" {
		t.Errorf("team ids binding = %v", names[BindingTeamIDs])
	}
}

func TestTeamOwnsNoRepositoryOneOwnedRepositoryIsNoReason(t *testing.T) {
	for _, owned := range []uint64{1, 7} {
		got, err := TeamOwnsNoRepository(context.Background(), &ownedCountClient{owned: owned}, "org-1", []string{"t1", "t2"}, asOf)
		if err != nil || got != nil {
			t.Errorf("%d owned = %v, %v; want nil, nil", owned, deref(got), err)
		}
	}
}

func TestTeamOwnsNoRepositoryNoTeamScopeCostsNothing(t *testing.T) {
	for _, ids := range [][]string{nil, {}, {""}, {"", ""}} {
		client := &ownedCountClient{owned: 0}
		got, err := TeamOwnsNoRepository(context.Background(), client, "org-1", ids, asOf)
		if err != nil || got != nil || len(client.statements) != 0 {
			t.Errorf("ids %q = %v, %v, %d statements; want nil, nil, 0", ids, deref(got), err, len(client.statements))
		}
	}
}

func TestTeamOwnsNoRepositoryReadFailureIsAnErrorNotAReason(t *testing.T) {
	boom := errors.New("boom")
	got, err := TeamOwnsNoRepository(context.Background(), &ownedCountClient{err: boom}, "org-1", []string{"t0"}, asOf)
	if !errors.Is(err, boom) || got != nil {
		t.Fatalf("got %v, %v; want the read error and no reason", deref(got), err)
	}
}

func TestTeamOwnsNoRepositoryWireWord(t *testing.T) {
	if string(ReasonTeamOwnsNoRepository) != "team_owns_no_repository" {
		t.Errorf("wire word = %q", ReasonTeamOwnsNoRepository)
	}
}
