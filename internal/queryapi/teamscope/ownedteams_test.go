package teamscope

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

type ownedRows struct {
	ids    []string
	cursor int
	err    error
}

func (r *ownedRows) Next() bool { return r.cursor < len(r.ids) }
func (r *ownedRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = r.ids[r.cursor]
	r.cursor++
	return nil
}
func (r *ownedRows) Err() error { return r.err }
func (*ownedRows) Close() error { return nil }

type ownedClient struct {
	answer   []string
	err      error
	rowsErr  error
	reads    int
	bindings []dhclickhouse.Binding
	text     string
}

func (c *ownedClient) Query(_ context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	c.reads++
	c.bindings, c.text = bindings, statement
	if c.err != nil {
		return nil, c.err
	}
	return &ownedRows{ids: c.answer, err: c.rowsErr}, nil
}

func TestOwnedTeamsKeepsRequestOrderDropsBlanksAndBindsTheOrg(t *testing.T) {
	client := &ownedClient{answer: []string{"b", "a"}}
	asOf := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	got, err := OwnedTeams(context.Background(), client, "org-7", []string{"a", "", "c", "b"}, asOf)
	if err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("owned = %v, err %v; want [a b] in requested order", got, err)
	}
	for _, b := range client.bindings {
		if b.Name == BindingOrgID && b.Value != "org-7" {
			t.Fatalf("org binding %v, want org-7", b.Value)
		}
		if b.Name == BindingTeamIDs {
			if ids, _ := b.Value.([]string); len(ids) != 3 {
				t.Fatalf("team ids bound %v, want the 3 non-blank ones", ids)
			}
		}
	}
	if !strings.Contains(client.text, "org_id = {"+BindingOrgID+":String}") || !strings.Contains(client.text, OwnedTeamsMarker) {
		t.Fatalf("the statement carries no org predicate or no marker:\n%s", client.text)
	}
}

func TestOwnedTeamsAnEmptyRequestReadsNothingAndAReadErrorIsAnError(t *testing.T) {
	client := &ownedClient{}
	if got, err := OwnedTeams(context.Background(), client, "org-7", []string{"", ""}, time.Now()); got != nil || err != nil || client.reads != 0 {
		t.Fatalf("empty request: %v %v reads %d; want nil nil 0", got, err, client.reads)
	}
	if _, err := OwnedTeams(context.Background(), &ownedClient{err: errors.New("boom")}, "org-7", []string{"a"}, time.Now()); err == nil {
		t.Fatal("a failed read was treated as an answer")
	}
}

// The rows ended on an error after the ids that were read: those ids are not an answer.
func TestOwnedTeamsReturnsTheErrorTheRowsEndedOn(t *testing.T) {
	client := &ownedClient{answer: []string{"a"}, rowsErr: errors.New("stream cut")}
	got, err := OwnedTeams(context.Background(), client, "org-7", []string{"a"}, time.Now())
	if err == nil || got != nil {
		t.Fatalf("owned %v, err %v; want an error and no teams", got, err)
	}
}
