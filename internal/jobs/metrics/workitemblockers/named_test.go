package workitemblockers

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// namedStore is a NamedQuerier over the memory store: it records the named statement, turns each placeholder back
// into a ? with its value, and answers through the positional store, so both callers are answered by one store.
type namedStore struct {
	store      *memoryStore
	statements []namedStatement
}

type namedStatement struct {
	text   string
	params []Param
}

var namedPlaceholder = regexp.MustCompile(`\{(p[0-9]+):(String|Array\(String\)|UInt64)\}`)

func (named *namedStore) QueryNamed(ctx context.Context, statement string, params []Param) (Rows, error) {
	named.statements = append(named.statements, namedStatement{statement, params})
	var args []any
	next := 0
	var failure error
	text := namedPlaceholder.ReplaceAllStringFunc(statement, func(match string) string {
		name := namedPlaceholder.FindStringSubmatch(match)[1]
		if next >= len(params) || params[next].Name != name {
			failure = errors.New("named store: placeholders out of order with the parameters")
			return match
		}
		args = append(args, params[next].Value)
		next++
		return "?"
	})
	if failure != nil {
		return nil, failure
	}
	if next != len(params) {
		return nil, errors.New("named store: a parameter with no placeholder")
	}
	return named.store.Query(ctx, text, args...)
}

// CHAOS-13088: a caller that binds by name runs the SAME statements as the positional caller -- the text differs only
// in each ? being a typed named placeholder, the arguments are the same values in the same order -- and gets the same
// relations and ends, hence the same spans.
func TestTheNamedReadRunsTheSameStatementsAsThePositionalRead(t *testing.T) {
	ctx := context.Background()
	naming := []string{"gh:acme/api#0", "gh:acme/web#1", "extkey:OPS-1"}

	positionalStore := newMemoryStore(3)
	relations, err := LoadRelationsNaming(ctx, positionalStore, "org", naming, 100)
	if err != nil {
		t.Fatal(err)
	}
	ends, err := LoadEndsLimited(ctx, positionalStore, "org", relations, 100)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := workitemmetrics.BlockedIntervalsWithStats(relations, ends)

	named := &namedStore{store: newMemoryStore(3)}
	got, err := LoadBlockedIntervalsNaming(ctx, named, "org", naming, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("named spans = %#v, positional spans = %#v", got, want)
	}
	if len(want) == 0 {
		t.Fatal("the fixture has no span: the comparison proves nothing")
	}

	if len(named.statements) != len(positionalStore.statements) {
		t.Fatalf("named read ran %d statements, positional read %d", len(named.statements), len(positionalStore.statements))
	}
	for index, statement := range named.statements {
		positional := positionalStore.statements[index]
		if strings.Contains(statement.text, "?") {
			t.Errorf("named statement %d still has a ?:\n%s", index, statement.text)
		}
		if back := namedPlaceholder.ReplaceAllString(statement.text, "?"); back != positional.query {
			t.Errorf("statement %d differs beyond its placeholders:\nnamed:\n%s\npositional:\n%s", index, statement.text, positional.query)
		}
		values := make([]any, 0, len(statement.params))
		for _, param := range statement.params {
			values = append(values, param.Value)
		}
		if !reflect.DeepEqual(values, positional.args) {
			t.Errorf("statement %d arguments: named %#v, positional %#v", index, values, positional.args)
		}
	}
}

// The spans of a named item are those of the organization-wide read: a relation that names the item is read whatever
// end names it, with both its ends.
func TestTheNamedSpansAreTheOrganizationWideSpansOfTheNamedItems(t *testing.T) {
	ctx := context.Background()
	all, _, err := LoadBlockedIntervals(ctx, newMemoryStore(4), "org")
	if err != nil {
		t.Fatal(err)
	}
	naming := []string{"gh:acme/web#1", "gh:acme/web#3"}
	got, err := LoadBlockedIntervalsNaming(ctx, &namedStore{store: newMemoryStore(4)}, "org", naming, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]workitemmetrics.BlockedInterval{}
	for _, id := range naming {
		if spans, found := all[id]; found {
			want[id] = spans
		}
	}
	if len(want) == 0 {
		t.Fatal("the organization-wide read has no span for the named items: the comparison proves nothing")
	}
	for id, spans := range want {
		if !reflect.DeepEqual(got[id], spans) {
			t.Errorf("%s: named spans %#v, organization-wide spans %#v", id, got[id], spans)
		}
	}
}

func TestTheNamedReadRefusesWhatItCannotBind(t *testing.T) {
	ctx := context.Background()
	if _, err := LoadBlockedIntervalsNaming(ctx, nil, "org", []string{"gh:acme/api#0"}, 10); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("no connection: %v", err)
	}
	named := &namedStore{store: newMemoryStore(1)}
	for _, organization := range []string{"", " "} {
		if _, err := LoadBlockedIntervalsNaming(ctx, named, organization, []string{"gh:acme/api#0"}, 10); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("organization %q: %v", organization, err)
		}
	}
	if _, err := LoadBlockedIntervalsNaming(ctx, named, "org", []string{"gh:acme/api#0"}, 0); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("limit 0: %v", err)
	}
	spans, err := LoadBlockedIntervalsNaming(ctx, named, "org", nil, 10)
	if err != nil || spans != nil || len(named.statements) != 0 {
		t.Fatalf("no name: spans %v, err %v, %d statements; want nothing read", spans, err, len(named.statements))
	}
}

func TestNameParameters(t *testing.T) {
	text, params, err := nameParameters("SELECT a WHERE org_id = ? AND has(?, x) LIMIT ?", []any{"org", []string{"a"}, 3})
	if err != nil {
		t.Fatal(err)
	}
	if want := "SELECT a WHERE org_id = {p0:String} AND has({p1:Array(String)}, x) LIMIT {p2:UInt64}"; text != want {
		t.Fatalf("text = %q, want %q", text, want)
	}
	if want := []Param{{"p0", "org"}, {"p1", []string{"a"}}, {"p2", 3}}; !reflect.DeepEqual(params, want) {
		t.Fatalf("params = %#v, want %#v", params, want)
	}
	for _, bad := range []struct {
		statement string
		args      []any
	}{
		{"SELECT ? AND ?", []any{"one"}},
		{"SELECT ?", []any{"one", "two"}},
		{"SELECT ?", []any{1.5}},
		{"SELECT ?", []any{-1}},
	} {
		if _, _, err := nameParameters(bad.statement, bad.args); !errors.Is(err, ErrUnnamedArgument) {
			t.Errorf("nameParameters(%q, %#v) = %v, want ErrUnnamedArgument", bad.statement, bad.args, err)
		}
	}
}
