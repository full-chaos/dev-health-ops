package experiments

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// matcherCH answers the allowlist key matcher with one repository per key, or fails it, and records every read.
type matcherCH struct {
	recordingCH
	failMatcher bool
	bindings    [][]clickhouse.Binding
}

type matcherRows struct {
	keys  []string
	index int
}

func (r *matcherRows) Next() bool   { r.index++; return r.index <= len(r.keys) }
func (r *matcherRows) Err() error   { return nil }
func (r *matcherRows) Close() error { return nil }
func (r *matcherRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.keys[r.index-1]
	*dest[1].(*[]string) = []string{fmt.Sprintf("aaaaaaaa-aaaa-4aaa-8aaa-%012d", r.index)}
	return nil
}

func (c *matcherCH) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	c.bindings = append(c.bindings, bindings)
	c.mu.Unlock()
	for _, binding := range bindings {
		if binding.Name == teamscope.BindingCeilingKeys {
			c.mu.Lock()
			c.sqls = append(c.sqls, statement)
			c.mu.Unlock()
			if c.failMatcher {
				return nil, errors.New("max_execution_time exceeded")
			}
			keys, _ := binding.Value.([]string)
			return &matcherRows{keys: keys}, nil
		}
	}
	return c.recordingCH.Query(ctx, statement, bindings)
}

func (c *matcherCH) ceilingBound() (bound, unbound int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, statement := range c.sqls {
		if !strings.Contains(statement, "repo_metrics_daily") {
			continue
		}
		has := false
		for _, binding := range c.bindings[i] {
			has = has || binding.Name == teamscope.BindingCeilingIDs
		}
		if has {
			bound++
		} else {
			unbound++
		}
	}
	return bound, unbound
}

var ceilingNow = time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)

// CHAOS-13162: an allowlist scope is a repository ceiling on the build: one key matcher read, and every
// repository-keyed read carries the ceiling. Legacy and Organization are today's build: no matcher, no ceiling.
func TestNewBuilderHoldsAnAllowlistScopeToTheCeiling(t *testing.T) {
	restricted := &matcherCH{}
	got := Resolve(context.Background(), NewBuilder(home.QueryClient(restricted), pepgate.Allowlist("org-1", []string{"acme/a"})), "org-1", nil, ceilingNow)
	if !got.DerivedFromOpportunities {
		t.Fatal("the build under a ceiling failed")
	}
	if n := restricted.countWith("identity_path"); n != 1 {
		t.Fatalf("%d key matcher reads, want 1", n)
	}
	if bound, unbound := restricted.ceilingBound(); bound == 0 || unbound != 0 {
		t.Fatalf("repo_metrics_daily reads: %d with the ceiling, %d without; want all with it", bound, unbound)
	}
	for name, scope := range map[string]pepgate.Scope{"legacy": pepgate.Legacy("org-1"), "organization": pepgate.Organization("org-1"), "none": {}} {
		open := &matcherCH{}
		Resolve(context.Background(), NewBuilder(home.QueryClient(open), scope), "org-1", nil, ceilingNow)
		if bound, _ := open.ceilingBound(); bound != 0 || open.countWith("identity_path") != 0 {
			t.Fatalf("%s scope read a matcher or a ceiling", name)
		}
	}
}

// A ceiling that cannot be resolved fails the build: the field answers no experiment, never a build over more
// repositories.
func TestNewBuilderFailsClosedWhenTheCeilingCannotBeResolved(t *testing.T) {
	failing := &matcherCH{failMatcher: true}
	got := Resolve(context.Background(), NewBuilder(home.QueryClient(failing), pepgate.Allowlist("org-1", []string{"acme/a"})), "org-1", nil, ceilingNow)
	if got.DerivedFromOpportunities || len(got.Items) != 0 {
		t.Fatalf("answered %+v, want no experiment", got)
	}
	if n := len(failing.sqls); n != 1 {
		t.Fatalf("%d reads after the failed matcher, want only the matcher", n)
	}
}
