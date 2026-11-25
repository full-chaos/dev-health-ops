package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const homeDevScopeID = "dev-synthetic-a@example.invalid"

// homeDevProbeClient records every statement and its bindings. known answers the developer existence read of a scope with
// one hit (the scope is known); otherwise it answers every read with no rows.
type homeDevProbeClient struct {
	known      bool
	mu         sync.Mutex
	statements []string
	bindings   [][]clickhouse.Binding
}

func (c *homeDevProbeClient) Query(_ context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, b)
	if c.known && strings.Contains(statement, "SELECT hit FROM") {
		return &homeDevOneHitRows{}, nil
	}
	return emptyRows{}, nil
}

type homeDevOneHitRows struct{ done bool }

func (r *homeDevOneHitRows) Next() bool {
	if r.done {
		return false
	}
	r.done = true
	return true
}
func (r *homeDevOneHitRows) Scan(dest ...any) error {
	if len(dest) == 1 {
		if s, ok := dest[0].(*string); ok {
			*s = homeDevScopeID
		}
	}
	return nil
}
func (r *homeDevOneHitRows) Err() error   { return nil }
func (r *homeDevOneHitRows) Close() error { return nil }

func homeDevRun(t *testing.T, ch *homeDevProbeClient, scope *model.ScopeFilterInput, what *model.WhatFilterInput) {
	t.Helper()
	r := &Resolver{ClickHouse: ch, Postgres: pgNoRows{&recordingHomePGClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-synthetic-1"})
	if _, err := r.Query().Home(ctx, "org-synthetic-1", &model.FilterInput{Scope: scope, What: what}, nil); err != nil {
		t.Fatalf("home: %v", err)
	}
}

var homeDevDate = regexp.MustCompile(`\d{4}-\d{2}-\d{2}([T ][0-9:.]+(Z|[+-][0-9:]+)?)?`)

func homeDevDigest(ch *homeDevProbeClient) (string, int) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	lines := make([]string, 0, len(ch.statements))
	for i, statement := range ch.statements {
		bindings := append([]clickhouse.Binding(nil), ch.bindings[i]...)
		sort.Slice(bindings, func(a, b int) bool { return bindings[a].Name < bindings[b].Name })
		raw, _ := json.Marshal(bindings)
		lines = append(lines, statement+"\n"+homeDevDate.ReplaceAllString(string(raw), "D"))
	}
	sort.Strings(lines)
	sum := sha256.Sum256([]byte(strings.Join(lines, "\n--\n")))
	return hex.EncodeToString(sum[:]), len(lines)
}

func homeDevIsValueRead(statement string) bool {
	return strings.Contains(statement, "AS value") || strings.HasPrefix(strings.TrimSpace(statement), "SELECT day") || strings.Contains(statement, "sumIf(prs_merged")
}

// A KNOWN developer scope serves no organization number: every metric value read carries the no-data filter (the state an
// unknown developer id already gets), and no signal or investment table is read.
func TestHomeKnownDeveloperServesNoOrganizationNumber(t *testing.T) {
	ch := &homeDevProbeClient{known: true}
	homeDevRun(t, ch, &model.ScopeFilterInput{Level: model.ScopeLevelInputDeveloper, Ids: []string{homeDevScopeID}}, nil)
	values := 0
	for i, statement := range ch.statements {
		for _, table := range []string{"recommendations_daily", "compounding_risk_daily", "investment"} {
			if strings.Contains(statement, table) {
				t.Errorf("read %d reads %s under a developer scope", i, table)
			}
		}
		if !homeDevIsValueRead(statement) {
			continue
		}
		values++
		if !strings.Contains(statement, "1 = 0") {
			t.Errorf("read %d serves organization data under a developer scope: %.120s", i, strings.Join(strings.Fields(statement), " "))
		}
	}
	if values == 0 {
		t.Fatal("no metric value read was issued: the cell measures nothing")
	}
}
