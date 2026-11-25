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

type homeDevArm struct {
	name  string
	scope *model.ScopeFilterInput
	what  *model.WhatFilterInput
	known bool
	want  string
	reads int
}

// Every arm that names no developer, and a developer id the organization does not know, answer byte for byte the reads main
// issued: the digests are of the sorted statements and bindings, dates masked, taken AT MAIN (5dd4758412f2) from this code.
func TestHomeScopeArmsOtherThanAKnownDeveloperAreByteEqualToMain(t *testing.T) {
	const orgDigest = "d206347a2a7bd828af06b2f69b06b0d16405a471abb13f3ac5889c7e47abef9f"
	arms := []homeDevArm{
		{"no filter", nil, nil, false, orgDigest, 40},
		{"org", &model.ScopeFilterInput{Level: model.ScopeLevelInputOrg}, nil, false, orgDigest, 40},
		{"team", &model.ScopeFilterInput{Level: model.ScopeLevelInputTeam, Ids: []string{"team-synthetic-a"}}, nil, false, "224f1786f71d061c586b5abe0a669b17b3ba050e8cab9a972e8cd55e6401cbcd", 40},
		{"repo", &model.ScopeFilterInput{Level: model.ScopeLevelInputRepo, Ids: []string{"repo-synthetic-a"}}, nil, false, "e4bab9a9cfc7fd11b82850834ffc7949fa0e9d710574b7a4e9ca8606b0eb3b04", 30},
		{"org with what.repos", &model.ScopeFilterInput{Level: model.ScopeLevelInputOrg}, &model.WhatFilterInput{Repos: []string{"repo-synthetic-a"}}, false, "d668ae210f92b0ebcdf1031cc5da3c76e65dce25a8ef660d1b430f3ea9576199", 39},
		{"developer, no ids", &model.ScopeFilterInput{Level: model.ScopeLevelInputDeveloper}, nil, false, orgDigest, 40},
		{"developer, unknown id", &model.ScopeFilterInput{Level: model.ScopeLevelInputDeveloper, Ids: []string{homeDevScopeID}}, nil, false, "75745f707794ce581d6be6bc6b9f76b31c4957e265fed6f6b4526e8af05f35b8", 40},
	}
	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			ch := &homeDevProbeClient{known: arm.known}
			homeDevRun(t, ch, arm.scope, arm.what)
			digest, reads := homeDevDigest(ch)
			if digest != arm.want || reads != arm.reads {
				t.Fatalf("reads changed: digest %s (%d reads), main %s (%d reads)", digest, reads, arm.want, arm.reads)
			}
		})
	}
}

// A known developer id gets the same answer shape as an unknown one (no existence oracle): the same statements and bindings,
// and every read is an organization-health read (the same text the organization scope issues) or carries the no-data filter.
func TestHomeKnownDeveloperReadsTheSameAsAnUnknownOne(t *testing.T) {
	scope := &model.ScopeFilterInput{Level: model.ScopeLevelInputDeveloper, Ids: []string{homeDevScopeID}}
	known, unknown, org := &homeDevProbeClient{known: true}, &homeDevProbeClient{}, &homeDevProbeClient{}
	homeDevRun(t, known, scope, nil)
	homeDevRun(t, unknown, scope, nil)
	homeDevRun(t, org, nil, nil)
	kd, kn := homeDevDigest(known)
	ud, un := homeDevDigest(unknown)
	if kd != ud || kn != un {
		t.Fatalf("a known developer reads %d statements (%s), an unknown one %d (%s)", kn, kd, un, ud)
	}
	orgStatements := map[string]bool{}
	for _, s := range org.statements {
		orgStatements[s] = true
	}
	for _, s := range known.statements {
		if !orgStatements[s] && !strings.Contains(s, "SELECT hit FROM") && !strings.Contains(s, "1 = 0") {
			t.Errorf("a developer-scope read that is neither an organization-health read nor under the no-data filter: %.120s", strings.Join(strings.Fields(s), " "))
		}
	}
}

// An unknown developer id with repositories named reads what an unknown developer id reads with none named (main read 30
// repository-narrowed statements instead): no repository-narrowed organization number is served under a person's name.
func TestHomeUnknownDeveloperWithNamedReposReadsTheNoDataStatements(t *testing.T) {
	const unknownDeveloperDigest = "75745f707794ce581d6be6bc6b9f76b31c4957e265fed6f6b4526e8af05f35b8"
	ch := &homeDevProbeClient{}
	homeDevRun(t, ch, &model.ScopeFilterInput{Level: model.ScopeLevelInputDeveloper, Ids: []string{homeDevScopeID}}, &model.WhatFilterInput{Repos: []string{"repo-synthetic-a"}})
	if digest, reads := homeDevDigest(ch); digest != unknownDeveloperDigest || reads != 40 {
		t.Fatalf("reads: digest %s (%d reads), want the unknown-developer statements %s (40 reads)", digest, reads, unknownDeveloperDigest)
	}
}

func homeDevResult(t *testing.T, scope *model.ScopeFilterInput, what *model.WhatFilterInput) *model.HomeResult {
	t.Helper()
	r := &Resolver{ClickHouse: &homeDevProbeClient{}, Postgres: pgNoRows{&recordingHomePGClient{}}}
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-synthetic-1"})
	res, err := r.Query().Home(ctx, "org-synthetic-1", &model.FilterInput{Scope: scope, What: what}, nil)
	if err != nil {
		t.Fatalf("home: %v", err)
	}
	return res
}

// A repository filter the developer no-data guard short-circuits is not reported as applied.
func TestHomeDeveloperScopeDoesNotReportTheRepoFilterAsApplied(t *testing.T) {
	res := homeDevResult(t, &model.ScopeFilterInput{Level: model.ScopeLevelInputDeveloper, Ids: []string{homeDevScopeID}}, &model.WhatFilterInput{Repos: []string{"repo-synthetic-a"}})
	if len(res.Deltas) == 0 {
		t.Fatal("no delta in the response: the cell measures nothing")
	}
	for _, delta := range res.Deltas {
		if delta.RepoFilterApplied != nil && *delta.RepoFilterApplied {
			t.Errorf("metric %s reports repoFilterApplied=true while its read carries the no-data filter", delta.Metric)
		}
	}
}
