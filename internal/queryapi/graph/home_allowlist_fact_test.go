package graph

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

type statementRecordingCHClient struct {
	mu         sync.Mutex
	statements []string
	bindings   []clickhouse.Binding
}

func (c *statementRecordingCHClient) Query(_ context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statements = append(c.statements, statement)
	c.bindings = append(c.bindings, b...)
	return nil, errors.New("statementRecordingCHClient: recorded")
}

// Fact cell (CHAOS-13086, no behaviour change): the GraphQL home resolver reads no scope. An allowlist scope on the
// claims reaches home with no ceiling term: the answer is served and carries no basis-scope marker.
func TestHome_GraphQLResolverIgnoresAnAllowlistScope(t *testing.T) {
	ch := &statementRecordingCHClient{}
	r := &Resolver{ClickHouse: ch, Postgres: pgNoRows{&recordingHomePGClient{}}}
	scope := pepgate.Allowlist("org-1", []string{"acme/a"})
	ctx := authctx.WithClaims(context.Background(), authctx.Claims{OrgID: "org-1", Scope: scope})
	_, _ = r.Query().Home(ctx, "org-1", nil, nil)
	if len(ch.statements) == 0 {
		t.Fatal("no ClickHouse statement ran")
	}
	for _, s := range ch.statements {
		if strings.Contains(s, "ceiling_") {
			t.Fatalf("a ceiling term reached a GraphQL home statement:\n%s", s)
		}
	}
	for _, b := range ch.bindings {
		if strings.HasPrefix(b.Name, "ceiling_") {
			t.Fatalf("a ceiling binding %q reached GraphQL home", b.Name)
		}
	}
}
