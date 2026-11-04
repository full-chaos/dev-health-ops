package decisionstate

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionrule"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Live and BaseLive come from the shared rule's Live fragment at the transaction's clock, and from nothing hand-written.
func TestSessionQueryIsBuiltFromTheSharedRule(t *testing.T) {
	rule, err := sessionrule.New(`"auth"`, 7*24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	query := sessionQuery(rule, `"auth".sessions`)
	for _, alias := range []string{"s", "b"} {
		if !strings.Contains(query, rule.Live(alias, "now()")) {
			t.Errorf("the session query lacks the shared rule for alias %s", alias)
		}
	}
	rest := strings.ReplaceAll(strings.ReplaceAll(query, rule.Live("s", "now()"), ""), rule.Live("b", "now()"), "")
	for _, hand := range []string{"revoked_at", "expires_at"} {
		if strings.Contains(rest, hand) {
			t.Errorf("the session query tests %s by hand outside the shared rule", hand)
		}
	}
}

func TestNewRequiresARefreshLifetime(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://u:p@127.0.0.1:1/db")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	users := authapi.PGUsers{Pool: pool, AuthSchema: schema}
	if _, err := New(Config{Pool: pool, Schema: schema, Users: users}); err == nil {
		t.Error("a reader with no refresh lifetime was built: there is no second default")
	}
	if _, err := New(Config{Pool: pool, Schema: schema, Users: users, RefreshTTL: time.Hour}); err != nil {
		t.Errorf("New: %v", err)
	}
}
