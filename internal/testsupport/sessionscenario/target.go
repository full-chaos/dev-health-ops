package sessionscenario

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
	chclickhouse "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// Environment names of an external target (CHAOS-10157): the corpus runs against any server that answers
// the auth routes at BaseURL, go-api today and auth-service later. The target must share Key as its signing
// key, run SocialEnv with its provider endpoints on a FakeProvider, trust loopback peers, and use the
// migrated Postgres and ClickHouse the URIs name, which the corpus seeds.
const (
	EnvBaseURL       = "DEVHEALTH_AUTH_ORACLE_BASE_URL"
	EnvPostgresURI   = "DEVHEALTH_AUTH_ORACLE_POSTGRES_URI"
	EnvClickHouseURI = "DEVHEALTH_AUTH_ORACLE_CLICKHOUSE_URI"
	EnvValkeyURI     = "DEVHEALTH_AUTH_ORACLE_VALKEY_URI"
)

// Target is the server under test and the stores it shares with the corpus.
type Target struct {
	BaseURL       string
	PostgresURI   string
	ClickHouseURI string
	// ValkeyURI is flushed to open fresh rate-limit windows; ResetLimits does it when the limiter is elsewhere.
	ValkeyURI   string
	ResetLimits func()
}

// Validate is an error unless the corpus can open fresh rate-limit windows on the target between batches.
func (tg Target) Validate() error {
	if tg.BaseURL == "" {
		return fmt.Errorf("no base URL")
	}
	if tg.PostgresURI == "" {
		return fmt.Errorf("no Postgres URI: the corpus seeds and reads rows in the target's Postgres")
	}
	if tg.ClickHouseURI == "" {
		return fmt.Errorf("no ClickHouse URI: the corpus seeds the activity tables that decide the active organization at login")
	}
	if tg.ValkeyURI == "" && tg.ResetLimits == nil {
		return fmt.Errorf("no %s and no limit reset: the rate-limit steps share one window and answer 429", EnvValkeyURI)
	}
	return nil
}

// TargetFromEnv reads an external target; ok is false when EnvBaseURL is unset.
func TargetFromEnv(t *testing.T) (Target, bool) {
	t.Helper()
	base := os.Getenv(EnvBaseURL)
	if base == "" {
		return Target{}, false
	}
	target := Target{BaseURL: base, PostgresURI: os.Getenv(EnvPostgresURI), ClickHouseURI: os.Getenv(EnvClickHouseURI), ValkeyURI: os.Getenv(EnvValkeyURI)}
	if err := target.Validate(); err != nil {
		t.Fatalf("%s is set but the target is unusable: %v", EnvBaseURL, err)
	}
	return target, true
}

func (tg Target) resetLimits(t *testing.T, ctx context.Context) {
	t.Helper()
	if tg.ResetLimits != nil {
		tg.ResetLimits()
	}
	if tg.ValkeyURI == "" {
		return
	}
	venueoracle.ResetLimiterKeys(t, ctx, tg.ValkeyURI)
}

// Prepare seeds the target's stores and mints the seed access tokens.
func Prepare(t *testing.T, ctx context.Context, tg Target) map[string]string {
	t.Helper()
	if err := tg.Validate(); err != nil {
		t.Fatalf("target unusable: %v", err)
	}
	pool := venueoracle.OpenPool(t, ctx, tg.PostgresURI)
	hash, err := bcrypt.GenerateFromPassword([]byte(Password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	specs := Seed(t, ctx, pool, string(hash))
	conn, err := chclickhouse.Open(ctx, chclickhouse.DefaultConfig(tg.ClickHouseURI))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, statement := range MetricsStatements {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed metrics: %v", err)
		}
	}
	return MintTokens(t, specs)
}

// MintTokens mints the seed access tokens Seed returned specs for.
func MintTokens(t *testing.T, specs map[string]map[string]any) map[string]string {
	t.Helper()
	signer, err := edgetoken.NewSigner(Key, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	tokens := map[string]string{}
	for name, spec := range specs {
		claims := edgetoken.AccessClaims{Role: "member"}
		claims.UserID, _ = spec["user_id"].(string)
		claims.Email, _ = spec["email"].(string)
		claims.OrgID, _ = spec["org_id"].(string)
		if role, ok := spec["role"].(string); ok {
			claims.Role = role
		}
		claims.IsSuperuser, _ = spec["is_superuser"].(bool)
		if value, ok := spec["username"].(string); ok {
			claims.Username = &value
		}
		if value, ok := spec["full_name"].(string); ok {
			claims.FullName = &value
		}
		if version, ok := spec["token_version"].(int); ok {
			claims.TokenVersion = int64(version)
		}
		token, err := signer.Access(claims, time.Now(), fmt.Sprintf("00000000-0000-4000-8000-%012d", len(tokens)))
		if err != nil {
			t.Fatal(err)
		}
		tokens[name] = token
	}
	return tokens
}

// RunTarget replays the corpus against tg, whose stores Prepare seeded (tokens are its result), and returns
// the receipt. It reaches the target through its URL and stores only.
func RunTarget(t *testing.T, ctx context.Context, tg Target, tokens map[string]string) string {
	t.Helper()
	pool := venueoracle.OpenPool(t, ctx, tg.PostgresURI)
	return Run(&Harness{
		T: t, GoBase: tg.BaseURL, Tokens: tokens,
		Exec: func(sql string) {
			if _, err := pool.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
		},
		Reset: func() { tg.resetLimits(t, ctx) },
		Rows:  func(query string) (string, string) { return "", TableRows(t, ctx, tg.PostgresURI, query) },
	})
}
