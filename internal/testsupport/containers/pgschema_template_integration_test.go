//go:build integration

package containers_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

func runShared(t *testing.T, body func()) {
	t.Helper()
	ran := false
	code := containers.RunWithSharedServers(func() int {
		body()
		ran = true
		return 0
	})
	if !ran || code != 0 {
		t.Fatalf("shared run: ran %v, exit code %d", ran, code)
	}
}

func tableColumns(ctx context.Context, t *testing.T, uri string) []string {
	t.Helper()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	rows, err := conn.Query(ctx, `SELECT table_schema||'.'||table_name||'.'||column_name||':'||data_type||':'||coalesce(column_default, '')
	  FROM information_schema.columns WHERE table_schema NOT IN ('pg_catalog','information_schema') ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// Start must hand out the same schema a fresh Apply builds, and many of them
// at once must not disturb one another.
func TestStartEqualsAFreshApplyAndSurvivesParallelCallers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runShared(t, func() { startEqualsAFreshApply(ctx, t) })
}

func startEqualsAFreshApply(ctx context.Context, t *testing.T) {
	t.Helper()
	plain, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plain.Close(context.Background()) }()
	pgschema.ApplyURI(ctx, t, plain.URI)
	want := tableColumns(ctx, t, plain.URI)
	if len(want) == 0 {
		t.Fatal("the fresh Apply built no columns")
	}
	t.Run("callers", func(t *testing.T) {
		for range 8 {
			t.Run("one", func(t *testing.T) {
				t.Parallel()
				instance := pgschema.Start(ctx, t)
				defer func() { _ = instance.Close(context.Background()) }()
				got := tableColumns(ctx, t, instance.URI)
				if len(got) != len(want) {
					t.Fatalf("%d columns, want %d", len(got), len(want))
				}
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("column %d: %q, want %q", i, got[i], want[i])
					}
				}
			})
		}
	})
}

func TestStartTemplateOffBuildsTheChainOnTheCallersOwnDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	t.Setenv(containers.PostgresTemplateEnv, "off")
	runShared(t, func() {
		instance := pgschema.Start(ctx, t)
		defer func() { _ = instance.Close(context.Background()) }()
		if len(tableColumns(ctx, t, instance.URI)) == 0 {
			t.Error("Start with the template off returned an unmigrated database")
		}
	})
}
