//go:build integration

package health

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestHealthPostgresAnswersFromTheRecordedRevision(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)`); err != nil {
		t.Fatalf("create alembic_version: %v", err)
	}

	cases := []struct {
		name      string
		revisions []string
		want      string
	}{
		{"known head", []string{"0152"}, "ok"},
		{"minimum", []string{"0075"}, "ok"},
		{"unknown newer head", []string{"0999"}, "ok"},
		{"unknown newer head beside the cutover branch head", []string{"0066", "0153"}, "ok"},
		{"below the minimum", []string{"0074"}, "down"},
		{"cutover branch head alone", []string{"0066"}, "down"},
		{"unparseable", []string{"abcd"}, "down"},
		{"five digits", []string{"00751"}, "down"},
		{"empty string", []string{""}, "down"},
		{"no row", nil, "down"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `TRUNCATE alembic_version`); err != nil {
				t.Fatal(err)
			}
			for _, revision := range test.revisions {
				if _, err := pool.Exec(ctx, `INSERT INTO alembic_version (version_num) VALUES ($1)`, revision); err != nil {
					t.Fatal(err)
				}
			}
			_, body := serve(t, Deps{Pool: pool}, http.MethodGet, "/health")
			want := `"postgres":"` + test.want + `"`
			if !strings.Contains(body, want) {
				t.Errorf("recorded %q: /health body %s lacks %s", test.revisions, body, want)
			}
		})
	}
}
