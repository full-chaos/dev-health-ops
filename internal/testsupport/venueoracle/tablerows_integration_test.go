//go:build integration

package venueoracle

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// fatalRecorder records TableRows' fatal report and stops it, as
// t.Fatalf stops a test.
type fatalRecorder struct{ message string }

type stopped struct{}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatal(args ...any) {
	r.message = fmt.Sprint(args...)
	panic(stopped{})
}

func (r *fatalRecorder) Fatalf(format string, args ...any) {
	r.message = fmt.Sprintf(format, args...)
	panic(stopped{})
}

func runTableRows(ctx context.Context, uri, query string) (rows, fatal string) {
	recorder := &fatalRecorder{}
	defer func() {
		if recovered := recover(); recovered != nil {
			if _, ok := recovered.(stopped); !ok {
				panic(recovered)
			}
			fatal = recorder.message
		}
	}()
	return TableRows(recorder, ctx, uri, query), ""
}

// TestTableRowsRefusesDecodedJSONOnPostgres runs TableRows against a real
// Postgres: every json-typed result column (json, jsonb and their arrays)
// fails with the cast instruction, and a ::text cast returns the stored
// text unchanged (spacing and key order of a json value kept).
func TestTableRowsRefusesDecodedJSONOnPostgres(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	for _, column := range []string{`'{"b": 1,"a":2}'::json`, `'{"a": 1}'::jsonb`, `ARRAY['{}'::json]`, `ARRAY['{}'::jsonb]`} {
		_, fatal := runTableRows(ctx, instance.URI, "SELECT 1 AS id, "+column+" AS doc")
		if !strings.Contains(fatal, "cast doc (") || !strings.Contains(fatal, "to text") {
			t.Errorf("%s: TableRows did not refuse the decoded column; fatal=%q", column, fatal)
		}
	}
	rows, fatal := runTableRows(ctx, instance.URI, `SELECT '{"b": 1,"a":2}'::json::text AS doc`)
	if fatal != "" || rows != `{"b": 1,"a":2}` {
		t.Errorf("::text column: rows=%q fatal=%q, want the stored text", rows, fatal)
	}
}
