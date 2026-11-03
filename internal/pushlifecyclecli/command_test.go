package pushlifecyclecli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

var gotDSN string

type call struct {
	apply  bool
	schema string
}

func runVerb(t *testing.T, args []string, lookup map[string]string) (code int, out, errOut string, calls []call) {
	t.Helper()
	oldConnect, oldBackfill := connectFn, backfillFn
	t.Cleanup(func() { connectFn, backfillFn = oldConnect, oldBackfill })
	gotDSN = ""
	connectFn = func(_ context.Context, dsn string) (*pgxpool.Pool, error) { gotDSN = dsn; return nil, nil }
	backfillFn = func(_ context.Context, _ *pgxpool.Pool, schema string, apply bool, _ int) (customerpush.BackfillReport, error) {
		calls = append(calls, call{apply, schema})
		return customerpush.BackfillReport{Candidates: 3, SourceBound: 2, OrganizationWide: 1, Revoked: 1, Written: map[bool]int{true: 3}[apply]}, nil
	}
	var so, se bytes.Buffer
	env := cli.Env{Args: args, Stdout: &so, Stderr: &se, Lookup: func(k string) (string, bool) { v, ok := lookup[k]; return v, ok }}
	code = run(context.Background(), env)
	return code, so.String(), se.String(), calls
}

func TestBackfillCreatedIsADryRunUnlessApplyIsGiven(t *testing.T) {
	lookup := map[string]string{"POSTGRES_URI": "postgres://x"}
	code, out, _, calls := runVerb(t, nil, lookup)
	if code != cli.ExitOK || len(calls) != 1 || calls[0].apply || calls[0].schema != "auth" {
		t.Fatalf("code %d calls %+v", code, calls)
	}
	if !strings.Contains(out, "candidates=3") || !strings.Contains(out, "written=0") || !strings.Contains(out, "dry-run") {
		t.Fatalf("output %q", out)
	}
	code, out, _, calls = runVerb(t, []string{"-apply"}, lookup)
	if code != cli.ExitOK || len(calls) != 1 || !calls[0].apply || !strings.Contains(out, "written=3") {
		t.Fatalf("apply: code %d calls %+v out %q", code, calls, out)
	}
}

func TestBackfillCreatedRefusesBadArgumentsBeforeConnecting(t *testing.T) {
	lookup := map[string]string{"POSTGRES_URI": "postgres://x"}
	for _, args := range [][]string{{"extra"}, {"-batch", "0"}, {"-batch", "100000"}} {
		if code, _, _, calls := runVerb(t, args, lookup); code != cli.ExitUsage || len(calls) != 0 {
			t.Errorf("args %v: code %d calls %d", args, code, len(calls))
		}
	}
	if code, _, _, calls := runVerb(t, nil, nil); code != cli.ExitFailure || len(calls) != 0 {
		t.Errorf("no DSN: code %d calls %d", code, len(calls))
	}
}

func TestBackfillCreatedExitsNonZeroWhenATokenFailed(t *testing.T) {
	oldBackfill := backfillFn
	t.Cleanup(func() { backfillFn = oldBackfill })
	oldConnect := connectFn
	t.Cleanup(func() { connectFn = oldConnect })
	connectFn = func(context.Context, string) (*pgxpool.Pool, error) { return nil, nil }
	backfillFn = func(context.Context, *pgxpool.Pool, string, bool, int) (customerpush.BackfillReport, error) {
		return customerpush.BackfillReport{Candidates: 2, Written: 1, Failed: 1}, nil
	}
	var so, se bytes.Buffer
	env := cli.Env{Args: []string{"-apply"}, Stdout: &so, Stderr: &se, Lookup: func(k string) (string, bool) { return "postgres://x", k == "POSTGRES_URI" }}
	if code := run(context.Background(), env); code != cli.ExitFailure {
		t.Fatalf("code %d, want %d", code, cli.ExitFailure)
	}
}

func TestBackfillCreatedDefaultReadsPostgresURI(t *testing.T) {
	code, _, _, calls := runVerb(t, nil, map[string]string{"POSTGRES_URI": "postgres://default", "API_DATABASE_URI": "postgres://other"})
	if code != cli.ExitOK || len(calls) != 1 || gotDSN != "postgres://default" {
		t.Fatalf("code %d calls %d dsn-match %v", code, len(calls), gotDSN == "postgres://default")
	}
}

func TestBackfillCreatedDSNEnvFlagReadsTheNamedVariable(t *testing.T) {
	code, _, _, calls := runVerb(t, []string{"-dsn-env", "API_DATABASE_URI"}, map[string]string{"API_DATABASE_URI": "postgres://api"})
	if code != cli.ExitOK || len(calls) != 1 || gotDSN != "postgres://api" {
		t.Fatalf("code %d calls %d dsn-match %v", code, len(calls), gotDSN == "postgres://api")
	}
}

func TestBackfillCreatedDSNEnvFlagReadsTheNamedFileForm(t *testing.T) {
	f := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(f, []byte("postgres://fromfile\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	code, _, _, calls := runVerb(t, []string{"-dsn-env", "API_DATABASE_URI"}, map[string]string{"API_DATABASE_URI_FILE": f})
	if code != cli.ExitOK || len(calls) != 1 || gotDSN != "postgres://fromfile" {
		t.Fatalf("code %d calls %d dsn-match %v", code, len(calls), gotDSN == "postgres://fromfile")
	}
}

func TestBackfillCreatedDSNEnvFlagMissingVariableNamesTheNameAndDoesNotConnect(t *testing.T) {
	code, _, errOut, calls := runVerb(t, []string{"-dsn-env", "NOPE_URI"}, map[string]string{"POSTGRES_URI": "postgres://x"})
	if code != cli.ExitFailure || len(calls) != 0 || gotDSN != "" {
		t.Fatalf("code %d calls %d", code, len(calls))
	}
	if !strings.Contains(errOut, "NOPE_URI") || strings.Contains(errOut, "postgres://") {
		t.Fatalf("stderr %q", errOut)
	}
}

func TestBackfillCreatedDSNEnvFlagRefusesImplausibleNames(t *testing.T) {
	lookup := map[string]string{"POSTGRES_URI": "postgres://x"}
	for _, name := range []string{"", "lower", "1BAD", "A-B", "A B", "A=B", "postgres://x"} {
		code, _, errOut, calls := runVerb(t, []string{"-dsn-env", name}, lookup)
		if code != cli.ExitUsage || len(calls) != 0 || gotDSN != "" || !strings.Contains(errOut, "argument error: -dsn-env") {
			t.Errorf("name %q: code %d calls %d stderr %q", name, code, len(calls), errOut)
		}
	}
}

func TestBackfillCreatedPoolOpenFailureNeverPrintsTheDSN(t *testing.T) {
	const bad = "postgres://sensitive_login_c92:dsn_secret_a91@[::1"
	dir := t.TempDir()
	file := filepath.Join(dir, "dsn")
	if err := os.WriteFile(file, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	cells := []struct {
		name   string
		args   []string
		lookup map[string]string
		real   bool
		want   string
	}{
		{"named_var", []string{"-dsn-env", "API_DATABASE_URI"}, map[string]string{"API_DATABASE_URI": bad}, true, "API_DATABASE_URI"},
		{"named_file", []string{"-dsn-env", "API_DATABASE_URI"}, map[string]string{"API_DATABASE_URI_FILE": file}, true, "API_DATABASE_URI"},
		{"default_var", nil, map[string]string{"POSTGRES_URI": bad}, true, "POSTGRES_URI"},
		{"default_file", nil, map[string]string{"POSTGRES_URI_FILE": file}, true, "POSTGRES_URI"},
		{"fake_seam_error", []string{"-dsn-env", "API_DATABASE_URI"}, map[string]string{"API_DATABASE_URI": bad}, false, "API_DATABASE_URI"},
	}
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			oldConnect, oldBackfill := connectFn, backfillFn
			t.Cleanup(func() { connectFn, backfillFn = oldConnect, oldBackfill })
			if !c.real {
				connectFn = func(_ context.Context, dsn string) (*pgxpool.Pool, error) {
					return nil, errors.New("cannot parse `" + dsn + "`")
				}
			} else {
				connectFn = connect
			}
			backfillFn = func(context.Context, *pgxpool.Pool, string, bool, int) (customerpush.BackfillReport, error) {
				t.Fatal("backfill must not run")
				return customerpush.BackfillReport{}, nil
			}
			var so, se bytes.Buffer
			env := cli.Env{Args: c.args, Stdout: &so, Stderr: &se, Lookup: func(k string) (string, bool) { v, ok := c.lookup[k]; return v, ok }}
			if code := run(context.Background(), env); code != cli.ExitFailure {
				t.Fatalf("code %d, want %d", code, cli.ExitFailure)
			}
			got := se.String()
			for _, leak := range []string{"sensitive_login_c92", "dsn_secret_a91", "postgres://", "xxxxxx"} {
				if strings.Contains(got, leak) {
					t.Errorf("stderr leaks %q: %q", leak, got)
				}
			}
			if !strings.Contains(got, c.want) {
				t.Errorf("stderr does not name %s: %q", c.want, got)
			}
		})
	}
}
