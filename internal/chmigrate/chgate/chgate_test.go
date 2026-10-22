package chgate_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/chmigrate/chgate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

type fakeRecorder struct {
	versions []string
	err      error
}

func (f fakeRecorder) AppliedVersions(context.Context) ([]string, error) {
	return append([]string(nil), f.versions...), f.err
}

func buildVersions(t *testing.T) chgate.Build {
	t.Helper()
	build, err := chgate.LoadBuild()
	if err != nil {
		t.Fatalf("LoadBuild: %v", err)
	}
	return build
}

func atHead(t *testing.T) []string { return buildVersions(t).Versions() }

func TestLoadBuildHoldsBaselineAndChainInOrder(t *testing.T) {
	build := buildVersions(t)
	versions := build.Versions()
	chain, err := chmigrate.LoadChain()
	if err != nil || len(chain) == 0 {
		t.Fatalf("chain: %v", err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != len(baseline.Versions)+len(chain) {
		t.Fatalf("got %d versions, want %d", len(versions), len(baseline.Versions)+len(chain))
	}
	if build.Head() != chain[len(chain)-1].Version {
		t.Fatalf("head = %q, want last chain file %q", build.Head(), chain[len(chain)-1].Version)
	}
	seen := map[string]bool{}
	for _, v := range versions {
		if seen[v] {
			t.Fatalf("duplicate version %q", v)
		}
		seen[v] = true
	}
}

func TestCompare(t *testing.T) {
	build := buildVersions(t)
	full := atHead(t)
	cases := map[string]struct {
		applied []string
		want    pgmigrate.HeadVerdict
	}{
		"equal":            {full, pgmigrate.HeadsEqual},
		"behind chain":     {full[:len(full)-1], pgmigrate.HeadsBehind},
		"behind baseline":  {full[1:], pgmigrate.HeadsBehind},
		"ahead":            {append(append([]string(nil), full...), "999_future.sql"), pgmigrate.HeadsAhead},
		"ahead and behind": {append(append([]string(nil), full[1:]...), "999_future.sql"), pgmigrate.HeadsAhead},
	}
	for name, tc := range cases {
		if got := build.Compare(tc.applied); got != tc.want {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}

func run(t *testing.T, recorder chgate.Recorder) (error, string) {
	t.Helper()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	return chgate.Check(context.Background(), logger, "worker", recorder, buildVersions(t)), logs.String()
}

func TestCheckRefusesBehindWithNamesOnly(t *testing.T) {
	full := atHead(t)
	err, logs := run(t, fakeRecorder{versions: full[:len(full)-1]})
	var behind *chgate.BehindError
	if !errors.As(err, &behind) {
		t.Fatalf("want BehindError, got %v", err)
	}
	if !strings.Contains(logs, "refusing to start") || !strings.Contains(logs, full[len(full)-1]) {
		t.Fatalf("log must name the refusal and the build head: %s", logs)
	}
	if strings.Contains(err.Error(), "://") {
		t.Fatalf("error leaks a DSN: %v", err)
	}
}

func TestCheckStartsWhenEqualAheadOrUnreadable(t *testing.T) {
	full := atHead(t)
	cases := map[string]struct {
		recorder fakeRecorder
		wantLog  string
	}{
		"equal":       {fakeRecorder{versions: full}, ""},
		"ahead":       {fakeRecorder{versions: append(append([]string(nil), full...), "999_future.sql")}, "ahead of this build"},
		"empty":       {fakeRecorder{}, "no_versions_recorded"},
		"denied":      {fakeRecorder{err: &chgate.ReadError{Class: "permission_denied"}}, "permission_denied"},
		"missing":     {fakeRecorder{err: &chgate.ReadError{Class: "table_missing"}}, "table_missing"},
		"plain error": {fakeRecorder{err: errors.New("dial tcp: secret-host")}, "unreachable"},
	}
	for name, tc := range cases {
		err, logs := run(t, tc.recorder)
		if err != nil {
			t.Errorf("%s: want start, got %v", name, err)
		}
		if tc.wantLog != "" && !strings.Contains(logs, tc.wantLog) {
			t.Errorf("%s: log %q lacks %q", name, logs, tc.wantLog)
		}
		if strings.Contains(logs, "secret-host") {
			t.Errorf("%s: driver text leaked: %s", name, logs)
		}
	}
}

func TestEnforceSkipsAValueWithoutTheCapability(t *testing.T) {
	if err := chgate.Enforce(context.Background(), nil, "worker", struct{}{}); err != nil {
		t.Fatalf("got %v", err)
	}
	full := atHead(t)
	if err := chgate.Enforce(context.Background(), slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), "worker", fakeRecorder{versions: full[:1]}); err == nil {
		t.Fatal("behind recorder must refuse")
	}
}

func TestReadClass(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"denied":  {&clickhouse.Exception{Code: 497}, "permission_denied"},
		"table":   {&clickhouse.Exception{Code: 60}, "table_missing"},
		"db":      {&clickhouse.Exception{Code: 81}, "table_missing"},
		"other":   {&clickhouse.Exception{Code: 999}, "database_error"},
		"timeout": {context.DeadlineExceeded, "timeout"},
		"net":     {errors.New("dial tcp"), "unreachable"},
	}
	for name, tc := range cases {
		if got := chgate.ReadClass(tc.err); got != tc.want {
			t.Errorf("%s: got %q want %q", name, got, tc.want)
		}
	}
}
