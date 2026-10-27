//go:build integration

package adminops

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

const orgDeleteRefusedLine = "Error: ClickHouse is not configured, not reachable or not permitted to purge; organization deletion refused. Nothing was deleted.\n"

// `dho admin orgs delete` with no usable ClickHouse (no CLICKHOUSE_URI, a DSN
// that cannot connect, a login without ALTER DELETE) prints the refusal, exits
// 1, logs the refusal on stderr and leaves every row of both organizations;
// the ClickHouse password is printed nowhere.
func TestOrgsDeleteRefusesWithoutAUsableClickHouse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	db := startDatabase(t)
	instance, err := containers.StartClickHouseIsolated(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	const selectOnlyPassword = "orgdel-cli-select-pw-41ab"
	for _, statement := range []string{
		"CREATE USER orgdel_cli_select IDENTIFIED WITH plaintext_password BY '" + selectOnlyPassword + "'",
		"GRANT SELECT ON " + containers.ClickHouseDatabase + ".* TO orgdel_cli_select",
	} {
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	login := func(user, password, host string) string {
		parsed, err := url.Parse(instance.URI)
		if err != nil {
			t.Fatal(err)
		}
		if user != "" {
			parsed.User = url.UserPassword(user, password)
		}
		if host != "" {
			parsed.Host = host
		}
		return parsed.String()
	}
	fixture := &chFixture{nativeURI: instance.URI, exec: func(sql string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("clickhouse: %v\n%s", err, sql)
		}
	}}
	chRows := func() string {
		t.Helper()
		var target, control uint64
		for org, n := range map[string]*uint64{targetOrg: &target, controlOrg: &control} {
			if err := conn.QueryRow(ctx, "SELECT count() FROM backfill_log WHERE org_id = ?", org).Scan(n); err != nil {
				t.Fatal(err)
			}
		}
		raw, _ := json.Marshal(map[string]uint64{"target": target, "control": control})
		return string(raw)
	}

	for _, tc := range []struct {
		name   string
		env    map[string]string
		args   []string
		reason string
		secret string
	}{
		{name: "no CLICKHOUSE_URI", args: []string{"--org-id", targetOrg}, reason: "dsn_unset"},
		{name: "no CLICKHOUSE_URI, dry run", args: []string{"--org-id", targetOrg, "--dry-run"}, reason: "dsn_unset"},
		{name: "--analytics-db that cannot connect", args: []string{"--org-id", targetOrg, "--analytics-db", login("orgdel_cli_closed", "orgdel-cli-closed-pw-93fe", "127.0.0.1:1")}, reason: "open_failed", secret: "orgdel-cli-closed-pw-93fe"},
		{name: "login without ALTER DELETE", env: map[string]string{"CLICKHOUSE_URI": login("orgdel_cli_select", selectOnlyPassword, "")}, args: []string{"--org-id", targetOrg}, reason: "privilege_missing", secret: selectOnlyPassword},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db.seedOrganizations(t, false)
			fixture.reseed()
			pgBefore, err := json.Marshal(db.orgRowState(t))
			if err != nil {
				t.Fatal(err)
			}
			chBefore := chRows()

			env := map[string]string{"MIGRATION_DATABASE_URI": db.uri}
			for key, value := range tc.env {
				env[key] = value
			}
			var stdout, stderr bytes.Buffer
			code := runOrgsDelete(ctx, cli.Env{
				Args:   tc.args,
				Stdin:  strings.NewReader(""),
				Lookup: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
				Stdout: &stdout, Stderr: &stderr,
			})
			if code != cli.ExitFailure || stdout.String() != orgDeleteRefusedLine {
				t.Errorf("exit %d stdout %q, want exit %d and %q", code, stdout.String(), cli.ExitFailure, orgDeleteRefusedLine)
			}
			for _, want := range []string{"admin: org deletion refused", "reason=" + tc.reason, "org_id=" + targetOrg} {
				if !strings.Contains(stderr.String(), want) {
					t.Errorf("stderr %q lacks %q", stderr.String(), want)
				}
			}
			if tc.secret != "" && strings.Contains(stdout.String()+stderr.String(), tc.secret) {
				t.Errorf("the ClickHouse password is in the output: %q %q", stdout.String(), stderr.String())
			}
			pgAfter, err := json.Marshal(db.orgRowState(t))
			if err != nil {
				t.Fatal(err)
			}
			if string(pgAfter) != string(pgBefore) {
				t.Errorf("the refused delete changed Postgres rows:\n%s\n%s", pgBefore, pgAfter)
			}
			if chAfter := chRows(); chAfter != chBefore {
				t.Errorf("the refused delete changed ClickHouse rows: %s -> %s", chBefore, chAfter)
			}
		})
	}
}
