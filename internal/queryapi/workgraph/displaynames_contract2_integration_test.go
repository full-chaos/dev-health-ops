//go:build integration

package workgraph

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/operationalordering"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/opfixture"
)

// The query API runs with OPERATIONAL_ORDERING_CONTRACT unset (the chart exports
// it to the workers only) against contract-2 tables, so an unset variable must
// read the current row by revision. Fixture:
//
//	I1  a live version and a newer tombstone -> no name
//	I2  two live versions                    -> the newest title
func TestResolveIncidentDisplayNamesReadsTheCurrentRowOnContract2(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	instance, conn := opfixture.Start(ctx, t)
	const org = "org-c2"
	at := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	put := func(id, title string, revision int, deleted bool) {
		opfixture.InsertIncident(ctx, t, conn, opfixture.Incident{Org: org, ID: id, ServiceID: "s", Status: "open", Title: title, Revision: revision, Deleted: deleted, StartedAt: at})
	}
	put("I1", "first", 1, false)
	put("I1", "first", 2, true)
	put("I2", "old title", 1, false)
	put("I2", "new title", 2, false)

	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	ids := map[string]struct{}{"I1": {}, "I2": {}}

	run := func(value string, set bool) map[string]string {
		if set {
			t.Setenv(operationalordering.Env, value)
		} else {
			t.Setenv(operationalordering.Env, "")
			if err := os.Unsetenv(operationalordering.Env); err != nil {
				t.Fatal(err)
			}
		}
		resolved := map[string]string{}
		resolveIncidentDisplayNames(ctx, client, org, ids, resolved)
		return resolved
	}
	for name, tc := range map[string]struct {
		value string
		set   bool
	}{"unset": {"", false}, "two": {"2", true}} {
		got := run(tc.value, tc.set)
		if _, deleted := got["I1"]; deleted {
			t.Errorf("%s: the tombstoned incident resolved to %q", name, got["I1"])
		}
		if !strings.HasPrefix(got["I2"], "new title") {
			t.Errorf("%s: I2 resolved to %q, want the newest version's title", name, got["I2"])
		}
	}
	// Contract 1 and any other value are refused loudly: nothing resolves.
	for _, value := range []string{"1", "", "3"} {
		if got := run(value, true); len(got) != 0 {
			t.Errorf("OPERATIONAL_ORDERING_CONTRACT=%q resolved %v, want a refusal", value, got)
		}
	}
}
