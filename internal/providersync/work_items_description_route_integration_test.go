//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// A work item a provider sync stores carries the provider's description. The
// route runs as registered (prepared-snapshot recovery, so the commit path
// projects each row onto the keys the sink writes) against a real Postgres
// ledger and a real ClickHouse, and the stored column is read back.
func TestGitLabWorkItemsRouteStoresTheProviderDescription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	familyFlags := map[string]bool{}
	for _, dataset := range workitemcontract.FamilyDatasets() {
		familyFlags[workItemFamilyFlagForDataset(dataset)] = true
	}
	flagsJSON, err := json.Marshal(familyFlags)
	if err != nil {
		t.Fatal(err)
	}
	harness := startProviderLedgerHarness(t, ctx, "gitlab", "work-items", string(flagsJSON))
	descriptor, ok := Descriptor("gitlab", "work-items")
	if !ok || !descriptor.PreparedManifestRecovery {
		t.Fatal("gitlab/work-items is not registered for prepared recovery")
	}
	route := driftRoute{
		provider: "gitlab", dataset: "work-items", flags: string(flagsJSON),
		baseURL: "https://gitlab.com",
		handler: GitLabWorkItemsRouteHandler{
			StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10,
		},
	}
	family, err := NewGitLabWorkItemFamilyClickHouseEffects(
		harness.conn, leaseGuardAt(harness.repository, harness.claim, harness.now), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink := loggingSink{t: t, inner: family}
	session := &LeaseSession{
		Repository: harness.repository, Claim: harness.claim, LeaseDuration: time.Minute,
		Deadline: harness.now.Add(5 * time.Minute), Now: func() time.Time { return harness.now },
	}
	doer := fakehttp.Client(&gitLabWorkItemsDoer{responses: gitLabWorkItemResponses()})
	if _, err := driftExecutor(route, harness, harness.repository, harness.claim, harness.now,
		providerfoundation.HTTPDoer(doer), sink, sink).Execute(ctx, session, descriptor); err != nil {
		t.Fatal(err)
	}

	var description *string
	if err := harness.conn.QueryRow(ctx,
		`SELECT description FROM work_items FINAL WHERE org_id = ? AND provider = 'gitlab' AND title = 'Fix the API'`,
		harness.claim.OrgID,
	).Scan(&description); err != nil {
		t.Fatal(err)
	}
	if want := "blocks #7 and see CHAOS-9"; description == nil || *description != want {
		got := "<null>"
		if description != nil {
			got = *description
		}
		t.Fatalf("stored description=%q want %q", got, want)
	}
}
