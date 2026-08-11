//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// This is intentionally an integration test over the migrated `worklogs`
// table, not a recording-adapter test. It proves the three recovery properties
// that a unit fake cannot establish: the DateTime64(6) row survives a real
// write, an acknowledgement crash is reconciled by FINAL readback, and tenant
// and lease fences run before/after the real ClickHouse operation.
func TestJiraAtlassianWorklogCrashReadbackTenantAndLease(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	claim := nativeTestClaim("jira", "work-items")
	now := time.Date(2026, 8, 10, 12, 0, 0, 123456789, time.FixedZone("test-west", -7*60*60))
	row := jiraWorklogRow{
		WorkItemID: "jira:OPS-201", Provider: "jira", WorklogID: "wl-crash",
		Author: stringPointer("jira:accountid:worker"), StartedAt: now.Add(-time.Hour),
		TimeSpentSeconds: 2700, CreatedAt: now.Add(-59 * time.Minute),
		UpdatedAt: now.Add(-58 * time.Minute), LastSynced: now, OrgID: claim.OrgID,
	}
	raw, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch("worklogs", EffectReadbackRequired, []json.RawMessage{raw})
	if err != nil {
		t.Fatal(err)
	}

	adapter := JiraWorklogsClickHouseAdapter{Conn: conn}
	base := JiraAtlassianClickHouseEffects{
		Lease:    providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
		Worklogs: adapter,
	}
	crash := errors.New("simulated acknowledgement crash after real worklog write")
	first := crashAfterJiraAtlassianWrite{sink: base, destination: "worklogs", failure: crash}
	ledger := &memoryEffectLedger{}
	if _, err := (EffectCommitter{Ledger: ledger, Sink: first, Readback: first, Now: func() time.Time { return now }}).Commit(ctx, claim, []EffectBatch{effect}, now); !errors.Is(err, crash) {
		t.Fatalf("first commit error=%v, want acknowledgement crash", err)
	}
	if inspection, err := adapter.InspectJiraWorklogEffect(ctx, JiraWorkItemEffectIdentity{OrgID: claim.OrgID, Provider: "jira", Dataset: "work-items", Generation: claim.GenerationKey(), Destination: "worklogs", ContentDigest: effect.ContentDigest, RowCount: 1}, effect); err != nil || inspection != EffectExact {
		t.Fatalf("real ClickHouse readback after crash=%s err=%v", inspection, err)
	}

	second := crashAfterJiraAtlassianWrite{sink: base, destination: "never", failure: crash}
	result, err := (EffectCommitter{Ledger: ledger, Sink: second, Readback: second, Now: func() time.Time { return now.Add(time.Minute) }}).Commit(ctx, claim, []EffectBatch{effect}, now)
	if err != nil || result.MarkedCommitted != 1 || result.Written != 0 {
		t.Fatalf("recovery result=%+v err=%v", result, err)
	}

	foreign := row
	foreign.OrgID = "org-other"
	foreignRaw, err := json.Marshal(foreign)
	if err != nil {
		t.Fatal(err)
	}
	foreignEffect, err := BuildEffectBatch("worklogs", EffectReadbackRequired, []json.RawMessage{foreignRaw})
	if err != nil {
		t.Fatal(err)
	}
	if err := base.WriteEffect(ctx, claim, foreignEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign-tenant worklog accepted: %v", err)
	}

	leaseLost := JiraAtlassianClickHouseEffects{Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return providerfoundation.ErrLeaseLost }), Worklogs: adapter}
	if err := leaseLost.WriteEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
		t.Fatalf("lease-lost worklog write=%v", err)
	}
}

type crashAfterJiraAtlassianWrite struct {
	sink        JiraAtlassianClickHouseEffects
	destination string
	failure     error
}

func (sink crashAfterJiraAtlassianWrite) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	if err := sink.sink.WriteEffect(ctx, claim, effect); err != nil {
		return err
	}
	if effect.Destination == sink.destination {
		return sink.failure
	}
	return nil
}

func (sink crashAfterJiraAtlassianWrite) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	return sink.sink.InspectEffect(ctx, claim, effect)
}

var _ EffectSink = crashAfterJiraAtlassianWrite{}
var _ EffectReadback = crashAfterJiraAtlassianWrite{}
