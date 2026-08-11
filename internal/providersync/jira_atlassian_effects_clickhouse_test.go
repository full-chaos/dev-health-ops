package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type jiraAtlassianRecordingWorklogAdapter struct {
	writes, inspections int
}

func (adapter *jiraAtlassianRecordingWorklogAdapter) WriteJiraAtlassianEffect(_ context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) error {
	if identity.Destination != "worklogs" || effect.Destination != "worklogs" || identity.Provider != "jira" {
		return ErrInvalidConfiguration
	}
	adapter.writes++
	return nil
}

func (adapter *jiraAtlassianRecordingWorklogAdapter) InspectJiraAtlassianEffect(_ context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) (EffectInspection, error) {
	if identity.Destination != "worklogs" || effect.Destination != "worklogs" || identity.Provider != "jira" {
		return EffectConflict, ErrInvalidConfiguration
	}
	adapter.inspections++
	return EffectExact, nil
}

func (adapter *jiraAtlassianRecordingWorklogAdapter) WriteJiraWorklogEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) error {
	return adapter.WriteJiraAtlassianEffect(ctx, identity, effect)
}

func (adapter *jiraAtlassianRecordingWorklogAdapter) InspectJiraWorklogEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) (EffectInspection, error) {
	return adapter.InspectJiraAtlassianEffect(ctx, identity, effect)
}

func TestBuildJiraAtlassianEffectsRoutesEveryDestinationAndFencesWorklogs(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	now := time.Date(2026, 8, 10, 12, 0, 0, 123456000, time.UTC)
	row := jiraWorklogRow{WorkItemID: "jira:OPS-1", Provider: "jira", WorklogID: "wl-1", StartedAt: now.Add(-time.Hour), TimeSpentSeconds: 90, CreatedAt: now.Add(-time.Minute), UpdatedAt: now, LastSynced: now, OrgID: claim.OrgID}
	effects, err := BuildJiraAtlassianEffects(jiraAtlassianRows{Worklogs: []jiraWorklogRow{row}})
	if err != nil {
		t.Fatal(err)
	}
	want := JiraAtlassianEffectDestinations()
	got := make([]string, 0, len(effects))
	for _, effect := range effects {
		got = append(got, effect.Destination)
		if effect.Recovery != EffectReadbackRequired {
			t.Fatalf("effect=%+v", effect)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("destinations=%v want=%v", got, want)
	}

	legacy := &jiraRecordingEffectAdapter{}
	worklogs := &jiraAtlassianRecordingWorklogAdapter{}
	leaseCalls := 0
	sink := JiraAtlassianClickHouseEffects{
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { leaseCalls++; return nil }),
		Sprints: legacy, Dependencies: legacy, Interactions: legacy, Reopens: legacy, Transitions: legacy, WorkItems: legacy, Worklogs: worklogs,
	}
	for _, effect := range effects {
		if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		if inspection, err := sink.InspectEffect(context.Background(), claim, effect); err != nil || inspection != EffectExact {
			t.Fatalf("inspect %s=%s err=%v", effect.Destination, inspection, err)
		}
	}
	if worklogs.writes != 1 || worklogs.inspections != 1 || leaseCalls != len(effects)*3 {
		t.Fatalf("worklogs=%+v leaseCalls=%d effects=%d", worklogs, leaseCalls, len(effects))
	}

	foreign := row
	foreign.OrgID = "org-other"
	raw, err := json.Marshal(foreign)
	if err != nil {
		t.Fatal(err)
	}
	foreignEffect, err := BuildEffectBatch("worklogs", EffectReadbackRequired, []json.RawMessage{raw})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(context.Background(), claim, foreignEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("foreign worklog accepted: %v", err)
	}

	incomplete := sink
	incomplete.Worklogs = nil
	if missing := incomplete.MissingDestinations(); len(missing) != 1 || missing[0] != "worklogs" {
		t.Fatalf("missing=%v", missing)
	}
	if err := incomplete.WriteEffect(context.Background(), claim, effects[len(effects)-1]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("missing worklog adapter error=%v", err)
	}
}

var _ JiraAtlassianWorklogEffectAdapter = (*jiraAtlassianRecordingWorklogAdapter)(nil)
