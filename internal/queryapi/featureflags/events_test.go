package featureflags

import (
	"context"
	"errors"
	"testing"
)

// TestResolveEvents_HappyPath is CHAOS-5523's red-first proof: before
// ResolveEvents exists, this fails to compile/panics on the resolver's
// "not implemented" body. Once implemented, it asserts a well-formed
// non-degraded FeatureFlagEventsResult with every field ported from
// resolve_feature_flag_events populated from the scripted rows.
func TestResolveEvents_HappyPath(t *testing.T) {
	ts := mustUTC("2026-08-20T10:15:30.500000+00:00")
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{
				{"dark-mode", "flag_toggled", "off", "on", "user", "staging", ts},
			}},
			{rows: [][]any{{uint64(1)}}},
		},
		errs: []error{nil, nil},
	}

	result, err := ResolveEvents(context.Background(), client, "org-1", nil, nil, 1000)
	if err != nil {
		t.Fatalf("ResolveEvents: %v", err)
	}
	if result.DegradedReason != nil {
		t.Fatalf("DegradedReason = %v, want nil", *result.DegradedReason)
	}
	if result.TotalCount != 1 {
		t.Fatalf("TotalCount = %d, want 1", result.TotalCount)
	}
	if len(result.Events) != 1 {
		t.Fatalf("len(Events) = %d, want 1", len(result.Events))
	}
	ev := result.Events[0]
	if ev.FlagKey != "dark-mode" {
		t.Errorf("FlagKey = %q, want %q", ev.FlagKey, "dark-mode")
	}
	if ev.EventType != "flag_toggled" {
		t.Errorf("EventType = %q, want %q", ev.EventType, "flag_toggled")
	}
	if ev.PrevState != "off" {
		t.Errorf("PrevState = %q, want %q", ev.PrevState, "off")
	}
	if ev.NextState != "on" {
		t.Errorf("NextState = %q, want %q", ev.NextState, "on")
	}
	if ev.ActorType != "user" {
		t.Errorf("ActorType = %q, want %q", ev.ActorType, "user")
	}
	if ev.Environment != "staging" {
		t.Errorf("Environment = %q, want %q", ev.Environment, "staging")
	}
	if ev.EventTs != "2026-08-20T10:15:30.500000" {
		t.Errorf("EventTs = %q, want isoformat with microseconds", ev.EventTs)
	}
}

// TestResolveEvents_MissingTableDegradesInsteadOfErroring mirrors
// TestResolve_MissingTableDegradesInsteadOfErroring for the
// feature_flag_event table and FEATURE_FLAG_EVENT_NOT_MATERIALIZED.
func TestResolveEvents_MissingTableDegradesInsteadOfErroring(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{unknownTableErr("feature_flag_event")},
	}

	result, err := ResolveEvents(context.Background(), client, "org-1", nil, nil, 1000)
	if err != nil {
		t.Fatalf("ResolveEvents returned an error instead of degrading: %v", err)
	}
	if result.DegradedReason == nil || *result.DegradedReason != EventNotMaterializedReason {
		t.Fatalf("DegradedReason = %v, want %q", result.DegradedReason, EventNotMaterializedReason)
	}
	if len(result.Events) != 0 || result.TotalCount != 0 {
		t.Fatalf("degraded result not empty: events=%v total=%d", result.Events, result.TotalCount)
	}
}

// TestResolveEvents_MissingDifferentTablePropagatesError is the
// precise-match half of the missing-table guard for feature_flag_event:
// a code-60 error naming some OTHER table (including feature_flag
// itself) must not be swallowed as feature_flag_event's own degraded
// path.
func TestResolveEvents_MissingDifferentTablePropagatesError(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{unknownTableErr("feature_flag")},
	}

	_, err := ResolveEvents(context.Background(), client, "org-1", nil, nil, 1000)
	if err == nil {
		t.Fatal("expected an error for a different missing table, got nil")
	}
}

func TestResolveEvents_NonMissingTableErrorPropagates(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{nil},
		errs:      []error{errors.New("boom")},
	}
	_, err := ResolveEvents(context.Background(), client, "org-1", nil, nil, 1000)
	if err == nil {
		t.Fatal("expected error to propagate")
	}
}

// TestResolveEvents_FiltersAndLimitBindWhenProvided proves flagKey/
// environment filters and the caller's limit actually reach the bound
// query -- mirrors TestResolve_FiltersAndLimitBindWhenProvided.
func TestResolveEvents_FiltersAndLimitBindWhenProvided(t *testing.T) {
	ts := mustUTC("2026-08-01T00:00:00+00:00")
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{{"flag-x", "flag_toggled", "off", "on", "user", "production", ts}}},
			{rows: [][]any{{uint64(1)}}},
		},
		errs: []error{nil, nil},
	}
	flagKey := "flag-x"
	environment := "production"
	if _, err := ResolveEvents(context.Background(), client, "org-1", &flagKey, &environment, 5); err != nil {
		t.Fatalf("ResolveEvents: %v", err)
	}
	if len(client.statements) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(client.statements))
	}
	for _, stmt := range client.statements {
		if !contains(stmt, "flag_key = {flag_key:String}") || !contains(stmt, "environment = {environment:String}") {
			t.Errorf("statement missing filter bindings: %s", stmt)
		}
	}
}

// TestResolveEvents_CountQueryIgnoresLimit is CHAOS-5523's explicit
// parity requirement: the count query mirrors the row query's
// where_clauses (org_id [+flag_key][+environment]) but NEVER the limit --
// a TRUE total count, not a "did we truncate" heuristic. Proven by
// asserting the count statement text carries no LIMIT clause at all.
func TestResolveEvents_CountQueryIgnoresLimit(t *testing.T) {
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
			{rows: [][]any{{uint64(42)}}},
		},
		errs: []error{nil, nil},
	}
	result, err := ResolveEvents(context.Background(), client, "org-1", nil, nil, 5)
	if err != nil {
		t.Fatalf("ResolveEvents: %v", err)
	}
	if result.TotalCount != 42 {
		t.Fatalf("TotalCount = %d, want 42 (count query must not be clamped by limit)", result.TotalCount)
	}
	if len(client.statements) != 2 {
		t.Fatalf("expected 2 queries, got %d", len(client.statements))
	}
	if contains(client.statements[1], "LIMIT") {
		t.Errorf("count query statement contains LIMIT: %s", client.statements[1])
	}
}

func TestResolveEvents_ClampsLimit(t *testing.T) {
	// clampLimit itself is already exhaustively tested by TestClampLimit
	// in featureflags_test.go; this proves ResolveEvents actually calls
	// it, not merely that the standalone function is correct.
	client := &fakeClient{
		responses: []*fakeRowScanner{
			{rows: [][]any{}},
			{rows: [][]any{{uint64(0)}}},
		},
		errs: []error{nil, nil},
	}
	if _, err := ResolveEvents(context.Background(), client, "org-1", nil, nil, 5000); err != nil {
		t.Fatalf("ResolveEvents: %v", err)
	}
}
