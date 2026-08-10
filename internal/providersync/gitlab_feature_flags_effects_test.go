package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabFeatureFlagsEffectsAcceptsLegitimateEmptyEffects(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("gitlab", "feature-flags")
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink := GitLabFeatureFlagsClickHouseEffects{Lease: lease}

	for _, test := range []struct {
		destination string
		recovery    EffectRecoveryPolicy
	}{
		{destination: "feature_flag", recovery: EffectReplaySafe},
		{destination: "feature_flag_event", recovery: EffectReadbackRequired},
		{destination: "work_graph_edges", recovery: EffectReplaySafe},
	} {
		t.Run(test.destination, func(t *testing.T) {
			empty, err := effectBatchFromValues(test.destination, test.recovery, []any{})
			if err != nil {
				t.Fatal(err)
			}
			if err := sink.WriteEffect(ctx, claim, empty); err != nil {
				t.Fatalf("empty write error=%v", err)
			}
			inspection, err := sink.InspectEffect(ctx, claim, empty)
			if err != nil || inspection != EffectAbsent {
				t.Fatalf("empty inspection=%s error=%v", inspection, err)
			}
		})
	}

	githubClaim := nativeTestClaim("github", "feature-flags")
	empty, err := effectBatchFromValues("feature_flag", EffectReplaySafe, []any{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.WriteEffect(ctx, githubClaim, empty); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("GitLab sink accepted GitHub claim: %v", err)
	}
}

func TestGitLabFeatureFlagsEffectsRejectProviderDatasetAndDestinationBeforeClickHouse(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("gitlab", "feature-flags")
	valid, err := effectBatchFromValues(
		"feature_flag",
		EffectReplaySafe,
		[]launchDarklyFlagRow{gitlabFeatureFlagsEffectFlag(claim, "scope")},
	)
	if err != nil {
		t.Fatal(err)
	}
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })

	// Use another provider that legitimately supports the feature-flags
	// dataset. A github claim would fail Claim.Validate before the sink's
	// provider guard, so it cannot observe that guard's behavior.
	wrongProvider := claim
	wrongProvider.Provider = "launchdarkly"
	tests := []struct {
		name   string
		claim  Claim
		effect EffectBatch
	}{
		{name: "provider", claim: wrongProvider, effect: valid},
		{name: "dataset", claim: nativeTestClaim("gitlab", "commits"), effect: valid},
		{name: "destination", claim: claim, effect: func() EffectBatch {
			wrong := valid
			wrong.Destination = "feature_flag_link"
			return wrong
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := &gitlabFeatureFlagsEffectsRecordingConn{}
			sink := GitLabFeatureFlagsClickHouseEffects{Conn: conn, Lease: lease}
			if err := sink.WriteEffect(ctx, test.claim, test.effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write error=%v", err)
			}
			inspection, err := sink.InspectEffect(ctx, test.claim, test.effect)
			if !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("inspection=%s error=%v", inspection, err)
			}
			if conn.prepares != 0 || conn.queries != 0 {
				t.Fatalf("invalid request reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
			}
		})
	}

	linkEffect, err := effectBatchFromValues(
		"feature_flag_link",
		EffectReplaySafe,
		[]launchDarklyLinkRow{{OrgID: claim.OrgID, Provider: "gitlab"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := (GitLabFeatureFlagsClickHouseEffects{Lease: lease}).WriteEffect(ctx, claim, linkEffect); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("GitLab sink accepted forbidden link destination: %v", err)
	}
}

func TestGitLabFeatureFlagsEffectsFenceTenantAndLeaseBeforeClickHouse(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("gitlab", "feature-flags")
	validEffect, err := effectBatchFromValues(
		"feature_flag",
		EffectReplaySafe,
		[]launchDarklyFlagRow{gitlabFeatureFlagsEffectFlag(claim, "tenant")},
	)
	if err != nil {
		t.Fatal(err)
	}
	validLease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })

	t.Run("forged org is rejected before ClickHouse", func(t *testing.T) {
		forged := gitlabFeatureFlagsEffectFlag(claim, "forged")
		forged.OrgID = "org-forged"
		forgedEffect, err := effectBatchFromValues("feature_flag", EffectReplaySafe, []launchDarklyFlagRow{forged})
		if err != nil {
			t.Fatal(err)
		}
		conn := &gitlabFeatureFlagsEffectsRecordingConn{}
		sink := GitLabFeatureFlagsClickHouseEffects{Conn: conn, Lease: validLease}

		if err := sink.WriteEffect(ctx, claim, forgedEffect); !errors.Is(err, providerfoundation.ErrInvalidScope) {
			t.Fatalf("forged write error=%v", err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, forgedEffect)
		if !errors.Is(err, providerfoundation.ErrInvalidScope) || inspection != EffectConflict {
			t.Fatalf("forged inspection=%s error=%v", inspection, err)
		}
		if conn.prepares != 0 || conn.queries != 0 {
			t.Fatalf("forged tenant reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
		}
	})

	t.Run("lost lease prevents prepare and query", func(t *testing.T) {
		conn := &gitlabFeatureFlagsEffectsRecordingConn{}
		lost := providerfoundation.LeaseGuardFunc(func(context.Context) error {
			return providerfoundation.ErrLeaseLost
		})
		sink := GitLabFeatureFlagsClickHouseEffects{Conn: conn, Lease: lost}

		if err := sink.WriteEffect(ctx, claim, validEffect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("pre-prepare lease error=%v", err)
		}
		inspection, err := sink.InspectEffect(ctx, claim, validEffect)
		if !errors.Is(err, providerfoundation.ErrLeaseLost) || inspection != EffectConflict {
			t.Fatalf("pre-query inspection=%s error=%v", inspection, err)
		}
		if conn.prepares != 0 || conn.queries != 0 {
			t.Fatalf("lost lease reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
		}
	})

	t.Run("second lease assertion prevents send", func(t *testing.T) {
		conn := &gitlabFeatureFlagsEffectsRecordingConn{}
		lostAfterAppend := &gitlabFeatureFlagsEffectsSecondAssertionLosesLease{}
		sink := GitLabFeatureFlagsClickHouseEffects{Conn: conn, Lease: lostAfterAppend}

		if err := sink.WriteEffect(ctx, claim, validEffect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("pre-send lease error=%v", err)
		}
		if lostAfterAppend.calls != 2 || conn.prepares != 1 || conn.batch == nil ||
			conn.batch.appends != 1 || conn.batch.sends != 0 || conn.batch.aborts != 1 {
			t.Fatalf(
				"pre-send fence calls=%d prepares=%d batch=%+v",
				lostAfterAppend.calls, conn.prepares, conn.batch,
			)
		}
	})

	t.Run("valid GitLab effect is written", func(t *testing.T) {
		conn := &gitlabFeatureFlagsEffectsRecordingConn{}
		sink := GitLabFeatureFlagsClickHouseEffects{Conn: conn, Lease: validLease}
		if err := sink.WriteEffect(ctx, claim, validEffect); err != nil {
			t.Fatalf("valid GitLab write error=%v", err)
		}
		if conn.prepares != 1 || conn.batch == nil || conn.batch.appends != 1 || conn.batch.sends != 1 {
			t.Fatalf("GitLab write prepares=%d batch=%+v", conn.prepares, conn.batch)
		}
	})
}

func TestGitLabFeatureFlagsEffectsPreserveNullableStringAndTimeValues(t *testing.T) {
	claim := nativeTestClaim("gitlab", "feature-flags")
	now := time.Date(2026, 8, 10, 1, 2, 3, 456000000, time.UTC)
	archived := now.Add(-time.Hour)
	flag := gitlabFeatureFlagsEffectFlag(claim, "nullable")
	flag.RepoID = ""
	flag.ArchivedAt = &archived
	flag.CreatedAt = &now
	flag.LastSynced = now
	flagEffect, err := effectBatchFromValues("feature_flag", EffectReplaySafe, []launchDarklyFlagRow{flag})
	if err != nil {
		t.Fatal(err)
	}

	edge := launchDarklyEdgeRow{
		EdgeID: "edge-nullable", SourceType: "feature_flag", SourceID: "flag", TargetType: "repo",
		TargetID: "repo", EdgeType: "references", RepoID: "", Provider: "gitlab",
		Provenance: "native", Confidence: 0.875, Evidence: "flag", DiscoveredAt: now,
		LastSynced: now, EventAt: now, Day: "2026-08-10", OrgID: claim.OrgID,
	}
	edgeEffect, err := effectBatchFromValues("work_graph_edges", EffectReplaySafe, []launchDarklyEdgeRow{edge})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		name   string
		effect EffectBatch
		check  func(t *testing.T, values []any)
	}{
		{
			name:   "feature flag nullable time",
			effect: flagEffect,
			check: func(t *testing.T, values []any) {
				if len(values) != 10 {
					t.Fatalf("append values=%d want 10: %#v", len(values), values)
				}
				if got, ok := values[8].(*time.Time); !ok || got == nil || !got.Equal(archived) {
					t.Fatalf("archived_at=%T %#v", values[8], values[8])
				}
				if got, ok := values[9].(time.Time); !ok || !got.Equal(now) {
					t.Fatalf("last_synced=%T %#v", values[9], values[9])
				}
			},
		},
		{
			name:   "work graph nullable repo and times",
			effect: edgeEffect,
			check: func(t *testing.T, values []any) {
				if len(values) != 16 {
					t.Fatalf("append values=%d want 16: %#v", len(values), values)
				}
				if values[6] != nil {
					t.Fatalf("empty nullable repo_id=%#v want nil", values[6])
				}
				if got, ok := values[11].(time.Time); !ok || !got.Equal(now) {
					t.Fatalf("discovered_at=%T %#v", values[11], values[11])
				}
				if got, ok := values[13].(time.Time); !ok || !got.Equal(now) {
					t.Fatalf("event_ts=%T %#v", values[13], values[13])
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &gitlabFeatureFlagsEffectsRecordingConn{}
			sink := GitLabFeatureFlagsClickHouseEffects{
				Conn:  conn,
				Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
			}
			if err := sink.WriteEffect(context.Background(), claim, test.effect); err != nil {
				t.Fatal(err)
			}
			test.check(t, conn.batch.values[0])
		})
	}
}

func gitlabFeatureFlagsEffectFlag(claim Claim, suffix string) launchDarklyFlagRow {
	now := time.Date(2026, 8, 10, 1, 2, 3, 0, time.UTC)
	return launchDarklyFlagRow{
		OrgID: claim.OrgID, Provider: "gitlab", FlagKey: "flag-" + suffix,
		ProjectKey: "group/project", RepoID: "repo-" + suffix, Environment: "production",
		FlagType: "new_version_flag", CreatedAt: &now, LastSynced: now,
	}
}

type gitlabFeatureFlagsEffectsRecordingConn struct {
	driver.Conn
	prepares int
	queries  int
	batch    *gitlabFeatureFlagsEffectsRecordingBatch
}

func (conn *gitlabFeatureFlagsEffectsRecordingConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	conn.prepares++
	conn.batch = &gitlabFeatureFlagsEffectsRecordingBatch{}
	return conn.batch, nil
}

func (conn *gitlabFeatureFlagsEffectsRecordingConn) Query(
	context.Context, string, ...any,
) (driver.Rows, error) {
	conn.queries++
	return emptyGitLabFeatureFlagsEffectsRows{}, nil
}

type gitlabFeatureFlagsEffectsRecordingBatch struct {
	driver.Batch
	appends int
	sends   int
	aborts  int
	values  [][]any
}

func (batch *gitlabFeatureFlagsEffectsRecordingBatch) Append(values ...any) error {
	batch.appends++
	batch.values = append(batch.values, append([]any(nil), values...))
	return nil
}

func (batch *gitlabFeatureFlagsEffectsRecordingBatch) Send() error {
	batch.sends++
	return nil
}

func (batch *gitlabFeatureFlagsEffectsRecordingBatch) Abort() error {
	batch.aborts++
	return nil
}

type emptyGitLabFeatureFlagsEffectsRows struct{}

func (emptyGitLabFeatureFlagsEffectsRows) Next() bool                       { return false }
func (emptyGitLabFeatureFlagsEffectsRows) Scan(...any) error                { return nil }
func (emptyGitLabFeatureFlagsEffectsRows) ScanStruct(any) error             { return nil }
func (emptyGitLabFeatureFlagsEffectsRows) ColumnTypes() []driver.ColumnType { return nil }
func (emptyGitLabFeatureFlagsEffectsRows) Totals(...any) error              { return nil }
func (emptyGitLabFeatureFlagsEffectsRows) Columns() []string                { return nil }
func (emptyGitLabFeatureFlagsEffectsRows) Close() error                     { return nil }
func (emptyGitLabFeatureFlagsEffectsRows) Err() error                       { return nil }
func (emptyGitLabFeatureFlagsEffectsRows) HasData() bool                    { return false }

type gitlabFeatureFlagsEffectsSecondAssertionLosesLease struct{ calls int }

func (guard *gitlabFeatureFlagsEffectsSecondAssertionLosesLease) Assert(context.Context) error {
	guard.calls++
	if guard.calls == 2 {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}
