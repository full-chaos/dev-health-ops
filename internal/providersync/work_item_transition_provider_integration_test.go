//go:build integration

package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func storedTransitionProvider(ctx context.Context, t *testing.T, conn driver.Conn, orgID, workItemID string) string {
	t.Helper()
	var provider string
	if err := conn.QueryRow(ctx,
		`SELECT provider FROM work_item_transitions FINAL WHERE org_id = ? AND work_item_id = ?`,
		orgID, workItemID,
	).Scan(&provider); err != nil {
		t.Fatalf("read back %s: %v", workItemID, err)
	}
	return provider
}

func transitionEffect(t *testing.T, rows ...any) EffectBatch {
	t.Helper()
	encoded := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded, raw)
	}
	effect, err := BuildEffectBatch("work_item_transitions", EffectReadbackRequired, encoded)
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func gitHubTransitionIdentity(claim Claim, effect EffectBatch) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: "work-items",
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
}

// Every INSERT into work_item_transitions stores the row's provider; the
// stored row is read back with FINAL. One case per path that reaches an
// INSERT: the shared direct statement (GitHub, GitLab, Jira, the prepared
// route) and the Linear statement (direct and prepared).
func TestEveryTransitionInsertSiteStoresTheRowProvider(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	transition := func(claim Claim, workItemID string) githubWorkItemTransitionRow {
		row := workItemTransitionTestRow(claim.OrgID, now)
		row.WorkItemID, row.Provider = workItemID, claim.Provider
		return row
	}
	direct := GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}

	t.Run("github", func(t *testing.T) {
		claim := nativeTestClaim("github", "work-items")
		effect := transitionEffect(t, transition(claim, "gh:acme/api#1"))
		if err := direct.WriteGitHubWorkItemEffect(ctx, gitHubTransitionIdentity(claim, effect), effect); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := storedTransitionProvider(ctx, t, conn, claim.OrgID, "gh:acme/api#1"); got != "github" {
			t.Fatalf("stored provider=%q, want github", got)
		}
	})

	t.Run("gitlab", func(t *testing.T) {
		claim := nativeTestClaim("gitlab", "work-items")
		effect := transitionEffect(t, transition(claim, "gl:acme/api#1"))
		identity, err := newGitLabWorkItemEffectIdentity(claim, effect)
		if err != nil {
			t.Fatal(err)
		}
		adapter := GitLabWorkItemTransitionsClickHouseAdapter{Delegate: direct}
		if err := adapter.WriteGitLabWorkItemEffect(ctx, identity, effect); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := storedTransitionProvider(ctx, t, conn, claim.OrgID, "gl:acme/api#1"); got != "gitlab" {
			t.Fatalf("stored provider=%q, want gitlab", got)
		}
	})

	t.Run("jira", func(t *testing.T) {
		claim := nativeTestClaim("jira", "work-items")
		effect := transitionEffect(t, transition(claim, "jira:OPS-1"))
		if err := direct.WriteGitHubWorkItemEffect(ctx, gitHubTransitionIdentity(claim, effect), effect); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := storedTransitionProvider(ctx, t, conn, claim.OrgID, "jira:OPS-1"); got != "jira" {
			t.Fatalf("stored provider=%q, want jira", got)
		}
	})

	t.Run("prepared", func(t *testing.T) {
		claim := nativeTestClaim("github", "work-items")
		projected, err := projectPreparedRouteEffects("github", []EffectBatch{transitionEffect(t, transition(claim, "gh:acme/api#2"))})
		if err != nil {
			t.Fatalf("project: %v", err)
		}
		effect := projected[0]
		if err := direct.WriteGitHubWorkItemEffect(ctx, gitHubTransitionIdentity(claim, effect), effect); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := storedTransitionProvider(ctx, t, conn, claim.OrgID, "gh:acme/api#2"); got != "github" {
			t.Fatalf("stored provider=%q, want github", got)
		}
	})

	linear := LinearWorkItemTransitionsClickHouseAdapter{Conn: conn}
	linearRow := func(claim Claim, workItemID string) linearWorkItemTransitionRow {
		return linearWorkItemTransitionRow{
			WorkItemID: workItemID, Provider: "linear", OccurredAt: now.Add(-time.Hour),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID, LastSynced: now,
		}
	}
	for _, prepared := range []bool{false, true} {
		name, workItemID := "linear", "linear:ENG-1"
		if prepared {
			name, workItemID = "linear_prepared", "linear:ENG-2"
		}
		t.Run(name, func(t *testing.T) {
			claim := nativeTestClaim("linear", "work-items")
			effect := transitionEffect(t, linearRow(claim, workItemID))
			if prepared {
				projected, err := projectPreparedRouteEffects("linear", []EffectBatch{effect})
				if err != nil {
					t.Fatalf("project: %v", err)
				}
				effect = projected[0]
			}
			identity, err := newLinearWorkItemEffectIdentity(claim, effect)
			if err != nil {
				t.Fatal(err)
			}
			if err := linear.WriteLinearWorkItemEffect(ctx, identity, effect); err != nil {
				t.Fatalf("write: %v", err)
			}
			if got := storedTransitionProvider(ctx, t, conn, claim.OrgID, workItemID); got != "linear" {
				t.Fatalf("stored provider=%q, want linear", got)
			}
		})
	}
}

// The shared direct transition sink accepts exactly the providers that have a
// work-items capability, and refuses any other value loudly: an error, an
// ERROR log line naming the org and the value, and no stored row.
func TestTransitionSinkRefusesAProviderWithNoWorkItemsCapability(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	claim := nativeTestClaim("github", "work-items")
	direct := GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}
	for _, provider := range []string{"custom", "", "GitHub"} {
		t.Run("refuses_"+provider, func(t *testing.T) {
			var logged bytes.Buffer
			swapDefaultLogger(t, slog.New(slog.NewJSONHandler(&logged, nil)))
			row := workItemTransitionTestRow(claim.OrgID, now)
			row.WorkItemID, row.Provider = "gh:acme/refused#"+provider, provider
			effect := transitionEffect(t, row)
			identity := gitHubTransitionIdentity(claim, effect)
			if err := direct.WriteGitHubWorkItemEffect(ctx, identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write err=%v, want ErrInvalidConfiguration", err)
			}
			if inspection, err := direct.InspectGitHubWorkItemEffect(ctx, identity, effect); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("inspect=%s err=%v, want conflict + ErrInvalidConfiguration", inspection, err)
			}
			line := logged.String()
			if !strings.Contains(line, `"level":"ERROR"`) || !strings.Contains(line, `"org_id":"`+claim.OrgID+`"`) ||
				!strings.Contains(line, `"provider":"`+provider+`"`) {
				t.Fatalf("refusal log %q names no ERROR, org and provider", line)
			}
			var stored uint64
			if err := conn.QueryRow(ctx, `SELECT count() FROM work_item_transitions WHERE work_item_id = ?`, row.WorkItemID).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if stored != 0 {
				t.Fatalf("refused row stored %d times", stored)
			}
		})
	}
	for provider := range datasetCapabilities {
		if _, ok := Capability(provider, "work-items"); !ok {
			continue
		}
		t.Run("accepts_"+provider, func(t *testing.T) {
			row := workItemTransitionTestRow(claim.OrgID, now)
			row.WorkItemID, row.Provider = provider+":accepted#1", provider
			effect := transitionEffect(t, row)
			if err := direct.WriteGitHubWorkItemEffect(ctx, gitHubTransitionIdentity(claim, effect), effect); err != nil {
				t.Fatalf("write: %v", err)
			}
			if got := storedTransitionProvider(ctx, t, conn, claim.OrgID, row.WorkItemID); got != provider {
				t.Fatalf("stored provider=%q, want %q", got, provider)
			}
		})
	}
}
