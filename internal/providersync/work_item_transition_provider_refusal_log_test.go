package providersync

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// The GitLab and Linear transition sinks refuse a row of another provider
// before any ClickHouse I/O, and every refusal writes the shared ERROR line
// with the org and the refused provider value.
func TestGitLabAndLinearTransitionSinksLogTheRefusedProvider(t *testing.T) {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	expectRefusalLogged := func(t *testing.T, logged *bytes.Buffer, orgID, provider string) {
		t.Helper()
		line := logged.String()
		logged.Reset()
		if !strings.Contains(line, `"level":"ERROR"`) ||
			!strings.Contains(line, `"msg":"provider_sync.work_item_transition_provider_refused"`) ||
			!strings.Contains(line, `"org_id":"`+orgID+`"`) ||
			!strings.Contains(line, `"provider":"`+provider+`"`) {
			t.Fatalf("refusal log %q names no ERROR, org %q and provider %q", line, orgID, provider)
		}
	}
	encode := func(t *testing.T, row any) EffectBatch {
		t.Helper()
		raw, err := json.Marshal(row)
		if err != nil {
			t.Fatal(err)
		}
		effect, err := BuildEffectBatch("work_item_transitions", EffectReadbackRequired, []json.RawMessage{raw})
		if err != nil {
			t.Fatal(err)
		}
		return effect
	}

	for _, provider := range []string{"", "custom", "pagerduty", "GitLab", "linear"} {
		t.Run("gitlab/"+provider, func(t *testing.T) {
			var logged bytes.Buffer
			swapDefaultLogger(t, slog.New(slog.NewJSONHandler(&logged, nil)))
			claim := nativeTestClaim("gitlab", "work-items")
			effect := encode(t, gitlabWorkItemTransitionRow{
				WorkItemID: "gl:acme/api#1", Provider: provider, OccurredAt: now.Add(-time.Hour),
				FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID, LastSynced: now,
			})
			identity, err := newGitLabWorkItemEffectIdentity(claim, effect)
			if err != nil {
				t.Fatal(err)
			}
			adapter := GitLabWorkItemTransitionsClickHouseAdapter{Delegate: GitHubWorkItemTransitionsClickHouseAdapter{Conn: unreachableConn{t}}}
			if err := adapter.WriteGitLabWorkItemEffect(t.Context(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write err=%v, want ErrInvalidConfiguration", err)
			}
			expectRefusalLogged(t, &logged, claim.OrgID, provider)
			if inspection, err := adapter.InspectGitLabWorkItemEffect(t.Context(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("inspect=%s err=%v, want conflict + ErrInvalidConfiguration", inspection, err)
			}
			expectRefusalLogged(t, &logged, claim.OrgID, provider)
		})
	}

	for _, provider := range []string{"", "custom", "pagerduty", "Linear", "gitlab"} {
		t.Run("linear/"+provider, func(t *testing.T) {
			var logged bytes.Buffer
			swapDefaultLogger(t, slog.New(slog.NewJSONHandler(&logged, nil)))
			claim := nativeTestClaim("linear", "work-items")
			effect := encode(t, linearWorkItemTransitionRow{
				WorkItemID: "linear:ENG-1", Provider: provider, OccurredAt: now.Add(-time.Hour),
				FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID, LastSynced: now,
			})
			identity, err := newLinearWorkItemEffectIdentity(claim, effect)
			if err != nil {
				t.Fatal(err)
			}
			adapter := LinearWorkItemTransitionsClickHouseAdapter{Conn: unreachableConn{t}}
			if err := adapter.WriteLinearWorkItemEffect(t.Context(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write err=%v, want ErrInvalidConfiguration", err)
			}
			expectRefusalLogged(t, &logged, claim.OrgID, provider)
			if inspection, err := adapter.InspectLinearWorkItemEffect(t.Context(), identity, effect); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("inspect=%s err=%v, want conflict + ErrInvalidConfiguration", inspection, err)
			}
			expectRefusalLogged(t, &logged, claim.OrgID, provider)
		})
	}
}
