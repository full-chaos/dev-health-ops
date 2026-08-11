package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

func TestGitLabWorkItemFamilyEffectsComposeAllSixteenDestinations(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	if missing := sink.MissingDestinations(); len(missing) != 0 {
		t.Fatalf("missing=%v", missing)
	}
	raw, err := BuildGitLabWorkItemEffects(GitLabWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	derived, err := BuildGitLabWorkItemDerivedEffects(GitLabWorkItemDerivedEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	effects := append(raw, derived...)
	canonical := workItemRouteDestinations()
	if len(effects) != len(canonical) || len(effects) != 16 {
		t.Fatalf("effects=%d canonical=%d", len(effects), len(canonical))
	}
	seen := make(map[string]struct{}, len(effects))
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	for _, effect := range effects {
		if _, duplicate := seen[effect.Destination]; duplicate {
			t.Fatalf("duplicate destination %q", effect.Destination)
		}
		seen[effect.Destination] = struct{}{}
		inspection, inspectErr := sink.InspectEffect(context.Background(), claim, effect)
		if inspectErr != nil || inspection != EffectAbsent {
			t.Fatalf("%s empty readback=%s error=%v", effect.Destination, inspection, inspectErr)
		}
		if writeErr := sink.WriteEffect(context.Background(), claim, effect); writeErr != nil {
			t.Fatalf("%s empty write: %v", effect.Destination, writeErr)
		}
	}
	for _, destination := range canonical {
		if _, present := seen[destination]; !present {
			t.Fatalf("canonical destination %q was not composed", destination)
		}
	}
}

func TestGitLabWorkItemFamilyEffectsFailClosedWhenAnyAdapterIsMissing(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	sink.Derived.AIAttribution.Conn = nil
	missing := sink.MissingDestinations()
	if len(missing) != 1 || missing[0] != "ai_attribution" {
		t.Fatalf("missing=%v", missing)
	}
	raw, err := BuildGitLabWorkItemEffects(GitLabWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	if err := sink.WriteEffect(context.Background(), claim, raw[0]); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("partial family write error=%v", err)
	}
	if inspection, err := sink.InspectEffect(
		context.Background(), claim, raw[0],
	); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
		t.Fatalf("partial family readback=%s error=%v", inspection, err)
	}
}

func TestGitLabWorkItemFamilyEffectsRejectForeignAIProviderAndTenant(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease)
	if err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	base := gitlabAIAttributionRow{
		RecordID: uuid.NewSHA1(uuid.NameSpaceURL, []byte("gitlab-ai-tenant-fence")),
		OrgID:    uuid.MustParse(claim.OrgID), Provider: "gitlab", SubjectType: "pull_request",
		SubjectID: "9", RepoID: &repoID, Kind: "ai_assisted", Source: "pr_label",
		Confidence: 0.95, Evidence: map[string]any{"label": "codex"},
		ObservedAt: now, IngestedAt: now,
	}
	assertRejected := func(t *testing.T, row gitlabAIAttributionRow) {
		t.Helper()
		effects, err := BuildGitLabWorkItemDerivedEffects(GitLabWorkItemDerivedEffectRows{
			AIAttributions: []gitlabAIAttributionRow{row},
		})
		if err != nil {
			t.Fatal(err)
		}
		aiEffect := effects[0]
		identity, err := newGitLabWorkItemDerivedEffectIdentity(claim, aiEffect)
		if err != nil {
			t.Fatal(err)
		}
		if validGitLabAIAttributionEffect(gitlabDerivedGitHubIdentity(identity), aiEffect) {
			t.Fatal("foreign AI row passed the provider-local semantic fence")
		}
		if err := sink.WriteEffect(context.Background(), claim, aiEffect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("write error=%v", err)
		}
		if inspection, err := sink.InspectEffect(context.Background(), claim, aiEffect); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
			t.Fatalf("readback=%s error=%v", inspection, err)
		}
	}
	foreignProvider := base
	foreignProvider.Provider = "github"
	t.Run("provider", func(t *testing.T) { assertRejected(t, foreignProvider) })
	foreignTenant := base
	foreignTenant.OrgID = uuid.MustParse("88888888-8888-4888-8888-888888888888")
	t.Run("tenant", func(t *testing.T) { assertRejected(t, foreignTenant) })
}
