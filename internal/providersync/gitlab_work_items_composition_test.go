package providersync

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// The sync unit of GitLab composes the six raw tables and ai_attribution, and
// its sink refuses each of the nine tables the daily job computes from stored
// rows: a prepared effect for one of them is a configuration error on write
// and a conflict on readback, never a store call.
func TestGitLabWorkItemFamilyEffectsComposeRawAndAIAttributionOnly(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease, nil)
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
	canonical := workItemRouteDestinations()
	if len(canonical) != 7 {
		t.Fatalf("canonical=%v want the six raw tables and ai_attribution", canonical)
	}
	seen := make(map[string]struct{}, len(canonical))
	refused := make(map[string]struct{}, len(githubWorkItemDerivedDestinations))
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	for _, effect := range append(raw, derived...) {
		if slices.Contains(githubWorkItemDerivedDestinations, effect.Destination) {
			if writeErr := sink.WriteEffect(context.Background(), claim, effect); !errors.Is(writeErr, ErrInvalidConfiguration) {
				t.Fatalf("%s: the sync sink wrote a table of the daily job: %v", effect.Destination, writeErr)
			}
			inspection, inspectErr := sink.InspectEffect(context.Background(), claim, effect)
			if !errors.Is(inspectErr, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("%s: readback=%s error=%v want a refused conflict", effect.Destination, inspection, inspectErr)
			}
			refused[effect.Destination] = struct{}{}
			continue
		}
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
	if len(seen) != len(canonical) {
		t.Fatalf("accepted=%v canonical=%v", seen, canonical)
	}
	for _, destination := range canonical {
		if _, present := seen[destination]; !present {
			t.Fatalf("canonical destination %q was not composed", destination)
		}
	}
	for _, destination := range githubWorkItemDerivedDestinations {
		if _, present := refused[destination]; !present {
			t.Fatalf("the refusal of %q was not observed", destination)
		}
	}
}

func TestGitLabWorkItemFamilyEffectsFailClosedWhenAnyAdapterIsMissing(t *testing.T) {
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease, nil)
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
	sink, err := NewGitLabWorkItemFamilyClickHouseEffects(inertGitHubDerivedConn{}, lease, nil)
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
