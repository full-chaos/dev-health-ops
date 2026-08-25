//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
)

func newProjectMembershipEffectsConn(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

// TestGitHubProjectsV2PullRequestReachesClickHouseThroughTheEffectPath is
// CHAOS-4194's REACHABILITY proof, and the test whose absence let the defect
// exist twice.
//
// The producer's rows were once discarded by the normalizer; that was fixed,
// and they were then discarded again one layer later by the effects builder,
// which serialized seven families and neither of the new ones. Both times every
// test still passed, because every test asserted on the FETCH RESULT -- the one
// place the value provably still exists. So this test deliberately starts where
// those stopped: it takes the fetched rows through `buildGitHubWorkItemsRouteEffects`,
// through the real ClickHouse adapters, and reads the rows back out of the
// migrated tables. Nothing between the provider payload and the stored row is
// stubbed.
//
// It asserts BOTH new families, because they fail independently: a membership
// row whose `projects` row never landed satisfies the vocabulary constraint
// only by accident, and a `projects` row with no membership proves nothing at
// all.
func TestGitHubProjectsV2PullRequestReachesClickHouseThroughTheEffectPath(t *testing.T) {
	t.Run("durable_null_snapshots", testGitHubProjectsV2NullSnapshotsPreserveDurableMembershipAndWatermark)
	ctx, conn := newProjectMembershipEffectsConn(t)
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_PR","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"A PR","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	fetched, err := (GitHubProjectV2Fetcher{}).Fetch(
		ctx, claim, providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		githubProjectV2TestClient(t, doer), normalizedAt, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Premise. If the producer stopped emitting, the assertions below would
	// pass vacuously against an empty table rather than failing where the
	// behaviour actually broke.
	if len(fetched.Rows.ProjectMemberships) != 1 || len(fetched.Rows.Projects) != 1 {
		t.Fatalf("producer emitted memberships=%d projects=%d, want 1 and 1",
			len(fetched.Rows.ProjectMemberships), len(fetched.Rows.Projects))
	}

	// The derived destinations are required to be PRESENT (possibly empty) by
	// the builder's own completeness gate, so they are supplied empty here:
	// this test is about the two direct families, not about derivations.
	derived := map[string][]json.RawMessage{}
	for _, destination := range githubWorkItemDerivedDestinations {
		derived[destination] = []json.RawMessage{}
	}
	effects, err := buildGitHubWorkItemsRouteEffects(fetched.Rows, derived)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewGitHubWorkItemClickHouseEffects(
		conn, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	// The builder must produce an effect for EVERY destination, including the
	// two this ticket adds -- that is the completeness invariant, and it is the
	// half that was missing when the rows were built and then dropped.
	built := map[string]EffectBatch{}
	for _, effect := range effects {
		built[effect.Destination] = effect
	}
	if len(built) != len(githubWorkItemRouteDestinations()) {
		t.Fatalf("effects=%d want=%d", len(built), len(githubWorkItemRouteDestinations()))
	}
	// Only the two destinations under review are written here. The other
	// sixteen adapters have preconditions of their own -- several need a
	// Postgres-backed claim and a live lease guard -- and dragging those in
	// would make this test fail for reasons that have nothing to do with what
	// it is proving. Their write paths are covered by their own integration
	// tests; what was never covered, and what this test exists for, is that the
	// membership and catalogue rows travel from a provider payload all the way
	// into the migrated tables.
	for _, destination := range []string{"project_membership_transitions", "projects"} {
		effect, produced := built[destination]
		if !produced {
			t.Fatalf("the effects builder never produced a %s effect", destination)
		}
		if len(effect.Rows) != 1 {
			t.Fatalf("%s effect carries %d rows, want 1", destination, len(effect.Rows))
		}
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatalf("write %s: %v", destination, err)
		}
		// The readback is half of what makes a destination real: an adapter
		// that writes but cannot recognise its own row passes a count check and
		// fails every recovery.
		inspection, err := sink.InspectEffect(ctx, claim, effect)
		if err != nil {
			t.Fatalf("inspect %s: %v", destination, err)
		}
		if inspection != EffectExact {
			t.Fatalf("readback of %s = %s, want EffectExact", destination, inspection)
		}
	}

	var (
		subjectKind, subjectID, provider, toProjectID string
		repoID                                        uuid.UUID
		occurredAt                                    time.Time
	)
	if err := conn.QueryRow(ctx, `
SELECT subject_kind, subject_id, repo_id, provider, to_project_id, occurred_at
FROM project_membership_transitions FINAL WHERE org_id = ?`, claim.OrgID,
	).Scan(&subjectKind, &subjectID, &repoID, &provider, &toProjectID, &occurredAt); err != nil {
		t.Fatalf("no membership row reached ClickHouse: %v", err)
	}
	if subjectKind != "pull_request" || subjectID != "42" || provider != "github" {
		t.Errorf("stored subject = %s / %s / %s", subjectKind, subjectID, provider)
	}
	if toProjectID != "ghprojv2:acme#3" {
		t.Errorf("to_project_id = %q", toProjectID)
	}
	// The board item's own createdAt, not the sync clock. This is the property
	// that makes the content-determined event_id stable across re-syncs, and it
	// is only observable once the row is actually stored.
	if !occurredAt.Equal(time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)) {
		t.Errorf("occurred_at = %s, want the board item's createdAt", occurredAt)
	}
	// repo_id must equal what repositoryIdentity derives for the repository, or
	// the row names a repo nothing else in the graph uses.
	identity, err := repositoryIdentity("acme/api")
	if err != nil {
		t.Fatal(err)
	}
	if repoID.String() != identity {
		t.Errorf("repo_id = %s, want %s", repoID, identity)
	}

	// The `projects` row is what makes that destination resolvable at all --
	// without it every github membership is filtered out by the vocabulary
	// constraint, which is the gap this ticket found.
	var catalogID, catalogProvider, catalogName string
	if err := conn.QueryRow(ctx,
		`SELECT id, provider, name FROM projects FINAL WHERE org_id = ? AND provider = 'github'`,
		claim.OrgID).Scan(&catalogID, &catalogProvider, &catalogName); err != nil {
		t.Fatalf("no projects row reached ClickHouse: %v", err)
	}
	if catalogID != "ghprojv2:acme#3" || catalogProvider != "github" || catalogName == "" {
		t.Errorf("catalogue row = %s / %s / %q", catalogID, catalogProvider, catalogName)
	}

	// End to end: the presence view answers for the PR. This is the ticket's
	// acceptance sentence -- "a PR that is a project item in the provider has a
	// queryable PR->project mapping in the graph after sync" -- asked of the
	// graph rather than of any struct along the way.
	var presenceProject, presenceSource string
	if err := conn.QueryRow(ctx, `
SELECT project_id, source FROM project_membership_presence
WHERE org_id = ? AND subject_kind = 'pull_request' AND subject_id = '42'`, claim.OrgID,
	).Scan(&presenceProject, &presenceSource); err != nil {
		t.Fatalf("the PR has no presence edge: %v", err)
	}
	if presenceProject != "ghprojv2:acme#3" || presenceSource != "transition" {
		t.Errorf("presence = %q via %q", presenceProject, presenceSource)
	}
}
