//go:build integration

package usersbackfill

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/repoidentity"
)

func syncAudits(t *testing.T, e *reconEnv, ctx context.Context, org string) int {
	t.Helper()
	return e.count(t, ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = 'auth.repository_identity.synced' AND organization_id = $1`, org)
}

// A second Apply with the same input writes nothing: the commit guard counts a BEFORE INSERT even when ON CONFLICT then
// does nothing, so the insert must propose only the rows that are missing.
func TestRepoIdentityApplyTwiceWithTheSameInputWritesNoAuditRowAndReportsNoChange(t *testing.T) {
	ctx := context.Background()
	e := getReconEnv(t)
	org, _ := e.seedOrg(t, ctx, true)
	if err := e.store.ProjectOrganization(ctx, org); err != nil {
		t.Fatalf("ProjectOrganization: %v", err)
	}
	sink := repoidentity.PGSink{Pool: e.runtime, Schema: e.schema}
	repos := []repoidentity.Repo{{OrgID: org.String(), Provider: "github", ProviderRepoID: "1", Path: "acme/a"}, {OrgID: org.String(), Provider: "github", ProviderRepoID: "2", Path: "acme/b"}}
	first, err := sink.Apply(ctx, org, repos)
	if err != nil || !first.Changed {
		t.Fatalf("first apply: %+v %v", first, err)
	}
	before := syncAudits(t, e, ctx, org.String())
	second, err := sink.Apply(ctx, org, repos)
	if err != nil {
		t.Fatal(err)
	}
	if second.Changed {
		t.Error("a second apply with the same input reported Changed")
	}
	if after := syncAudits(t, e, ctx, org.String()); after != before {
		t.Errorf("a second apply wrote %d audit row(s)", after-before)
	}
	moved, err := sink.Apply(ctx, org, []repoidentity.Repo{{OrgID: org.String(), Provider: "github", ProviderRepoID: "1", Path: "acme/renamed"}, repos[1]})
	if err != nil || !moved.Changed {
		t.Fatalf("a rename must change: %+v %v", moved, err)
	}
	var path string
	if err := e.admin.QueryRow(ctx, `SELECT current_path FROM auth.repository_identities WHERE organization_id = $1 AND provider_repo_id = '1'`, org).Scan(&path); err != nil || path != "acme/renamed" {
		t.Errorf("path %q %v", path, err)
	}
}
