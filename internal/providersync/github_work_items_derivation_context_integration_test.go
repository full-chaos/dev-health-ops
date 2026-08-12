//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestGitHubWorkItemDerivationContextScansClickHouseNativeIntegerWidths(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	const (
		orgID    = "org-derivation-scan-widths"
		provider = "linear"
		teamID   = "team-scan-widths"
	)
	validFrom := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	asOf := validFrom.Add(time.Hour)
	updatedAt := validFrom.Add(time.Minute)
	repoID := uuid.MustParse("3a9b0121-f9f2-4b5f-88a1-39e4a1911ef2")

	for _, insert := range []struct {
		query string
		args  []any
	}{
		{
			query: `INSERT INTO team_project_ownership
				(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			args: []any{orgID, provider, teamID, "project-1", "CHAOS", "native", uint8(1), uint16(65530), int32(-27), validFrom, nil, updatedAt},
		},
		{
			query: `INSERT INTO team_repo_ownership
				(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			args: []any{orgID, provider, teamID, repoID, "full-chaos/dev-health", "exact", "native", uint8(0), uint16(1200), int32(18), validFrom, nil, updatedAt},
		},
		{
			query: `INSERT INTO team_memberships
				(org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			args: []any{orgID, provider, teamID, "member-1", "user-1", "user@example.com", []string{"email:user@example.com"}, "native", uint8(1), uint16(42), int32(-5), validFrom, nil, updatedAt},
		},
		{
			query: `INSERT INTO manual_attribution_fallbacks
				(org_id, provider, scope_type, scope_id, team_id, team_name, reason, priority, valid_from, valid_to, created_by, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			args: []any{orgID, provider, "project", "project-1", teamID, "Scan Widths", "regression", int32(-9), validFrom, nil, nil, updatedAt, updatedAt},
		},
	} {
		if err := conn.Exec(ctx, insert.query, insert.args...); err != nil {
			t.Fatalf("seed derivation context: %v", err)
		}
	}

	source := githubWorkItemClickHouseDerivationContextSource{Conn: conn}

	projects, err := source.loadProjects(context.Background(), orgID, asOf)
	if err != nil {
		t.Fatalf("load projects: %v", err)
	}
	if len(projects) != 1 || projects[0].IsPrimary != 1 || projects[0].Specificity != 65530 || projects[0].Priority != -27 {
		t.Fatalf("project numeric fields = %+v", projects)
	}

	repos, err := source.loadRepos(context.Background(), orgID, asOf)
	if err != nil {
		t.Fatalf("load repos: %v", err)
	}
	if len(repos) != 1 || repos[0].IsPrimary != 0 || repos[0].Specificity != 1200 || repos[0].Priority != 18 {
		t.Fatalf("repo numeric fields = %+v", repos)
	}

	members, err := source.loadMembers(context.Background(), orgID, asOf)
	if err != nil {
		t.Fatalf("load members: %v", err)
	}
	if len(members) != 1 || members[0].IsPrimary != 1 || members[0].Specificity != 42 || members[0].Priority != -5 {
		t.Fatalf("member numeric fields = %+v", members)
	}

	manual, err := source.loadManualFallbacks(context.Background(), orgID, asOf)
	if err != nil {
		t.Fatalf("load manual fallbacks: %v", err)
	}
	if len(manual) != 1 || manual[0].Priority != -9 {
		t.Fatalf("manual numeric fields = %+v", manual)
	}
}
