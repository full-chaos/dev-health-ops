package daily

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
)

// TestResolveRepoProviderFollowsDiscoverReposFallbackChain pins the rule that
// decides the `provider` column of all three edge tables.
//
// This is worth a dedicated test because `provider` is in NONE of the three
// sorting keys. A wrong value splits no rows, changes no counts and trips no
// dedup assertion -- it just mislabels every edge, silently. Structural checks
// cannot see it; only this test and the live oracle can.
//
// The chain (discover_repos, job_daily.py:194, then :1200):
//
//	source = r[3] if r[3] != "unknown" else <job provider>
//	provider = source or "unknown"
func TestResolveRepoProviderFollowsDiscoverReposFallbackChain(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		repoProvider string
		jobProvider  string
		want         string
	}{
		{"a real provider wins", "github", "auto", "github"},
		{"gitlab likewise", "gitlab", "auto", "gitlab"},
		{
			// The counter-intuitive one. A repo whose provider column
			// literally reads "unknown" does NOT get "unknown" -- Python
			// treats that string as absent and falls back to the job's
			// provider, so "auto" lands in the column.
			name:         "the literal \"unknown\" falls back to the job provider",
			repoProvider: "unknown", jobProvider: "auto", want: "auto",
		},
		{"empty falls back to the job provider", "", "auto", "auto"},
		{"job provider is honoured when set", "unknown", "github", "github"},
		{"both empty ends at \"unknown\"", "", "", "unknown"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			if got := resolveRepoProvider(testCase.repoProvider, testCase.jobProvider); got != testCase.want {
				t.Errorf("resolveRepoProvider(%q, %q) = %q, want %q",
					testCase.repoProvider, testCase.jobProvider, got, testCase.want)
			}
		})
	}
}

// TestUnmappedRepositoryFallsBackToUnknownNotTheJobProvider pins the OTHER
// fallback, which is a different one.
//
// _by_provider does `repo_provider_by_id.get(str(repo_id), "unknown")`: a repo
// that discover_repos never returned is labelled "unknown", NOT the job
// provider. The job-provider fallback happens inside the map's construction
// and a repo absent from the map was never subject to it. Collapsing these two
// fallbacks into one is the natural simplification and it is wrong.
func TestUnmappedRepositoryFallsBackToUnknownNotTheJobProvider(t *testing.T) {
	known := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	unmapped := uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	providers := map[string]string{known.String(): "github"}

	if got := workGraphEdgesProviderFor(providers, known); got != "github" {
		t.Errorf("mapped repo: got %q, want github", got)
	}
	if got := workGraphEdgesProviderFor(providers, unmapped); got != "unknown" {
		t.Errorf("unmapped repo: got %q, want unknown (NOT the job provider)", got)
	}
}

// TestExtractPerProviderSplitsAndSortsLikePython pins two things Python does
// that a single flattened call would not.
//
//  1. The extractor runs once per provider, so the per-repo deployment index a
//     heuristic incident walks is rebuilt per provider -- an incident can
//     never link to a deployment from a different provider.
//  2. Providers are visited in SORTED order (`for wf_provider in
//     sorted(edge_providers)`), which is what makes the emitted row order
//     deterministic.
func TestExtractPerProviderSplitsAndSortsLikePython(t *testing.T) {
	const org = "70d529e0-3c06-4597-8480-794fd02328b6"
	repoGitHub := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	repoGitLab := uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")

	providers := map[string]string{
		repoGitHub.String(): "github",
		repoGitLab.String(): "gitlab",
	}

	deployments := []workgraphedges.DeploymentRow{
		{RepoID: repoGitLab, DeploymentID: "gitlab-dep"},
		{RepoID: repoGitHub, DeploymentID: "github-dep"},
	}
	incidents := []workgraphedges.IncidentRow{
		{RepoID: repoGitHub, IncidentID: "inc-github"},
		{RepoID: repoGitLab, IncidentID: "inc-gitlab"},
	}

	result, err := extractPerProvider(
		providers, nil, deployments, incidents, org, time.Time{},
	)
	if err != nil {
		t.Fatalf("extractPerProvider: %v", err)
	}

	if len(result.DeploymentIncidentEdges) != 2 {
		t.Fatalf("expected one edge per provider, got %d", len(result.DeploymentIncidentEdges))
	}
	// "github" sorts before "gitlab", so github's edge is emitted first.
	first, second := result.DeploymentIncidentEdges[0], result.DeploymentIncidentEdges[1]
	if first.Provider != "github" || second.Provider != "gitlab" {
		t.Errorf("providers must be visited in sorted order: got %q then %q",
			first.Provider, second.Provider)
	}
	// Each incident linked ONLY to its own provider's deployment.
	if first.DeploymentID != "github-dep" || first.IncidentID != "inc-github" {
		t.Errorf("github edge crossed providers: %s -> %s", first.IncidentID, first.DeploymentID)
	}
	if second.DeploymentID != "gitlab-dep" || second.IncidentID != "inc-gitlab" {
		t.Errorf("gitlab edge crossed providers: %s -> %s", second.IncidentID, second.DeploymentID)
	}
}

// TestIncidentAdapterLeavesDeploymentIDEmpty pins CHAOS-5110 as a DELIBERATE
// reproduction rather than an oversight a future reader might "fix".
//
// active_incidents_query selects no deployment_id, so every incident reaching
// the extractor from the daily job takes the heuristic branch. If this ever
// starts returning a non-empty DeploymentID, the rows change from
// source="heuristic" to source="native" -- and since `source` is IN the
// sorting key, the new rows will sit BESIDE the old ones rather than replacing
// them.
func TestIncidentAdapterLeavesDeploymentIDEmpty(t *testing.T) {
	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	adapted := workGraphEdgeIncidents([]IncidentRow{
		{RepoID: repoID, IncidentID: "inc-1"},
	})
	if len(adapted) != 1 {
		t.Fatalf("expected 1 adapted incident, got %d", len(adapted))
	}
	if adapted[0].DeploymentID != "" {
		t.Errorf("the daily loader cannot supply a deployment_id (CHAOS-5110); "+
			"got %q -- if this is now intended, the source column moves from "+
			"heuristic to native and that is a key change, not a value change",
			adapted[0].DeploymentID)
	}
	if adapted[0].StartedAt == nil {
		t.Error("started_at must be carried through; the query's own WHERE guarantees it is non-NULL")
	}
	if adapted[0].LastSynced != nil {
		t.Error("last_synced is unreachable in the _dt chain here and must stay nil")
	}
}
