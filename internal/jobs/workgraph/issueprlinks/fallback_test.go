package issueprlinks

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
)

var (
	fallbackRepoA = uuid.MustParse("00000000-0000-4000-8000-00000000000a")
	fallbackRepoB = uuid.MustParse("00000000-0000-4000-8000-00000000000b")
)

func link(repo uuid.UUID, pr uint32, workItemID, provenance string) Link {
	return Link{RepoID: repo, PRNumber: pr, WorkItemID: workItemID, Provenance: provenance}
}

// One work item id per tracker, so the plan is proven for every provider.
var trackerIDs = map[string][2]string{
	"jira":   {"jira:OPS-7", "jira:API-1"},
	"linear": {"linear:CHAOS-1", "linear:ENG-1"},
	"github": {"gh:full-chaos/acr#5", "gh:full-chaos/acr#12"},
	"gitlab": {"gitlab:full-chaos/acr#5", "gitlab:full-chaos/acr#12"},
}

func TestPlanOutrankedFallbacksPullRequestSide(t *testing.T) {
	for nativeProvider, nativeIDs := range trackerIDs {
		for textProvider, textIDs := range trackerIDs {
			t.Run(nativeProvider+"_native/"+textProvider+"_text", func(t *testing.T) {
				stored := []Link{
					link(fallbackRepoA, 704, nativeIDs[0], ProvenanceNative),
					link(fallbackRepoA, 704, textIDs[1], edges.ProvenanceExplicitText),
					link(fallbackRepoA, 704, textIDs[1]+"x", edges.ProvenanceHeuristic),
					// Controls: no native link on either side.
					link(fallbackRepoA, 705, textIDs[1], edges.ProvenanceExplicitText),
					// Same number, other repository: another pull request.
					link(fallbackRepoB, 704, textIDs[1]+"y", edges.ProvenanceExplicitText),
				}
				got := PlanOutrankedFallbacks(stored)
				want := []Link{stored[1], stored[2]}
				sortLinks(want)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("plan = %+v, want %+v", got, want)
				}
			})
		}
	}
}

func TestPlanOutrankedFallbacksIssueSide(t *testing.T) {
	for provider, ids := range trackerIDs {
		t.Run(provider, func(t *testing.T) {
			stored := []Link{
				link(fallbackRepoA, 704, ids[0], ProvenanceNative),
				link(fallbackRepoA, 706, ids[0], edges.ProvenanceExplicitText),
				link(fallbackRepoB, 3, ids[0], edges.ProvenanceHeuristic),
				// Control: another issue on another pull request.
				link(fallbackRepoA, 706, ids[1], edges.ProvenanceExplicitText),
			}
			got := PlanOutrankedFallbacks(stored)
			want := []Link{stored[1], stored[2]}
			sortLinks(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("plan = %+v, want %+v", got, want)
			}
		})
	}
}

func TestPlanOutrankedFallbacksNeverPlansANativeOrUnknownTier(t *testing.T) {
	stored := []Link{
		link(fallbackRepoA, 704, "linear:CHAOS-1", ProvenanceNative),
		link(fallbackRepoA, 704, "linear:CHAOS-2", ProvenanceNative),
		link(fallbackRepoA, 704, "jira:API-1", "unknown"),
	}
	if got := PlanOutrankedFallbacks(stored); len(got) != 0 {
		t.Fatalf("plan = %+v, want none", got)
	}
}

func sortLinks(links []Link) {
	for i := range links {
		for j := i + 1; j < len(links); j++ {
			if retractionKey(links[j]) < retractionKey(links[i]) {
				links[i], links[j] = links[j], links[i]
			}
		}
	}
}

type execRecorder struct {
	queries []string
	args    [][]any
}

func (recorder *execRecorder) Exec(_ context.Context, query string, args ...any) error {
	recorder.queries = append(recorder.queries, query)
	recorder.args = append(recorder.args, args)
	return nil
}

func TestRetractOutrankedTargetsExactlyThePlannedRows(t *testing.T) {
	recorder := &execRecorder{}
	rows := []Link{
		link(fallbackRepoA, 704, "jira:API-1", edges.ProvenanceExplicitText),
		link(fallbackRepoA, 706, "jira:OPS-7", edges.ProvenanceHeuristic),
	}
	if err := RetractOutranked(context.Background(), recorder, "org-9821", rows); err != nil {
		t.Fatal(err)
	}
	if len(recorder.queries) != 1 {
		t.Fatalf("want one mutation, got %d", len(recorder.queries))
	}
	query := recorder.queries[0]
	for _, part := range []string{"ALTER TABLE work_graph_issue_pr", "provenance IN ('explicit_text', 'heuristic')", "mutations_sync = 2"} {
		if !strings.Contains(query, part) {
			t.Fatalf("statement lacks %q:\n%s", part, query)
		}
	}
	want := []any{
		clickhouse.Named("org_id", "org-9821"),
		clickhouse.Named("keys", []string{
			fallbackRepoA.String() + "|jira:API-1|704|explicit_text",
			fallbackRepoA.String() + "|jira:OPS-7|706|heuristic",
		}),
	}
	if !reflect.DeepEqual(recorder.args[0], want) {
		t.Fatalf("args = %#v, want %#v", recorder.args[0], want)
	}
}

func TestRetractOutrankedWithNoRowsRunsNoMutation(t *testing.T) {
	recorder := &execRecorder{}
	if err := RetractOutranked(context.Background(), recorder, "org-9821", nil); err != nil {
		t.Fatal(err)
	}
	if len(recorder.queries) != 0 {
		t.Fatalf("want no mutation, got %v", recorder.queries)
	}
}

func TestRetractOutrankedRefusesANativeRow(t *testing.T) {
	recorder := &execRecorder{}
	err := RetractOutranked(context.Background(), recorder, "org-9821",
		[]Link{link(fallbackRepoA, 704, "linear:CHAOS-1", ProvenanceNative)})
	if err == nil || len(recorder.queries) != 0 {
		t.Fatalf("want a refusal and no mutation, got err=%v queries=%v", err, recorder.queries)
	}
}

func TestRetractOutrankedRequiresAnOrganization(t *testing.T) {
	recorder := &execRecorder{}
	err := RetractOutranked(context.Background(), recorder, "",
		[]Link{link(fallbackRepoA, 704, "jira:API-1", edges.ProvenanceExplicitText)})
	if err == nil || len(recorder.queries) != 0 {
		t.Fatalf("want a refusal and no mutation, got err=%v queries=%v", err, recorder.queries)
	}
}
