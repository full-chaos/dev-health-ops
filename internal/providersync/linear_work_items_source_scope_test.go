package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const linearTeamsTwoTeamsResponse = `{"data":{"teams":{"nodes":[{"id":"team-eng","key":"ENG","name":"Engineering"},{"id":"team-mob","key":"MOB","name":"Mobile"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`

func linearScopedTeamResponse(key string) string {
	return `{"data":{"teams":{"nodes":[{"id":"team-` + key + `","key":"` + key + `","name":"` + key + `"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}`
}

// linearWorkspaceDoer answers like Linear for a workspace with teams ENG and
// MOB: the team list, one team by key, and the issues of the team in the
// issue filter. It counts requests and the issue crawls per team.
type linearWorkspaceDoer struct {
	requests int
	crawls   map[string]int
}

func (doer *linearWorkspaceDoer) Do(request *http.Request) (*http.Response, error) {
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	doer.requests++
	var body struct {
		Variables struct {
			Filter struct {
				Team *struct {
					Key struct {
						In []string `json:"in"`
					} `json:"key"`
				} `json:"team"`
				Key *struct {
					Eq string `json:"eq"`
				} `json:"key"`
			} `json:"filter"`
		} `json:"variables"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	filter := body.Variables.Filter
	answer := ""
	switch {
	case filter.Team != nil:
		team := filter.Team.Key.In[0]
		doer.crawls[team]++
		answer = linearLifecycleIssueResponse(team+"-1", team)
	case filter.Key != nil:
		answer = linearScopedTeamResponse(filter.Key.Eq)
	default:
		answer = linearTeamsTwoTeamsResponse
	}
	return &http.Response{
		StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(answer)), Request: request,
	}, nil
}

// collectLinearSource runs the production handler (no forced global discovery)
// for one source of an org and returns the work-item ids it wrote and the
// doer, which counted what it asked Linear.
func collectLinearSource(t *testing.T, source string, metadata, config map[string]any) (map[string]int, *linearWorkspaceDoer) {
	t.Helper()
	doer := &linearWorkspaceDoer{crawls: map[string]int{}}
	claim := nativeTestClaim("linear", "work-items")
	claim.SourceExternalID = source
	claim.SourceMetadata = metadata
	claim.IntegrationConfig = config
	noCycles := false
	batch, err := (LinearWorkItemsRouteHandler{FetchCycles: &noCycles}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "linear", ID: claim.CredentialID},
		linearWorkItemsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatalf("source %s: %v", source, err)
	}
	keys := map[string]int{}
	for _, effect := range batch.Effects {
		if effect.Destination != "work_items" {
			continue
		}
		for _, row := range effect.Rows {
			var decoded struct {
				WorkItemID string `json:"work_item_id"`
			}
			if err := json.Unmarshal(row, &decoded); err != nil {
				t.Fatal(err)
			}
			keys[decoded.WorkItemID]++
		}
	}
	return keys, doer
}

func TestLinearSourcesOfOneOrgWriteEachRowOnce(t *testing.T) {
	managed := map[string]any{"planner_managed_sync_config_id": "cfg-1"}
	total := map[string]int{}
	for _, source := range []string{"ENG", "MOB"} {
		keys, doer := collectLinearSource(t, source, managed, map[string]any{})
		if doer.requests != 2 || doer.crawls[source] != 1 || len(doer.crawls) != 1 {
			t.Fatalf("source %s: requests=%d crawls=%v, want a team lookup and one crawl of its own team", source, doer.requests, doer.crawls)
		}
		for key, count := range keys {
			total[key] += count
		}
	}
	if len(total) != 2 {
		t.Fatalf("rows=%v, want ENG-1 and MOB-1", total)
	}
	for key, count := range total {
		if count != 1 {
			t.Fatalf("work item %s written %d times across two sources, want 1", key, count)
		}
	}
}

func TestLinearOrgWidePlaceholderStillCrawlsEveryTeam(t *testing.T) {
	cases := map[string]struct {
		metadata, config map[string]any
	}{
		"marked":                  {map[string]any{"org_wide_placeholder": true}, map[string]any{}},
		"legacy unmarked":         {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, map[string]any{"repo": "  ", "team_id": nil}},
		"legacy numeric id":       {map[string]any{"planner_managed_sync_config_id": float64(1)}, map[string]any{}},
		"legacy false":            {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, map[string]any{"team_id": false}},
		"legacy zero":             {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, map[string]any{"project_id": float64(0)}},
		"legacy empty containers": {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, map[string]any{"repo": []any{}, "project_key": map[string]any{}}},
		"legacy no config":        {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, nil},
	}
	for name, tc := range cases {
		keys, doer := collectLinearSource(t, "linear", tc.metadata, tc.config)
		if doer.requests != 3 || len(keys) != 2 || doer.crawls["ENG"] != 1 || doer.crawls["MOB"] != 1 {
			t.Fatalf("%s: requests=%d crawls=%v keys=%v, want the team list and both teams crawled once", name, doer.requests, doer.crawls, keys)
		}
	}
}

func TestLinearOrgWidePlaceholderIsNotAnyProviderNamedSource(t *testing.T) {
	unit := nativeTestClaim("linear", "work-items").Unit
	unit.SourceExternalID = "linear"
	cases := map[string]struct {
		metadata, config map[string]any
	}{
		"not planner managed": {map[string]any{}, map[string]any{}},
		"explicit team":       {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, map[string]any{"team_id": "ENG"}},
		"explicit repo":       {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, map[string]any{"repo": "MOB"}},
		"explicit number":     {map[string]any{"planner_managed_sync_config_id": "cfg-1"}, map[string]any{"team_id": float64(7)}},
		"marker not true":     {map[string]any{"org_wide_placeholder": "true"}, map[string]any{}},
	}
	for name, tc := range cases {
		unit.SourceMetadata, unit.IntegrationConfig = tc.metadata, tc.config
		if linearOrgWidePlaceholder(unit) {
			t.Fatalf("%s must be a scoped source", name)
		}
	}
}
