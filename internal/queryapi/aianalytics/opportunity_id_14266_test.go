package aianalytics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// CHAOS-14266: a repository has one repetitive-change cluster per author, work type and title prefix; each
// cluster is one recommendation and must have its own opportunityId, the same on every read.

const (
	repo14266 = "11111111-1111-1111-1111-111111111111"
	// The author keys of the privacy cell. Neither may reach the answer or a log line.
	secretEmail14266 = "alice.secret@example.com"
	secretName14266  = "Bob Hidden-Name"
)

func cluster14266(prefix, author, workType string, n int, numbers ...string) map[string]any {
	refs := []any{}
	for _, number := range numbers {
		refs = append(refs, "git_pull_requests:"+repo14266+":"+number)
	}
	return map[string]any{"repo_id": repo14266, "title_prefix": prefix, "prs_total": float64(n), "pr_refs": refs,
		"cluster_author": author, "cluster_work_type": workType}
}

// repetitiveIDs14266 answers aiOpportunities over the clusters and returns each REPETITIVE_CHANGE id by its first
// evidence ref (one pull request per cluster in these cells), failing on an id served twice in the answer.
func repetitiveIDs14266(t *testing.T, rep ...map[string]any) map[string]string {
	t.Helper()
	got, err := AiOpportunities(context.Background(), &fixtureClientD{c: oracleDCase{Rep: rep}}, "org-1", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	byRef := map[string]string{}
	served := map[string]int{}
	for _, r := range got.Recommendations {
		served[r.OpportunityID]++
		if r.Kind == model.AIOpportunityKindRepetitiveChange {
			byRef[r.EvidenceRefs[0]] = r.OpportunityID
		}
	}
	for id, n := range served {
		if n > 1 {
			t.Errorf("opportunityId %s is served %d times in one answer", id, n)
		}
	}
	return byRef
}

func ref14266(number string) string { return "git_pull_requests:" + repo14266 + ":" + number }

func TestCell14266TwoClustersOneRepoDistinctIDs(t *testing.T) {
	ids := repetitiveIDs14266(t,
		cluster14266("bump deps for", "a@example.com", "pull_request", 6, "1", "2"),
		cluster14266("fix flaky test", "a@example.com", "pull_request", 7, "3", "4"))
	if len(ids) != 2 {
		t.Fatalf("want 2 repetitive recommendations, got %d", len(ids))
	}
	if ids[ref14266("1")] == ids[ref14266("3")] {
		t.Errorf("two clusters of one repository share opportunityId %s", ids[ref14266("1")])
	}
	if want := stableOpportunityID("repetitive_change", repo14266, `["bump deps for","a@example.com","pull_request"]`); ids[ref14266("1")] != want {
		t.Errorf("id = %s, want %s: the hash of the cluster's prefix, author and work type", ids[ref14266("1")], want)
	}
}

func TestCell14266SamePrefixTwoAuthorsOrWorkTypesDistinctIDs(t *testing.T) {
	for name, rows := range map[string][]map[string]any{
		"two authors": {
			cluster14266("bump deps for", "a@example.com", "pull_request", 6, "1", "2"),
			cluster14266("bump deps for", "b@example.com", "pull_request", 6, "3", "4"),
		},
		"two work types": {
			cluster14266("bump deps for", "a@example.com", "pull_request", 6, "1", "2"),
			cluster14266("bump deps for", "a@example.com", "bug", 6, "3", "4"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			ids := repetitiveIDs14266(t, rows...)
			if len(ids) != 2 || ids[ref14266("1")] == ids[ref14266("3")] {
				t.Errorf("two clusters with one prefix must have two ids: %v", ids)
			}
		})
	}
}

// The id is a function of the cluster's key alone: not of the other clusters, the row order, the members or
// the count.
func TestCell14266IDStableAcrossReads(t *testing.T) {
	a := cluster14266("bump deps for", "a@example.com", "pull_request", 6, "1", "2")
	b := cluster14266("fix flaky test", "b@example.com", "bug", 7, "3", "4")
	alone := repetitiveIDs14266(t, a)
	both := repetitiveIDs14266(t, a, b)
	reversed := repetitiveIDs14266(t, b, a)
	grown := repetitiveIDs14266(t, cluster14266("bump deps for", "a@example.com", "pull_request", 9, "1", "5", "2"), b)
	for name, ids := range map[string]map[string]string{"with a second cluster": both, "reversed": reversed, "grown": grown} {
		if ids[ref14266("1")] != alone[ref14266("1")] {
			t.Errorf("%s: id %s, want %s (the same cluster)", name, ids[ref14266("1")], alone[ref14266("1")])
		}
	}
	if both[ref14266("3")] != reversed[ref14266("3")] {
		t.Errorf("the second cluster's id moved with the row order: %s != %s", both[ref14266("3")], reversed[ref14266("3")])
	}
}

// otherKindIDsAtMain14266 are the ids main served (f3ab486b179d) over the recorded ai/* cases for the nine kinds
// that are not REPETITIVE_CHANGE, by kind|repository|team.
func otherKindIDsAtMain14266() map[string]string {
	return map[string]string{
		"DEPENDENCY_UPDATES|11111111-1111-1111-1111-111111111111|":        "2cb0eb36c894b1cca9525f38",
		"DOCUMENTATION_DRIFT|11111111-1111-1111-1111-111111111111|":       "c3c175417265d3d00db68004",
		"FLAKY_TEST_TRIAGE|22222222-2222-2222-2222-222222222222|":         "9b3098b7f4ac85e69dd51561",
		"FLAKY_TEST_TRIAGE|33333333-3333-3333-3333-333333333333|":         "8a69cf92ea04b9ba7540c5a9",
		"HIGH_REVIEW_LOAD|11111111-1111-1111-1111-111111111111|":          "9f2cae12360eb28716c30c3b",
		"HIGH_REVIEW_LOAD|33333333-3333-3333-3333-333333333333|":          "936c5d8d52892821ff984542",
		"HIGH_REWORK|11111111-1111-1111-1111-111111111111|":               "4cf6c79f0f1610e01423cf96",
		"HIGH_REWORK|22222222-2222-2222-2222-222222222222|":               "40abb6fe6d35e2e8f896d2a4",
		"MECHANICAL_MIGRATIONS|33333333-3333-3333-3333-333333333333|":     "ea49200f8e787de21779a292",
		"SLOW_CYCLE|11111111-1111-1111-1111-111111111111|":                "0fea5f8862e0d04c1a938052",
		"SLOW_CYCLE|33333333-3333-3333-3333-333333333333|":                "d82091c46ffe0bb6dbe45b9c",
		"TEST_GENERATION|11111111-1111-1111-1111-111111111111|":           "369afb14ff3cb3e4864fc196",
		"TEST_GENERATION|22222222-2222-2222-2222-222222222222|":           "ad7c4b9c6b3ebf33bcaa5797",
		"TEST_GENERATION|22222222-2222-2222-2222-222222222222|team-b":     "6117540400be95696944a4b1",
		"TEST_GENERATION|33333333-3333-3333-3333-333333333333|":           "a0999ad677d929d57f283e8e",
		"UNCOVERED_TEST_AREA|11111111-1111-1111-1111-111111111111|":       "c538097ef3225ad076fe6fb2",
		"UNCOVERED_TEST_AREA|22222222-2222-2222-2222-222222222222|team-b": "c7cb8c852a59f57f5f4e7f9d",
		"UNCOVERED_TEST_AREA|33333333-3333-3333-3333-333333333333|":       "039bf33476694a26be355258",
	}
}

func TestCell14266OtherKindsIDsByteEqual(t *testing.T) {
	pinned := otherKindIDsAtMain14266()
	seenKinds := map[model.AIOpportunityKind]bool{}
	seenPins := map[string]bool{}
	for name, c := range loadDCases(t) {
		if c.Kind != "ai" {
			continue
		}
		var in *model.AIScopeInput
		if c.Scope != nil {
			in = &model.AIScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID}
		}
		got, err := AiOpportunities(context.Background(), &fixtureClientD{c: c}, "org-1", in, 100)
		if err != nil {
			continue // a recorded failure case
		}
		for _, r := range got.Recommendations {
			if r.Kind == model.AIOpportunityKindRepetitiveChange {
				continue
			}
			team := ""
			if r.TeamID != nil {
				team = *r.TeamID
			}
			key := string(r.Kind) + "|" + *r.RepoID + "|" + team
			want, ok := pinned[key]
			if !ok {
				t.Errorf("%s: %s has no id pinned at main", name, key)
				continue
			}
			if r.OpportunityID != want {
				t.Errorf("%s: %s id = %s, want %s (main)", name, key, r.OpportunityID, want)
			}
			seenKinds[r.Kind] = true
			seenPins[key] = true
		}
	}
	if len(seenKinds) != len(opportunityKinds)-1 {
		t.Errorf("the recorded cases serve %d of the %d other kinds", len(seenKinds), len(opportunityKinds)-1)
	}
	if len(seenPins) != len(pinned) {
		t.Errorf("%d of %d pinned ids were served", len(seenPins), len(pinned))
	}
}

// The author key is hash input only: the served answer holds no part of it and no new field, and no log line of
// the read holds it (the catalogue read fails here, so its warning is written).
func TestCell14266ServedAnswerCarriesNoClusterAuthor(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	client := &fixtureClientD{c: oracleDCase{Rep: []map[string]any{
		cluster14266("bump deps for", secretEmail14266, "pull_request", 6, "1", "2"),
		cluster14266("bump deps for", secretName14266, "pull_request", 6, "3", "4"),
	}}, catalogueReposErr: errors.New("catalogue down")}
	got, err := AiOpportunities(context.Background(), client, "org-1", nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Recommendations) != 2 || got.Recommendations[0].OpportunityID == got.Recommendations[1].OpportunityID {
		t.Fatalf("want two repetitive recommendations with two ids: %+v", got.Recommendations)
	}
	body, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if logs.Len() == 0 {
		t.Fatal("no log line was written: the log half of this cell is not exercised")
	}
	for _, part := range []string{secretEmail14266, "alice.secret", "example.com", secretName14266, "Hidden-Name", "Bob Hidden"} {
		if bytes.Contains(body, []byte(part)) {
			t.Errorf("the served answer holds %q: %s", part, body)
		}
		if strings.Contains(logs.String(), part) {
			t.Errorf("a log line holds %q: %s", part, logs.String())
		}
	}
	allowed := map[string]bool{}
	for _, f := range reflect.VisibleFields(reflect.TypeOf(model.AIOpportunity{})) {
		allowed[strings.Split(f.Tag.Get("json"), ",")[0]] = true
	}
	var served struct {
		Recommendations []map[string]any `json:"recommendations"`
	}
	if err := json.Unmarshal(body, &served); err != nil {
		t.Fatal(err)
	}
	for _, r := range served.Recommendations {
		for field := range r {
			if !allowed[field] {
				t.Errorf("the served recommendation has the field %q, which is not a schema field", field)
			}
		}
	}
	if len(allowed) != 11 {
		t.Errorf("AIOpportunity has %d fields, want the 11 of the schema: a new field needs this cell's review", len(allowed))
	}
}

// The oracle acceptance gate of the REPETITIVE_CHANGE declaration: with the declaration applied, every known defect
// planted into the Go answer of a recorded case is still a difference from the recorded Python answer.
func TestRepetitiveIDDeclarationStillFindsDefects(t *testing.T) {
	firstOf := func(m map[string]any, repetitive bool) map[string]any {
		for _, item := range m["recommendations"].([]any) {
			r := item.(map[string]any)
			if (r["kind"] == "REPETITIVE_CHANGE") == repetitive {
				return r
			}
		}
		t.Fatal("the case has no such recommendation")
		return nil
	}
	defects := []struct {
		name  string
		plant func(got map[string]any)
	}{
		{"repetitive id back to the per-repository form", func(got map[string]any) {
			r := firstOf(got, true)
			r["opportunityId"] = stableOpportunityID("repetitive_change", r["repoId"].(string), "")
		}},
		{"repetitive id of another cluster key", func(got map[string]any) {
			r := firstOf(got, true)
			r["opportunityId"] = stableOpportunityID("repetitive_change", r["repoId"].(string), repetitiveClusterKey("other", "", ""))
		}},
		{"another kind's id changed", func(got map[string]any) { firstOf(got, false)["opportunityId"] = "000000000000000000000000" }},
		{"another kind's id given a cluster key", func(got map[string]any) {
			r := firstOf(got, false)
			r["opportunityId"] = stableOpportunityID("high_rework", r["repoId"].(string), repetitiveClusterKey("", "", ""))
		}},
		{"repetitive rationale changed", func(got map[string]any) {
			r := firstOf(got, true)
			r["rationale"] = strings.Replace(r["rationale"].(string), "30 days", "31 days", 1)
		}},
		{"repetitive score changed", func(got map[string]any) { firstOf(got, true)["score"] = 0.123 }},
		{"repetitive evidence ref dropped", func(got map[string]any) {
			r := firstOf(got, true)
			r["evidenceRefs"] = r["evidenceRefs"].([]any)[1:]
		}},
		{"repetitive kind changed", func(got map[string]any) { firstOf(got, true)["kind"] = "DEPENDENCY_UPDATES" }},
		{"recommendation dropped", func(got map[string]any) {
			got["recommendations"] = got["recommendations"].([]any)[1:]
		}},
	}
	clean := func(plant func(map[string]any)) bool {
		c := loadDCases(t)["ai/everything"]
		answer, err := AiOpportunities(context.Background(), &fixtureClientD{c: c}, "org-1", nil, c.Limit)
		if err != nil {
			t.Fatal(err)
		}
		enc, _ := json.Marshal(answer)
		var got map[string]any
		_ = json.Unmarshal(enc, &got)
		if plant != nil {
			plant(got)
		}
		declareTitleNaming(t, got, c.Expected, c.Kind)
		declareFlakyRationale(t, got, c.Expected, c.Kind)
		declareDrilldownRoots(t, got, c.Expected)
		problems := declareRepetitiveIDs(got, c.Expected, c)
		return len(problems) == 0 && reflect.DeepEqual(normalizeD(got), normalizeD(c.Expected))
	}
	if !clean(nil) {
		t.Fatal("the unplanted answer differs from the recorded one: the gate has no baseline")
	}
	for _, d := range defects {
		if clean(d.plant) {
			t.Errorf("known defect %q is not found: the declaration hides it", d.name)
		}
	}
}
