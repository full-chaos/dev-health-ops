//go:build integration

// The sentences and prior values the REST summaries build from a delta
// (CHAOS-9063), through the production handlers against a real ClickHouse: a
// delta states a move between two measured values (deltarule), so a metric with
// a value in one window only has no "held steady", no invented prior value and
// no "+0%". GraphQL Home is built by the same home.BuildResponse.
package server

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/people"
)

type restHomeSentences struct {
	Summary []struct {
		Text string `json:"text"`
	} `json:"summary"`
	Constraint struct {
		Title string `json:"title"`
		Claim string `json:"claim"`
	} `json:"constraint"`
	Events []struct {
		Text string `json:"text"`
	} `json:"events"`
	Signals []struct {
		Metric       string  `json:"metric"`
		Title        string  `json:"title"`
		CurrentValue string  `json:"current_value"`
		PriorValue   *string `json:"prior_value"`
		Delta        *string `json:"delta"`
		Why          string  `json:"why_it_matters"`
	} `json:"signals"`
}

func getJSON(t *testing.T, mux *http.ServeMux, org, target string, into any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req = req.WithContext(authctx.WithClaims(req.Context(), authctx.Claims{OrgID: org, Role: "owner"}))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET %s = HTTP %d: %s", target, rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), into); err != nil {
		t.Fatalf("decode %s: %v", target, err)
	}
}

func TestRESTHomeSentencesNeedTwoMeasuredWindows(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))
	const target = "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	seed := func(org string, days map[time.Time]uint32) {
		repo := uuid.New()
		for day, churn := range days {
			if err := conn.Exec(context.Background(),
				`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
				repo, day, churn, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), org); err != nil {
				t.Fatal(err)
			}
		}
	}
	claims := func(s restHomeSentences) string {
		var all []string
		for _, sentence := range s.Summary {
			all = append(all, sentence.Text)
		}
		all = append(all, s.Constraint.Title, s.Constraint.Claim)
		for _, event := range s.Events {
			all = append(all, event.Text)
		}
		return strings.Join(all, " | ")
	}

	// Churn has a value in the current window only.
	seed("sentences-current-only", map[time.Time]uint32{current: 5})
	var one restHomeSentences
	getJSON(t, mux, "sentences-current-only", target, &one)
	if text := claims(one); strings.Contains(text, "held steady") || strings.Contains(text, "Churn") {
		t.Errorf("a metric with a value in one window only is named in a claim: %s", text)
	}
	if len(one.Summary) != 0 || one.Constraint.Title != "" || one.Constraint.Claim != "" {
		t.Errorf("summary %v constraint %+v, want no sentence and an empty constraint card: no metric has two measured windows", one.Summary, one.Constraint)
	}
	found := false
	for _, signal := range one.Signals {
		if signal.Metric != "churn" {
			continue
		}
		found = true
		if signal.CurrentValue != "5 loc" || signal.PriorValue != nil || signal.Delta != nil {
			t.Errorf("churn signal = current %q prior %v delta %v, want current 5 loc, no prior value, no delta", signal.CurrentValue, signal.PriorValue, signal.Delta)
		}
		if strings.Contains(signal.Title, "flat") || strings.Contains(signal.Why, "flat") || strings.Contains(signal.Title, "steady") {
			t.Errorf("churn signal claims a trend: %q / %q", signal.Title, signal.Why)
		}
	}
	if !found {
		t.Error("the response has no churn signal for a metric that holds a current value")
	}

	// Control: churn in both windows keeps its sentence, constraint and prior value.
	seed("sentences-both", map[time.Time]uint32{current: 5, prior: 10})
	var two restHomeSentences
	getJSON(t, mux, "sentences-both", target, &two)
	if len(two.Summary) == 0 || !strings.Contains(two.Summary[0].Text, "Code Churn fell 50%") {
		t.Errorf("summary = %v, want a sentence naming the measured fall of Code Churn", two.Summary)
	}
	// The constraint names the metric with the highest delta among those with two
	// measured windows (here Rework Ratio, 0 %, rows of the same table), so it
	// is a claim about a measured move.
	if two.Constraint.Claim == "" || !strings.Contains(two.Constraint.Claim, "over the last 7 days") {
		t.Errorf("constraint = %+v, want one claim about a measured move", two.Constraint)
	}
	for _, signal := range two.Signals {
		if signal.Metric == "churn" && (signal.PriorValue == nil || *signal.PriorValue != "10 loc" || signal.Delta == nil || *signal.Delta != "-50%") {
			t.Errorf("two-window churn signal = prior %v delta %v, want 10 loc and -50%%", signal.PriorValue, signal.Delta)
		}
	}
}

func TestRESTPersonNarrativeNeedsTwoMeasuredWindows(t *testing.T) {
	t.Setenv("IDENTITY_MAPPING_PATH", filepath.Join(t.TempDir(), "missing.yaml"))
	conn, client := startTeamScopeClickHouse(t)
	reader, err := people.NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/people/{person_id}/summary", newPeopleSummaryHandler(reader))
	now := time.Now().UTC()
	day := func(daysAgo int) time.Time {
		return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -daysAgo)
	}
	const identity = "alice@example.com"
	personID := fmt.Sprintf("%x", md5.Sum([]byte(identity)))
	seed := func(org string, rows map[int]uint32) {
		repo := uuid.New()
		for daysAgo, loc := range rows {
			if err := conn.Exec(context.Background(),
				`INSERT INTO user_metrics_daily (repo_id, day, identity_id, author_email, loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
				repo, day(daysAgo), identity, identity, loc, now, org); err != nil {
				t.Fatal(err)
			}
		}
	}
	narrative := func(org string) []string {
		var body struct {
			Narrative []struct {
				Text string `json:"text"`
			} `json:"narrative"`
		}
		getJSON(t, mux, org, "/api/v1/people/"+personID+"/summary?range_days=14&compare_days=14", &body)
		out := []string{}
		for _, sentence := range body.Narrative {
			out = append(out, sentence.Text)
		}
		return out
	}
	// The person exists through a row long before both windows; churn has a
	// value in no window, then in the current window only: no metric has two.
	seed("narrative-none", map[int]uint32{90: 1})
	if got := narrative("narrative-none"); len(got) != 0 {
		t.Errorf("narrative with no value in either window = %v, want none (nothing was measured)", got)
	}
	seed("narrative-current-only", map[int]uint32{90: 1, 3: 5})
	if got := narrative("narrative-current-only"); len(got) != 0 {
		t.Errorf("narrative with a value in the current window only = %v, want none", got)
	}
	// Control: churn in both windows is named, and only churn.
	seed("narrative-both", map[int]uint32{90: 1, 3: 5, 20: 10})
	got := narrative("narrative-both")
	if len(got) != 1 || !strings.Contains(got[0], "Code churn decreased") {
		t.Errorf("narrative with two measured windows = %v, want one sentence about the decrease of code churn", got)
	}
}

// "driven by <ids>" names the drivers of the percent in the sentence: only
// repositories that hold a value in BOTH windows have a delta to drive it. A
// repository with a current value and no comparison row is not named (it read as
// a delta of 0 and ranked by it, so it could displace a measured driver), and
// when no driver has a delta the sentence names none (CHAOS-9063).
func TestRESTHomeDrivenByNamesOnlyDriversWithAMeasuredDelta(t *testing.T) {
	conn, client := startTeamScopeClickHouse(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/home", newHomeGetHandler(client, nil))
	const target = "/api/v1/home?range_days=7&compare_days=7&end_date=2026-08-25"
	current, prior := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC), time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	seed := func(org string, repo uuid.UUID, day time.Time, churn uint32) {
		t.Helper()
		if err := conn.Exec(context.Background(),
			`INSERT INTO repo_metrics_daily (repo_id, day, total_loc_touched, computed_at, org_id) VALUES (?, ?, ?, ?, ?)`,
			repo, day, churn, time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC), org); err != nil {
			t.Fatal(err)
		}
	}
	churnSentence := func(org string) string {
		var body restHomeSentences
		getJSON(t, mux, org, target, &body)
		for _, sentence := range body.Summary {
			if strings.HasPrefix(sentence.Text, "Code Churn") {
				return sentence.Text
			}
		}
		t.Fatalf("%s: no Code Churn sentence in %+v", org, body.Summary)
		return ""
	}

	// Four repositories with both windows (10 -> 5) and one with a current value only.
	const org = "driven-by-measured"
	var measured []uuid.UUID
	for range 4 {
		repo := uuid.New()
		measured = append(measured, repo)
		seed(org, repo, prior, 10)
		seed(org, repo, current, 5)
	}
	currentOnly := uuid.New()
	seed(org, currentOnly, current, 900)
	// Every repository holds a name; the sentence names the drivers by NAME, never
	// by uuid (CHAOS-9046).
	nameRepo := func(org string, repo uuid.UUID, name string) {
		t.Helper()
		at := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
		if err := conn.Exec(context.Background(),
			`INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES (?, ?, ?, ?, ?, 'github')`,
			repo, name, at, at, org); err != nil {
			t.Fatal(err)
		}
	}
	for i, repo := range measured {
		nameRepo(org, repo, fmt.Sprintf("acme/measured-%d", i))
	}
	nameRepo(org, currentOnly, "acme/current-only")
	sentence := churnSentence(org)
	if strings.Contains(sentence, "current-only") || strings.Contains(sentence, currentOnly.String()) {
		t.Errorf("the sentence names the repository with no comparison row as a driver: %s", sentence)
	}
	named := 0
	for i, repo := range measured {
		if strings.Contains(sentence, fmt.Sprintf("acme/measured-%d", i)) {
			named++
		}
		if strings.Contains(sentence, repo.String()) {
			t.Errorf("the sentence prints the repository uuid %s: %s", repo, sentence)
		}
	}
	if named != 3 {
		t.Errorf("the sentence names %d of the measured repositories by name, want the 3 the lookup limit allows: %s", named, sentence)
	}

	// No driver has a delta: every repository with a current value has no
	// comparison row (the prior window is held by a repository that has no
	// current row), so the sentence names no driver.
	const none = "driven-by-none"
	seed(none, uuid.New(), prior, 10)
	seed(none, uuid.New(), current, 5)
	seed(none, uuid.New(), current, 7)
	if sentence := churnSentence(none); strings.Contains(sentence, "driven by") {
		t.Errorf("no driver has a delta, but the sentence names one: %s", sentence)
	}
}
