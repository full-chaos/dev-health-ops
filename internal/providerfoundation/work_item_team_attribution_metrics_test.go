package providerfoundation

import (
	"bytes"
	"strings"
	"testing"
)

// CHAOS-4244: dev_health_work_item_team_attributions_written_total splits the
// stored assignee_membership rank by WHICH identity resolved it (author vs
// assignee) -- that split is the dimension chris's <=2% target and the
// reporter-membership rescue question both hinge on, not the stored rank
// alone. source="unassigned" is the residual itself. Unbounded inputs
// collapse to "other" rather than minting a series.
func TestWorkItemTeamAttributionWrittenCounterRendersPerProviderAndSource(t *testing.T) {
	t.Parallel()
	metrics := NewMetrics()
	metrics.RecordWorkItemTeamAttributionWritten("GitHub", "author")
	metrics.RecordWorkItemTeamAttributionWritten("github", "author")
	metrics.RecordWorkItemTeamAttributionWritten("github", "assignee")
	metrics.RecordWorkItemTeamAttributionWritten("github", "linked_issue")
	metrics.RecordWorkItemTeamAttributionWritten("github", "unassigned")
	metrics.RecordWorkItemTeamAttributionWritten("jira", "project")
	metrics.RecordWorkItemTeamAttributionWritten("gitlab", "repo")
	metrics.RecordWorkItemTeamAttributionWritten("linear", "native_team")
	var nilMetrics *Metrics
	nilMetrics.RecordWorkItemTeamAttributionWritten("github", "author")

	var output bytes.Buffer
	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	rendered := output.String()
	for _, want := range []string{
		"# TYPE dev_health_work_item_team_attributions_written_total counter",
		`dev_health_work_item_team_attributions_written_total{provider="github",source="author"} 2`,
		`dev_health_work_item_team_attributions_written_total{provider="github",source="assignee"} 1`,
		`dev_health_work_item_team_attributions_written_total{provider="github",source="linked_issue"} 1`,
		`dev_health_work_item_team_attributions_written_total{provider="github",source="unassigned"} 1`,
		`dev_health_work_item_team_attributions_written_total{provider="jira",source="project"} 1`,
		`dev_health_work_item_team_attributions_written_total{provider="gitlab",source="repo"} 1`,
		// native_team is not in the CHAOS-4244 vocabulary -- collapses to "other".
		`dev_health_work_item_team_attributions_written_total{provider="linear",source="other"} 1`,
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("missing %q in:\n%s", want, rendered)
		}
	}
	if got := strings.Count(rendered, "dev_health_work_item_team_attributions_written_total{"); got != 7 {
		t.Fatalf("series=%d want 7 in:\n%s", got, rendered)
	}
}

func TestMetricWorkItemTeamAttributionSourceLabelIsClosedVocabulary(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"author": "author", "Author": "author", "  author  ": "author",
		"assignee": "assignee", "linked_issue": "linked_issue",
		"project": "project", "repo": "repo", "unassigned": "unassigned",
		"native_team": "other", "manual_fallback": "other", "": "other",
	}
	for input, want := range cases {
		if got := MetricWorkItemTeamAttributionSourceLabel(input); got != want {
			t.Fatalf("MetricWorkItemTeamAttributionSourceLabel(%q) = %q, want %q", input, got, want)
		}
	}
}
