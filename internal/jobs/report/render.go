package report

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type DeterministicRenderer struct{}

func NewDeterministicRenderer() *DeterministicRenderer {
	return &DeterministicRenderer{}
}

func (*DeterministicRenderer) Render(_ context.Context, input QueryResult) (Artifact, error) {
	if input.Plan.PlanID == "" || input.Plan.ReportType == "" || input.Plan.OrganizationID == "" {
		return Artifact{}, ErrContractMismatch
	}
	narratives := buildNarratives(input.Plan, input.Charts)
	provenance := buildProvenance(input.Plan, input.Charts, narratives)
	markdown := renderMarkdown(input.Plan, input.Charts, narratives, provenance)
	metadata := make(map[string]string, len(input.Metadata)+2)
	for key, value := range input.Metadata {
		metadata[key] = value
	}
	metadata["content_type"] = "text/markdown; charset=utf-8"
	metadata["provenance_schema"] = "report-run.v1"
	return Artifact{Markdown: markdown, Metadata: metadata, Provenance: provenance}, nil
}

type narrative struct {
	sectionType       string
	title             string
	body              string
	supportingMetrics []string
}

var sectionMetrics = map[string][]string{
	"summary":   {},
	"delivery":  {"items_completed", "cycle_time_p50_hours", "lead_time_p50_hours"},
	"quality":   {"failure_rate", "pass_rate", "line_coverage_pct", "coverage_regression_count"},
	"testops":   {"success_rate", "flake_rate", "rerun_rate", "retry_dependency_rate", "avg_queue_seconds", "median_duration_seconds"},
	"wellbeing": {"after_hours_commit_ratio", "weekend_commit_ratio"},
}

var sectionTitles = map[string]string{
	"summary": "Summary", "delivery": "Delivery", "quality": "Quality",
	"testops": "TestOps", "wellbeing": "Wellbeing",
}

func buildNarratives(plan Plan, charts []ChartResult) []narrative {
	available := make(map[string]ChartResult)
	orderedAvailable := make([]string, 0, len(charts))
	for _, chart := range charts {
		if len(chart.DataPoints) == 0 {
			continue
		}
		if _, exists := available[chart.Spec.Metric]; !exists {
			orderedAvailable = append(orderedAvailable, chart.Spec.Metric)
		}
		available[chart.Spec.Metric] = chart
	}
	result := make([]narrative, 0, len(plan.Sections))
	for _, section := range plan.Sections {
		var metrics []string
		if section == "summary" {
			metrics = append(metrics, orderedAvailable...)
			if len(metrics) > 3 {
				metrics = metrics[:3]
			}
		} else {
			for _, metricName := range sectionMetrics[section] {
				if _, ok := available[metricName]; ok {
					metrics = append(metrics, metricName)
				}
			}
		}
		sentences := make([]string, 0, len(metrics))
		for _, metricName := range metrics {
			sentences = append(sentences, chartSentence(available[metricName]))
		}
		body := strings.Join(sentences, "\n\n")
		if body == "" {
			body = "Available evidence appears limited for this section in the current report window."
		}
		title := sectionTitles[section]
		if title == "" {
			title = titleCase(section)
		}
		result = append(result, narrative{sectionType: section, title: title, body: body, supportingMetrics: metrics})
	}
	return result
}

func chartSentence(chart ChartResult) string {
	current := chart.DataPoints[len(chart.DataPoints)-1].Y
	currentText := formatValue(current, chart.Unit)
	if len(chart.DataPoints) < 2 {
		return fmt.Sprintf("%s appears near %s for the selected window.", metricTitle(chart), currentText)
	}
	priorText := formatValue(chart.DataPoints[0].Y, chart.Unit)
	return fmt.Sprintf(
		"%s appears near %s, compared with %s at the opening of the selected window.",
		metricTitle(chart), currentText, priorText,
	)
}

func metricTitle(chart ChartResult) string {
	if definition, ok := supportedMetrics[chart.Spec.Metric]; ok {
		return definition.DisplayName
	}
	return titleCase(chart.Spec.Metric)
}

func formatValue(value float64, unit string) string {
	switch unit {
	case "ratio":
		return fmt.Sprintf("%.1f%%", value*100)
	case "percent":
		return fmt.Sprintf("%.1f%%", value)
	case "seconds":
		return fmt.Sprintf("%.1fs", value)
	case "minutes":
		return fmt.Sprintf("%.1fm", value)
	case "hours":
		return fmt.Sprintf("%.1fh", value)
	case "count":
		return fmt.Sprintf("%.0f", value)
	default:
		return fmt.Sprintf("%.2f", value)
	}
}

func buildProvenance(plan Plan, charts []ChartResult, narratives []narrative) []ProvenanceRecord {
	result := make([]ProvenanceRecord, 0, len(charts)+len(narratives)+1)
	for _, chart := range charts {
		result = append(result, ProvenanceRecord{
			ProvenanceID: provenanceID(plan.PlanID, "chart", chart.Spec.ChartID),
			ArtifactType: "chart", ArtifactID: chart.Spec.ChartID,
			SourceTable: chart.SourceTable, Metric: chart.Spec.Metric,
		})
	}
	for _, section := range narratives {
		source, metricName := "", ""
		if len(section.supportingMetrics) > 0 {
			metricName = section.supportingMetrics[0]
			if definition, ok := supportedMetrics[metricName]; ok {
				source = definition.SourceTable
			}
		}
		result = append(result, ProvenanceRecord{
			ProvenanceID: provenanceID(plan.PlanID, "narrative", section.sectionType),
			ArtifactType: "narrative", ArtifactID: section.sectionType,
			SourceTable: source, Metric: metricName,
		})
	}
	reportMetric := ""
	if len(plan.RequestedMetrics) > 0 {
		reportMetric = plan.RequestedMetrics[0]
	}
	result = append(result, ProvenanceRecord{
		ProvenanceID: provenanceID(plan.PlanID, "report", plan.PlanID),
		ArtifactType: "report", ArtifactID: plan.PlanID, Metric: reportMetric,
	})
	return result
}

func provenanceID(planID, artifactType, artifactID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(planID+":"+artifactType+":"+artifactID)).String()
}

func renderMarkdown(plan Plan, charts []ChartResult, narratives []narrative, provenance []ProvenanceRecord) string {
	window := "unspecified window"
	if plan.TimeRangeStart != "" && plan.TimeRangeEnd != "" {
		window = plan.TimeRangeStart + " → " + plan.TimeRangeEnd
	}
	summary := "Available evidence appears limited for this summary."
	for _, section := range narratives {
		if section.sectionType == "summary" {
			summary = section.body
			break
		}
	}
	lines := []string{
		"# " + titleCase(plan.ReportType) + " Report — " + window,
		"",
		"## Summary",
		summary,
		"",
	}
	for _, section := range narratives {
		if section.sectionType == "summary" {
			continue
		}
		lines = append(lines, "## "+section.title, section.body, "", "### Insights",
			"No grounded insights for this section.", "", "### Charts")
		found := false
		for _, chart := range charts {
			if !contains(section.supportingMetrics, chart.Spec.Metric) {
				continue
			}
			found = true
			lines = append(lines, renderChart(chart), "")
		}
		if !found {
			lines = append(lines, "No charts linked to this section.", "")
		}
	}
	lines = append(lines, "## Provenance", "")
	if len(provenance) == 0 {
		lines = append(lines, "No provenance records available.")
	} else {
		for _, record := range provenance {
			source, metricName := record.SourceTable, record.Metric
			if source == "" {
				source = "n/a"
			}
			if metricName == "" {
				metricName = "n/a"
			}
			lines = append(lines, fmt.Sprintf("- **%s:%s** — sources: %s; metrics: %s",
				record.ArtifactType, record.ArtifactID, source, metricName))
		}
	}
	scope := formatScope(plan)
	generated := strings.TrimSuffix(plan.CreatedAt.UTC().Format(time.RFC3339), "Z") + "+00:00"
	lines = append(lines, "", "---",
		fmt.Sprintf("Generated at %s | Confidence: %s | Scope: %s", generated, plan.ConfidenceThreshold, scope))
	return strings.TrimSpace(strings.Join(lines, "\n")) + "\n"
}

func renderChart(chart ChartResult) string {
	if len(chart.DataPoints) == 0 {
		return "#### " + chart.Title + "\n\n_No data returned for this chart._"
	}
	lines := []string{"#### " + chart.Title, "", "| x | y | group |", "| --- | ---: | --- |"}
	for _, point := range chart.DataPoints {
		lines = append(lines, fmt.Sprintf("| %s | %s | %s |",
			point.X, strconv.FormatFloat(point.Y, 'f', -1, 64), point.Group))
	}
	return strings.Join(lines, "\n")
}

func formatScope(plan Plan) string {
	parts := make([]string, 0, 3)
	if len(plan.ScopeTeams) > 0 {
		parts = append(parts, "teams="+strings.Join(plan.ScopeTeams, ", "))
	}
	if len(plan.ScopeRepos) > 0 {
		parts = append(parts, "repos="+strings.Join(plan.ScopeRepos, ", "))
	}
	if len(plan.ScopeServices) > 0 {
		parts = append(parts, "services="+strings.Join(plan.ScopeServices, ", "))
	}
	if len(parts) == 0 {
		return "global"
	}
	return strings.Join(parts, " | ")
}

func titleCase(value string) string {
	words := strings.Fields(strings.ReplaceAll(value, "_", " "))
	for index := range words {
		if len(words[index]) > 0 {
			words[index] = strings.ToUpper(words[index][:1]) + strings.ToLower(words[index][1:])
		}
	}
	return strings.Join(words, " ")
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

type SHA256ArtifactAdapter struct{}

func NewSHA256ArtifactAdapter() *SHA256ArtifactAdapter {
	return &SHA256ArtifactAdapter{}
}

func (*SHA256ArtifactAdapter) Store(_ context.Context, runID string, artifact Artifact) (Artifact, error) {
	if runID == "" || artifact.Markdown == "" {
		return Artifact{}, ErrContractMismatch
	}
	fingerprint := fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(artifact.Markdown)))
	if artifact.Fingerprint != "" && artifact.Fingerprint != fingerprint {
		return Artifact{}, ErrArtifactConflict
	}
	artifact.Fingerprint = fingerprint
	return artifact, nil
}

// InAppNotificationAdapter represents the current product notification:
// ReportRun's delivered status is visible through GraphQL polling. Python has
// no external report notifier; adding email or webhook delivery here would
// invent a side effect absent from the authoritative path.
type InAppNotificationAdapter struct{}

func NewInAppNotificationAdapter() *InAppNotificationAdapter {
	return &InAppNotificationAdapter{}
}

func (*InAppNotificationAdapter) Notify(_ context.Context, reportID, key string) error {
	if _, err := uuid.Parse(reportID); err != nil || !strings.HasPrefix(key, "report.ready:") {
		return ErrContractMismatch
	}
	if _, err := uuid.Parse(strings.TrimPrefix(key, "report.ready:")); err != nil {
		return ErrContractMismatch
	}
	return nil
}
