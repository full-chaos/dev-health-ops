package report

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

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
	"cicd":      {"pipelines_count", "avg_duration_minutes", "p90_duration_minutes", "avg_queue_minutes"},
}

var sectionTitles = map[string]string{
	"summary": "Summary", "delivery": "Delivery", "quality": "Quality",
	"testops": "TestOps", "wellbeing": "Wellbeing", "cicd": "CI/CD",
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
			title = section
		}
		result = append(result, narrative{sectionType: section, title: title, body: body, supportingMetrics: metrics})
	}
	return result
}

// groupAxisNouns name the group axes: their points are groups ordered by value, not times, so they get no comparison
// with the opening of the window.
var groupAxisNouns = map[string]string{"team": "team", "repo": "repository", "service": "service"}

func chartSentence(chart ChartResult) string {
	current := chart.DataPoints[len(chart.DataPoints)-1].Y
	currentText := formatValue(current, chart.Unit)
	if len(chart.DataPoints) < 2 {
		return fmt.Sprintf("%s appears near %s for the selected window.", metricTitle(chart), currentText)
	}
	if noun, grouped := groupAxisNouns[chart.Spec.GroupBy]; grouped {
		low, high := current, current
		for _, point := range chart.DataPoints {
			low, high = min(low, point.Y), max(high, point.Y)
		}
		return fmt.Sprintf("%s ranges from %s to %s across %d %s groups in the selected window.",
			metricTitle(chart), formatValue(low, chart.Unit), formatValue(high, chart.Unit), len(chart.DataPoints), noun)
	}
	priorText := formatValue(chart.DataPoints[0].Y, chart.Unit)
	return fmt.Sprintf(
		"%s appears near %s, compared with %s at the opening of the selected window.",
		metricTitle(chart), currentText, priorText,
	)
}

func metricTitle(chart ChartResult) string {
	if definition, ok := supportedMetrics[chart.Spec.Metric]; ok {
		return markdownLineStart(markdownText(definition.DisplayName))
	}
	return markdownLineStart(markdownText(chart.Spec.Metric))
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

// otherChartsTitle heads the charts no listed section printed; a plan section whose title is this name (case ignored) is renamed so the heading is unique.
const otherChartsTitle = "Other Charts"

func renderMarkdown(plan Plan, charts []ChartResult, narratives []narrative, provenance []ProvenanceRecord) string {
	window := "unspecified window"
	if plan.TimeRangeStart != "" && plan.TimeRangeEnd != "" {
		window = markdownText(plan.TimeRangeStart) + " → " + markdownText(plan.TimeRangeEnd)
	}
	summary := "Available evidence appears limited for this summary."
	for _, section := range narratives {
		if section.sectionType == "summary" {
			summary = section.body
			break
		}
	}
	lines := []string{
		"# " + markdownText(reportTypeTitle(plan.ReportType)) + " Report — " + window,
		"",
		"## Summary",
		summary,
		"",
	}
	printed := make([]bool, len(charts))
	for _, section := range narratives {
		if section.sectionType == "summary" {
			continue
		}
		title := section.title
		if strings.EqualFold(strings.TrimSpace(title), otherChartsTitle) {
			title = otherChartsTitle + " (plan section)"
		}
		lines = append(lines, "## "+markdownText(title), section.body, "", "### Insights",
			"No grounded insights for this section.", "", "### Charts")
		found := false
		for index, chart := range charts {
			if printed[index] || !contains(section.supportingMetrics, chart.Spec.Metric) {
				continue
			}
			found = true
			printed[index] = true
			lines = append(lines, renderChart(chart), "")
		}
		if !found {
			lines = append(lines, "No charts linked to this section.", "")
		}
	}
	var unlisted []ChartResult
	for index, chart := range charts {
		if !printed[index] {
			unlisted = append(unlisted, chart)
		}
	}
	if len(unlisted) > 0 {
		lines = append(lines, "## "+otherChartsTitle)
		for _, chart := range unlisted {
			lines = append(lines, renderChart(chart), "")
		}
	}
	if len(plan.UnchartedMetrics) > 0 {
		lines = append(lines, "## Requested Metrics Without a Chart")
		for _, metric := range plan.UnchartedMetrics {
			lines = append(lines, "- "+markdownListStart(metric.Label)+": "+markdownText(metric.Reason))
		}
		lines = append(lines, "")
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
				markdownText(strings.TrimSpace(record.ArtifactType)), markdownText(strings.TrimSpace(record.ArtifactID)), markdownText(source), markdownText(metricName)))
		}
	}
	scope := formatScope(plan)
	generated := strings.TrimSuffix(plan.CreatedAt.UTC().Format(time.RFC3339), "Z") + "+00:00"
	lines = append(lines, "", "---",
		fmt.Sprintf("Generated at %s | Confidence: %s | Scope: %s", generated, markdownText(plan.ConfidenceThreshold), scope))
	return strings.TrimSpace(strings.Join(lines, "\n")) + "\n"
}

func renderChart(chart ChartResult) string {
	if chart.NotApplicable != "" {
		return "#### " + markdownText(chart.Title) + "\n\n" + emphasisLine("Not applicable", chart.NotApplicable)
	}
	note := ""
	if chart.Note != "" {
		note = "\n\n" + emphasisLine("Note", chart.Note)
	}
	if len(chart.DataPoints) == 0 {
		return "#### " + markdownText(chart.Title) + "\n\n_No data returned for this chart._" + note
	}
	lines := []string{"#### " + markdownText(chart.Title), "", "| x | y | group |", "| --- | ---: | --- |"}
	for _, point := range chart.DataPoints {
		lines = append(lines, fmt.Sprintf("| %s | %s | %s |",
			markdownTableCell(point.X), strconv.FormatFloat(point.Y, 'f', -1, 64), markdownTableCell(point.Group)))
	}
	return strings.Join(lines, "\n") + note
}

var markdownLineBreaks = strings.NewReplacer("\r\n", " ", "\n", " ", "\r", " ")

// escapeMarkdown folds a line break to a space and puts a backslash before each character of escapes. A value
// cannot start a block, a heading or a provenance split (a backslash and "#" are always in the set); inline Markdown
// inside a value may render.
func escapeMarkdown(value, escapes string) string {
	value = markdownLineBreaks.Replace(value)
	var b strings.Builder
	for _, r := range value {
		if strings.ContainsRune(escapes, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// markdownText writes free text into a heading, note or list line, edge space dropped (a reader drops it there and a
// closing marker after a space does not close): each character that starts inline Markdown, an
// entity, a heading or a backslash escape gets a backslash. An "_" between two ASCII letters or digits stays plain:
// CommonMark never reads it as emphasis, and source-table and metric names hold it.
func markdownText(value string) string {
	runes := []rune(strings.TrimSpace(markdownLineBreaks.Replace(value)))
	var b strings.Builder
	for i, r := range runes {
		if strings.ContainsRune("\\*_`[]<>~&#", r) && !(r == '_' && i > 0 && i < len(runes)-1 && asciiAlnum(runes[i-1]) && asciiAlnum(runes[i+1])) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func asciiAlnum(r rune) bool {
	return r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

// emphasisLine writes "_label: value_". Surrounding space of the value is dropped: a closing "_" after a space does
// not close the emphasis and the line would show its underscores.
func emphasisLine(label, value string) string {
	if value = strings.TrimSpace(markdownLineBreaks.Replace(value)); value == "" {
		return "_" + label + ":_"
	}
	return "_" + label + ": " + markdownText(value) + "_"
}

// markdownListStart writes the first text of a list item as one table-cell escaped label. Whitespace the reader would
// drop or fold (a run at either edge, or any run with a tab, line break or other space inside) is written as a visible
// code span of escapes (`\n`, `\u0020`), so two labels that differ only by such whitespace read differently and a
// padded name does not read as the name without padding. A single ASCII space between words stays a space. A span that
// follows text gets one space before it: a backtick next to a pasted URL would join its link (a space ends it).
func markdownListStart(value string) string {
	runes := []rune(value)
	var b strings.Builder
	for i := 0; i < len(runes); {
		if !unicode.IsSpace(runes[i]) {
			j := i
			for j < len(runes) && !unicode.IsSpace(runes[j]) {
				j++
			}
			b.WriteString(markdownTableCell(string(runes[i:j])))
			i = j
			continue
		}
		j := i
		plain := true
		for j < len(runes) && unicode.IsSpace(runes[j]) {
			plain = plain && runes[j] == ' '
			j++
		}
		if plain && i > 0 && j < len(runes) {
			b.WriteString(string(runes[i:j]))
		} else {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteByte('`')
			for _, r := range runes[i:j] {
				switch r {
				case '\n':
					b.WriteString(`\n`)
				case '\r':
					b.WriteString(`\r`)
				case '\t':
					b.WriteString(`\t`)
				default:
					fmt.Fprintf(&b, `\u%04x`, r)
				}
			}
			b.WriteByte('`')
		}
		i = j
	}
	return markdownLineStart(b.String())
}

// markdownLineStart keeps escaped text that opens a line from opening a block: a leading "-" or "+" (list, rule) or a
// number with "." or ")" (ordered list). The other block starters ("#", ">", backtick, "~", "*", "=") are already escaped.
func markdownLineStart(text string) string {
	digits := 0
	for digits < len(text) && text[digits] >= '0' && text[digits] <= '9' {
		digits++
	}
	switch {
	case strings.HasPrefix(text, "-"), strings.HasPrefix(text, "+"):
		return "\\" + text
	case digits > 0 && digits < len(text) && (text[digits] == '.' || text[digits] == ')'):
		return text[:digits] + "\\" + text[digits:]
	}
	return text
}

// markdownTableCell writes a label as one table cell that reads as the label: each character that splits a cell,
// starts inline Markdown or starts a heading gets a backslash escape.
func markdownTableCell(value string) string { return escapeMarkdown(value, "\\|*_`[]<>~&#") }

func formatScope(plan Plan) string {
	parts := make([]string, 0, 3)
	if len(plan.ScopeTeams) > 0 {
		parts = append(parts, "teams="+markdownText(strings.Join(plan.ScopeTeams, ", ")))
	}
	if len(plan.ScopeRepos) > 0 {
		parts = append(parts, "repos="+markdownText(strings.Join(plan.ScopeRepos, ", ")))
	}
	if len(plan.ScopeServices) > 0 {
		parts = append(parts, "services="+markdownText(strings.Join(plan.ScopeServices, ", ")))
	}
	if len(parts) == 0 {
		return "global"
	}
	return strings.Join(parts, " | ")
}

func reportTypeTitle(value string) string {
	if _, ok := builtinReportTypes[value]; ok {
		return titleCase(value)
	}
	return value
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
