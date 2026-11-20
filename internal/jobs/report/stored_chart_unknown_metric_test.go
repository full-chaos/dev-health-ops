package report

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/extension"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// A stored chart spec whose metric is not an exact registry key does not stop the run: the spec is dropped at decode, the
// authored name is listed as uncharted, and one WARN (no name) plus one counter tick tell an operator.

type unknownMetricLoader struct{ plan []byte }

func (loader unknownMetricLoader) Load(_ context.Context, input QueryInput) (ReportDefinition, error) {
	return decodeReportDefinition(input.ReportID, "org-1", loader.plan, nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
}

type countingConn struct {
	driver.Conn
	queried int
}

func (conn *countingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queried++
	return nil, errors.New("inert connection")
}

func storedSpecPlan(requested string, specs ...[2]string) []byte {
	entries := make([]string, 0, len(specs))
	for index, spec := range specs {
		group := "null"
		if spec[1] != "" {
			group = `"` + spec[1] + `"`
		}
		metric, _ := json.Marshal(spec[0])
		entries = append(entries, fmt.Sprintf(`{"chart_id":"c%d","plan_id":"p1","chart_type":"line","metric":%s,"group_by":%s,"org_id":"org-1"}`,
			index, metric, group))
	}
	return []byte(fmt.Sprintf(`{"plan_id":"p1","report_type":"weekly_health","org_id":"org-1","sections":["summary"],
"requested_metrics":[%s],"created_at":"2026-01-08T00:00:00Z","chart_specs":[%s]}`, requested, strings.Join(entries, ",")))
}

type uncharted14329Probe struct {
	logs  *bytes.Buffer
	count func() int64
}

func probeUncharted(t *testing.T) uncharted14329Probe {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	counter, err := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test").Int64Counter(unchartedStoredSpecCounterName)
	if err != nil {
		t.Fatal(err)
	}
	previousCounter, previousLogger := unchartedStoredSpecCounter, slog.Default()
	t.Cleanup(func() { unchartedStoredSpecCounter = previousCounter; slog.SetDefault(previousLogger) })
	unchartedStoredSpecCounter = counter
	logs := &bytes.Buffer{}
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, nil)))
	return uncharted14329Probe{logs: logs, count: func() int64 {
		var data metricdata.ResourceMetrics
		if err := reader.Collect(context.Background(), &data); err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, scope := range data.ScopeMetrics {
			for _, m := range scope.Metrics {
				if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == unchartedStoredSpecCounterName {
					for _, point := range sum.DataPoints {
						total += point.Value
					}
				}
			}
		}
		return total
	}}
}

func queryStored(t *testing.T, plan []byte) (QueryResult, *countingConn, error) {
	t.Helper()
	conn := &countingConn{}
	adapter := &ClickHouseQueryAdapter{loader: unknownMetricLoader{plan}, conn: conn,
		now: func() time.Time { return time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC) }}
	result, err := adapter.Query(context.Background(), QueryInput{ReportID: "r1", RunID: "run1"})
	return result, conn, err
}

func TestStoredChartWithACustomMetricNameDoesNotStopTheRun(t *testing.T) {
	for _, name := range []string{"Release _Candidate_", "Pipelines_Count"} {
		for _, group := range []string{"day", "team", ""} {
			t.Run(name+"/"+group, func(t *testing.T) {
				probe := probeUncharted(t)
				result, conn, err := queryStored(t, storedSpecPlan(`"`+name+`"`, [2]string{name, group}))
				if err != nil {
					t.Fatalf("run stopped: %v", err)
				}
				want := []UnchartedMetric{{Label: name, Reason: unchartedNoMetric}}
				if len(result.Charts) != 0 || fmt.Sprint(result.Plan.UnchartedMetrics) != fmt.Sprint(want) || conn.queried != 0 {
					t.Fatalf("charts=%d uncharted=%v queried=%d, want 0 charts, %v, 0 queries", len(result.Charts), result.Plan.UnchartedMetrics, conn.queried, want)
				}
				if probe.count() != 1 || strings.Count(probe.logs.String(), "stored chart spec dropped") != 1 ||
					strings.Contains(probe.logs.String(), name) || !strings.Contains(probe.logs.String(), "plan_id=p1") {
					t.Fatalf("want one tick and one WARN with the plan id and no name: count=%d logs=%q", probe.count(), probe.logs.String())
				}
				artifact, err := NewDeterministicRenderer().Render(context.Background(), result)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Count(artifact.Markdown, "## Requested Metrics Without a Chart\n") != 1 ||
					strings.Count(artifact.Markdown, ": no metric matches this label") != 1 {
					t.Fatalf("report does not list the name once:\n%s", artifact.Markdown)
				}
			})
		}
	}
}

func TestStoredChartWithAnExactRegistryKeyStillReachesTheQuery(t *testing.T) {
	probe := probeUncharted(t)
	_, conn, err := queryStored(t, storedSpecPlan(`"pipelines_count"`, [2]string{"pipelines_count", "day"}))
	if err == nil || conn.queried != 1 || probe.count() != 0 || strings.Contains(probe.logs.String(), "stored chart spec dropped") {
		t.Fatalf("control: err=%v queried=%d ticks=%d logs=%q, want the query reached and no drop", err, conn.queried, probe.count(), probe.logs.String())
	}
}

func TestStoredChartKeepsItsKeyAndAuthoredTitle(t *testing.T) {
	plan := []byte(strings.Replace(string(storedSpecPlan(`"pipelines_count"`, [2]string{"pipelines_count", "day"})),
		`"org_id":"org-1"}]`, `"org_id":"org-1","title":"Release cadence"}]`, 1))
	definition, err := decodeReportDefinition("r1", "org-1", plan, nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil || len(definition.Charts) != 1 || definition.Charts[0].Metric != "pipelines_count" ||
		definition.Charts[0].Title != "Release cadence" || len(definition.Plan.UnchartedMetrics) != 0 {
		t.Fatalf("definition=%#v err=%v", definition, err)
	}
}

func TestStoredPlanMixesAKeyAndCustomNames(t *testing.T) {
	probe := probeUncharted(t)
	plan := storedSpecPlan(`"Release _Candidate_","pipelines_count","Release _Candidate_"`,
		[2]string{"Release _Candidate_", "day"}, [2]string{"pipelines_count", "day"}, [2]string{"Release _Candidate_", "team"}, [2]string{" ", "day"})
	definition, err := decodeReportDefinition("r1", "org-1", plan, nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(definition.Charts) != 1 || definition.Charts[0].Metric != "pipelines_count" || definition.Charts[0].ChartID != "c1" {
		t.Fatalf("charts = %#v, want only the registry key", definition.Charts)
	}
	if got := fmt.Sprint(definition.Plan.UnchartedMetrics); got != fmt.Sprint([]UnchartedMetric{{Label: "Release _Candidate_", Reason: unchartedNoMetric}}) {
		t.Fatalf("uncharted = %s, want the custom name once and no blank name", got)
	}
	if probe.count() != 3 {
		t.Fatalf("ticks = %d, want 1 per dropped spec (3)", probe.count())
	}
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{Plan: definition.Plan,
		Charts: []ChartResult{{Spec: definition.Charts[0], Title: "Pipelines count"}}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(artifact.Markdown, "- Release \\_Candidate\\_:") != 1 || strings.Contains(artifact.Markdown, "ArtifactID: c0") {
		t.Fatalf("render:\n%s", artifact.Markdown)
	}
	for _, record := range artifact.Provenance {
		if record.ArtifactType == "chart" && record.ArtifactID != "c1" {
			t.Fatalf("provenance holds a chart for a dropped spec: %#v", record)
		}
	}
}

func TestStoredChartWithABlankMetricIsNotListed(t *testing.T) {
	probe := probeUncharted(t)
	definition, err := decodeReportDefinition("r1", "org-1", storedSpecPlan(`""`, [2]string{" ", "day"}), nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil || len(definition.Charts) != 0 || len(definition.Plan.UnchartedMetrics) != 0 || probe.count() != 1 {
		t.Fatalf("definition=%#v err=%v ticks=%d", definition, err, probe.count())
	}
	if _, err := decodeReportDefinition("r1", "org-1", storedSpecPlan(`""`, [2]string{"", "day"}), nil, time.Now()); !errors.Is(err, ErrContractMismatch) {
		t.Fatalf("an empty metric must stay a contract mismatch, got %v", err)
	}
}

// The uncharted list shows the label as stored: whitespace the reader would drop or fold is written as a visible escape,
// the raw string stays the lookup and dedupe key (a padded registry key is not the key and does not chart).

func unchartedBullets(markdown string) []string {
	var bullets []string
	listed := false
	for _, line := range strings.Split(markdown, "\n") {
		if line == "## Requested Metrics Without a Chart" {
			listed = true
			continue
		}
		if listed && strings.HasPrefix(line, "## ") {
			break
		}
		if listed && strings.HasPrefix(line, "- ") {
			bullets = append(bullets, line)
		}
	}
	return bullets
}

func renderStoredSpecs(t *testing.T, specs ...[2]string) (ReportDefinition, string) {
	t.Helper()
	definition, err := decodeReportDefinition("r1", "org-1", storedSpecPlan(`""`, specs...), nil, time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	var charts []ChartResult
	for _, spec := range definition.Charts {
		charts = append(charts, ChartResult{Spec: spec, Title: "Pipelines count", SourceTable: "cicd_metrics_daily", Unit: "count",
			DataPoints: []DataPoint{{X: "2026-01-01", Y: 1}, {X: "2026-01-02", Y: 2}}})
	}
	artifact, err := NewDeterministicRenderer().Render(context.Background(), QueryResult{Plan: definition.Plan, Charts: charts})
	if err != nil {
		t.Fatal(err)
	}
	return definition, artifact.Markdown
}

var storedGroups = []string{"day", "team", ""}

func edgeSpaceRunes() []rune {
	var runes []rune
	for r := rune(0); r < 0x10000; r++ {
		if unicode.IsSpace(r) {
			runes = append(runes, r)
		}
	}
	return runes
}

func TestUnchartedListShowsAPaddedRegistryKeyAsPadded(t *testing.T) {
	for _, group := range storedGroups {
		for _, other := range storedGroups {
			for _, padded := range []string{"pipelines_count ", " pipelines_count", "pipelines_count\n", "pipelines_count\r\n", "\tpipelines_count"} {
				definition, markdown := renderStoredSpecs(t, [2]string{"pipelines_count", group}, [2]string{padded, other})
				bullets := unchartedBullets(markdown)
				if len(definition.Charts) != 1 || definition.Charts[0].Metric != "pipelines_count" || len(bullets) != 1 {
					t.Fatalf("%q/%q/%q: charts=%v bullets=%q, want only the exact key charted and one bullet", group, other, padded, definition.Charts, bullets)
				}
				if strings.HasPrefix(bullets[0], "- pipelines\\_count:") || !strings.Contains(bullets[0], "`\\") {
					t.Errorf("%q/%q/%q: bullet %q reads as the charted key, want a visible escape", group, other, padded, bullets[0])
				}
			}
		}
	}
}

func TestUnchartedListGivesEveryRawDistinctLabelADistinctBullet(t *testing.T) {
	labels := []string{"Release"}
	for _, r := range edgeSpaceRunes() {
		labels = append(labels, string(r)+"Release", "Release"+string(r))
	}
	labels = append(labels, "a b", "a\nb", "a\rb", "a\r\nb", "a\tb", "a b", "a\\nb", "a  b", "Release  ", " Release ")
	for _, group := range storedGroups {
		for _, other := range storedGroups {
			specs := make([][2]string, 0, len(labels))
			for index, label := range labels {
				groupBy := group
				if index%2 == 1 {
					groupBy = other
				}
				specs = append(specs, [2]string{label, groupBy})
			}
			_, markdown := renderStoredSpecs(t, specs...)
			bullets := unchartedBullets(markdown)
			seen := map[string]bool{}
			for _, bullet := range bullets {
				seen[bullet] = true
			}
			for _, bullet := range bullets {
				if !strings.HasSuffix(bullet, ": no metric matches this label") || strings.ContainsFunc(bullet, func(r rune) bool { return unicode.IsSpace(r) && r != ' ' }) {
					t.Fatalf("%q/%q: bullet %q holds a raw line break or space other than ' '", group, other, bullet)
				}
			}
			if len(bullets) != len(labels) || len(seen) != len(labels) {
				t.Fatalf("%q/%q: %d raw-distinct labels gave %d bullets, %d distinct", group, other, len(labels), len(bullets), len(seen))
			}
		}
	}
}

func TestUnchartedListWritesAPlainLabelAsBefore(t *testing.T) {
	for _, label := range []string{"Release _Candidate_", "pipelines_count_x", "two words", "a  b", "- dash", "1. one", "Lead *Time*|x ## Fake", "Lead_Time"} {
		_, markdown := renderStoredSpecs(t, [2]string{label, "day"})
		want := "- " + markdownLineStart(markdownTableCell(label)) + ": no metric matches this label"
		if bullets := unchartedBullets(markdown); len(bullets) != 1 || bullets[0] != want {
			t.Errorf("label %q: bullets %q, want %q", label, bullets, want)
		}
	}
}

// A pasted URL keeps its link destination and its escape stays a code span: the escape span after text is separated by one
// space, because a backtick next to a URL is taken into its GFM autolink.

var linkTarget = regexp.MustCompile(`href="([^"]*)"`)

func gfmLinks(t *testing.T, bullet string) (string, []string) {
	t.Helper()
	var out bytes.Buffer
	if err := goldmark.New(goldmark.WithExtensions(extension.GFM)).Convert([]byte(bullet), &out); err != nil {
		t.Fatal(err)
	}
	var hrefs []string
	for _, match := range linkTarget.FindAllStringSubmatch(out.String(), -1) {
		hrefs = append(hrefs, match[1])
	}
	return out.String(), hrefs
}

func singleBullet(t *testing.T, label string) string {
	t.Helper()
	_, markdown := renderStoredSpecs(t, [2]string{label, "day"})
	bullets := unchartedBullets(markdown)
	if len(bullets) != 1 {
		t.Fatalf("label %q: bullets %q", label, bullets)
	}
	return bullets[0]
}

func TestUnchartedUrlLabelKeepsItsLinkDestinationAndItsEscapeSpan(t *testing.T) {
	urls := map[string]string{
		"https://ci.example.com/builds": "https://ci.example.com/builds", "http://ci.example.com/builds": "http://ci.example.com/builds",
		"www.ci.example.com/builds": "http://www.ci.example.com/builds", "https://ci.example.com/builds?a=1": "https://ci.example.com/builds?a=1",
		"see https://ci.example.com/builds": "https://ci.example.com/builds",
	}
	for url, href := range urls {
		if _, got := gfmLinks(t, singleBullet(t, url)); fmt.Sprint(got) != fmt.Sprint([]string{href}) {
			t.Fatalf("plain %q: destinations %q, want [%q]", url, got, href)
		}
		if want := "- " + markdownLineStart(markdownTableCell(url)) + ": no metric matches this label"; singleBullet(t, url) != want {
			t.Fatalf("plain %q is not byte-equal to the base text %q", url, want)
		}
		for _, space := range []string{"\n", "\r\n", "\t", " ", " "} {
			for position, label := range map[string]string{"trailing": url + space, "interior": url + space + "tail", "leading": space + url} {
				html, got := gfmLinks(t, singleBullet(t, label))
				if fmt.Sprint(got) != fmt.Sprint([]string{href}) || (!strings.Contains(html, "<code>") && !(position == "interior" && space == " ")) {
					t.Errorf("%s %q + %q: destinations %q, want [%q] and an escape code span; html %s", position, url, space, got, href, html)
				}
			}
		}
	}
}

func TestUnchartedLabelWithoutAnEscapeGetsNoSeparator(t *testing.T) {
	for _, label := range []string{"ci.example.com", "team@example.com", "a b", "a  b", "Release _Candidate_"} {
		for _, space := range []string{"\n", " ", "\t"} {
			html, got := gfmLinks(t, singleBullet(t, label+space))
			_, want := gfmLinks(t, singleBullet(t, label))
			if fmt.Sprint(got) != fmt.Sprint(want) || !strings.Contains(html, "<code>") {
				t.Errorf("%q + %q: destinations %q want %q, html %s", label, space, got, want, html)
			}
		}
	}
	if bullet := singleBullet(t, "\nRelease"); !strings.HasPrefix(bullet, "- `\\n`Release") {
		t.Errorf("a leading escape gets a separator: %q", bullet)
	}
	if bullet := singleBullet(t, "Release\n"); !strings.HasPrefix(bullet, "- Release `\\n`: ") {
		t.Errorf("a trailing escape is not separated by one ASCII space: %q", bullet)
	}
}

func TestUnchartedSeparatorNeverMakesTwoRawLabelsEqual(t *testing.T) {
	groups := [][]string{
		{"abc\n", "abc \n", "abc  \n", "abc", "abc ", "abc  "},
		{"a\nb", "a \nb", "a\n b", "a \n b", "a b", "a  b"},
		{"https://ci.example.com/b\n", "https://ci.example.com/b \n", "https://ci.example.com/b  \n"},
	}
	for _, labels := range groups {
		specs := make([][2]string, 0, len(labels))
		for _, label := range labels {
			specs = append(specs, [2]string{label, "day"})
		}
		_, markdown := renderStoredSpecs(t, specs...)
		bullets := unchartedBullets(markdown)
		seen := map[string]bool{}
		for _, bullet := range bullets {
			seen[bullet] = true
		}
		if len(bullets) != len(labels) || len(seen) != len(labels) {
			t.Errorf("%q: %d bullets, %d distinct, want %d: %q", labels, len(bullets), len(seen), len(labels), bullets)
		}
	}
}
