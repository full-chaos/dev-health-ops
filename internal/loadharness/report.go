package loadharness

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ReportSchemaVersion is the version of the JSON report layout.
const ReportSchemaVersion = 1

// PathResult is the raw measurement of one path.
type PathResult struct {
	Name      string
	Latencies []time.Duration
	Errors    int
	Dropped   int
}

// ReportInput is what BuildReport turns into a Report.
type ReportInput struct {
	Profile     string
	StartedAt   time.Time
	Duration    time.Duration
	Rate        float64
	Concurrency int
	Paths       []PathResult
}

// PathReport is one path in the JSON report.
type PathReport struct {
	Name      string  `json:"name"`
	Count     int     `json:"count"`
	Errors    int     `json:"errors"`
	Dropped   int     `json:"dropped"`
	ErrorRate float64 `json:"error_rate"`
	P50Ms     float64 `json:"p50_ms"`
	P95Ms     float64 `json:"p95_ms"`
	P99Ms     float64 `json:"p99_ms"`
	MaxMs     float64 `json:"max_ms"`
}

// SLOCheck is one evaluated objective.
type SLOCheck struct {
	Path   string  `json:"path"`
	Metric string  `json:"metric"`
	Limit  float64 `json:"limit"`
	Actual float64 `json:"actual"`
	OK     bool    `json:"ok"`
}

// Report is the JSON report. It holds names, counts and timings only.
type Report struct {
	SchemaVersion int          `json:"schema_version"`
	Profile       string       `json:"profile"`
	StartedAt     string       `json:"started_at"`
	DurationMs    int64        `json:"duration_ms"`
	Rate          float64      `json:"rate"`
	Concurrency   int          `json:"concurrency"`
	Paths         []PathReport `json:"paths"`
	SLOChecks     []SLOCheck   `json:"slo_checks,omitempty"`
	SLOOK         bool         `json:"slo_ok"`
}

func toMs(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }

// BuildReport summarizes every path; paths are sorted by name.
func BuildReport(in ReportInput) Report {
	r := Report{
		SchemaVersion: ReportSchemaVersion,
		Profile:       in.Profile,
		StartedAt:     in.StartedAt.UTC().Format(time.RFC3339),
		DurationMs:    in.Duration.Milliseconds(),
		Rate:          in.Rate,
		Concurrency:   in.Concurrency,
		SLOOK:         true,
	}
	for _, p := range in.Paths {
		s := Summarize(p.Latencies, p.Errors, p.Dropped)
		r.Paths = append(r.Paths, PathReport{
			Name: p.Name, Count: s.Count, Errors: s.Errors, Dropped: s.Dropped,
			ErrorRate: s.ErrorRate, P50Ms: toMs(s.P50), P95Ms: toMs(s.P95),
			P99Ms: toMs(s.P99), MaxMs: toMs(s.Max),
		})
	}
	sort.Slice(r.Paths, func(i, j int) bool { return r.Paths[i].Name < r.Paths[j].Name })
	return r
}

// WriteReport writes the report as indented JSON.
func WriteReport(w io.Writer, r Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// SLO is a set of upper limits for one path. Limits maps a metric (p50, p95,
// p99, err) to its limit: milliseconds for the percentiles, a ratio for err.
type SLO struct {
	Path   string
	Limits map[string]float64
}

// ParseSLOs parses "path:p95=25ms,p99=75ms,err=0.01" specs.
func ParseSLOs(specs []string) ([]SLO, error) {
	var out []SLO
	for _, spec := range specs {
		path, rest, ok := strings.Cut(spec, ":")
		if !ok || path == "" || rest == "" {
			return nil, fmt.Errorf("slo %q: want path:metric=limit[,metric=limit]", spec)
		}
		slo := SLO{Path: path, Limits: map[string]float64{}}
		for _, kv := range strings.Split(rest, ",") {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return nil, fmt.Errorf("slo %q: %q is not metric=limit", spec, kv)
			}
			switch k {
			case "p50", "p95", "p99":
				d, err := time.ParseDuration(v)
				if err != nil || d < 0 {
					return nil, fmt.Errorf("slo %q: bad duration %q", spec, v)
				}
				slo.Limits[k] = toMs(d)
			case "err":
				f, err := strconv.ParseFloat(v, 64)
				if err != nil || f < 0 || f > 1 {
					return nil, fmt.Errorf("slo %q: err must be a ratio in [0,1]", spec)
				}
				slo.Limits[k] = f
			default:
				return nil, fmt.Errorf("slo %q: unknown metric %q", spec, k)
			}
		}
		out = append(out, slo)
	}
	return out, nil
}

// ApplySLOs evaluates the objectives against the report. An objective on a
// path with no result fails.
func ApplySLOs(r Report, slos []SLO) Report {
	byName := map[string]PathReport{}
	for _, p := range r.Paths {
		byName[p.Name] = p
	}
	r.SLOChecks = nil
	r.SLOOK = true
	for _, s := range slos {
		metrics := make([]string, 0, len(s.Limits))
		for m := range s.Limits {
			metrics = append(metrics, m)
		}
		sort.Strings(metrics)
		p, found := byName[s.Path]
		for _, m := range metrics {
			c := SLOCheck{Path: s.Path, Metric: m, Limit: s.Limits[m]}
			if found {
				switch m {
				case "p50":
					c.Actual = p.P50Ms
				case "p95":
					c.Actual = p.P95Ms
				case "p99":
					c.Actual = p.P99Ms
				case "err":
					c.Actual = p.ErrorRate
				}
				c.OK = c.Actual <= c.Limit
			}
			if !c.OK {
				r.SLOOK = false
			}
			r.SLOChecks = append(r.SLOChecks, c)
		}
	}
	return r
}
