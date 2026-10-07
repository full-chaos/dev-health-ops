package decisioneval

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// DefaultThroughputBatch is the batch of design 9.4 T2: 320 gate-pass bundles.
const DefaultThroughputBatch = 320

// ThroughputConfig configures the T2 report: one batch for each arm, sent with
// the production fan-out bound (concurrency 32), read from the ledger of that
// run. The batch outcomes are used for nothing else.
type ThroughputConfig struct {
	OutDir    string
	ReportDir string
	// Set is the set name of the batch run (DECISIONEVAL_SET=throughput).
	Set string
	// ExpectBatch is the batch size the design asks for (320). 0 skips the check.
	ExpectBatch int
}

// ThroughputArm is T2 for one arm.
type ThroughputArm struct {
	Arm             string  `json:"arm"`
	Rubric          string  `json:"rubric_version"`
	RunID           string  `json:"run_id"`
	Concurrency     string  `json:"concurrency,omitempty"`
	Classifications int     `json:"classifications"`
	Accepted        int     `json:"accepted"`
	WallSeconds     float64 `json:"wall_seconds"`
	PerMinuteAll    float64 `json:"classifications_per_minute_all"`
	PerMinuteOK     float64 `json:"classifications_per_minute_accepted"`
	Attempts        int     `json:"http_attempts"`
	Retries         int     `json:"retries"`
	HTTP429         int     `json:"http_429"`
	HTTP529         int     `json:"http_529"`
	OtherErrors     int     `json:"other_http_errors"`
	LatencyP50Ms    float64 `json:"latency_ms_p50_inside_batch"`
	LatencyP95Ms    float64 `json:"latency_ms_p95_inside_batch"`
}

// ThroughputReport is the T2 report.
type ThroughputReport struct {
	Valid    bool            `json:"valid"`
	Failures []string        `json:"failures"`
	Arms     []ThroughputArm `json:"arms"`
}

// ScoreThroughput reads the ledger of the batch run(s) and writes
// throughput.json and throughput.md.
func ScoreThroughput(cfg ThroughputConfig) (*ThroughputReport, error) {
	if cfg.Set == "" {
		cfg.Set = "throughput"
	}
	if cfg.ReportDir == "" {
		return nil, fmt.Errorf("throughput: report directory is not set")
	}
	if err := RefuseInsideRepo(cfg.ReportDir); err != nil {
		return nil, err
	}
	data, err := ReadLedger(cfg.OutDir)
	if err != nil {
		return nil, err
	}
	rep := &ThroughputReport{}
	conc := map[string]string{}
	for _, r := range data.Runs {
		conc[r.RunID] = r.Config["concurrency"]
	}
	type key struct{ arm, rubric, run string }
	groups := map[key][]ClassificationRecord{}
	for _, c := range data.Classifications {
		if c.Set != cfg.Set || c.Gate != "" {
			continue
		}
		k := key{c.Arm, c.Rubric, c.RunID}
		groups[k] = append(groups[k], c)
	}
	if len(groups) == 0 {
		rep.Failures = append(rep.Failures, fmt.Sprintf("no_classification_records_for_set:%s", cfg.Set))
	}
	keys := make([]key, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].arm != keys[j].arm {
			return keys[i].arm < keys[j].arm
		}
		return keys[i].run < keys[j].run
	})
	for _, k := range keys {
		cs := groups[k]
		ta := ThroughputArm{Arm: k.arm, Rubric: k.rubric, RunID: k.run, Concurrency: conc[k.run], Classifications: len(cs)}
		var start, end time.Time
		var lats []float64
		for _, c := range cs {
			if start.IsZero() || c.StartedAt.Before(start) {
				start = c.StartedAt
			}
			if c.FinishedAt.After(end) {
				end = c.FinishedAt
			}
			if c.Status == categorize.StatusOK || c.Status == categorize.StatusRepaired {
				ta.Accepted++
			}
			lats = append(lats, float64(c.LatencyMs))
			for _, a := range data.AttemptsOf(c) {
				ta.Attempts++
				if a.RetryOf > 0 {
					ta.Retries++
				}
				switch {
				case a.HTTPStatus == 429:
					ta.HTTP429++
				case a.HTTPStatus == 529:
					ta.HTTP529++
				case a.AttemptState == AttemptHTTPError || a.AttemptState == AttemptTransportError:
					ta.OtherErrors++
				}
			}
		}
		ta.WallSeconds = end.Sub(start).Seconds()
		if ta.WallSeconds <= 0 {
			rep.Failures = append(rep.Failures, fmt.Sprintf("no_wall_time:%s:%s (the records carry no start and finish times)", k.arm, k.run))
		} else {
			ta.PerMinuteAll = float64(ta.Classifications) / ta.WallSeconds * 60
			ta.PerMinuteOK = float64(ta.Accepted) / ta.WallSeconds * 60
		}
		ta.LatencyP50Ms, ta.LatencyP95Ms = percentileNearestRank(lats, 50), percentileNearestRank(lats, 95)
		if cfg.ExpectBatch > 0 && ta.Classifications != cfg.ExpectBatch {
			rep.Failures = append(rep.Failures, fmt.Sprintf("batch_size:%s:%s has %d classifications, the design asks for %d", k.arm, k.run, ta.Classifications, cfg.ExpectBatch))
		}
		if ta.Concurrency == "" {
			rep.Failures = append(rep.Failures, fmt.Sprintf("concurrency_unknown:%s:%s", k.arm, k.run))
		}
		rep.Arms = append(rep.Arms, ta)
	}
	sort.Strings(rep.Failures)
	rep.Valid = len(rep.Failures) == 0
	if err := os.MkdirAll(cfg.ReportDir, 0o700); err != nil {
		return rep, err
	}
	raw, _ := json.MarshalIndent(rep, "", "  ")
	if err := os.WriteFile(filepath.Join(cfg.ReportDir, "throughput.json"), raw, 0o600); err != nil {
		return rep, err
	}
	if err := os.WriteFile(filepath.Join(cfg.ReportDir, "throughput.md"), []byte(RenderThroughput(rep)), 0o600); err != nil {
		return rep, err
	}
	if !rep.Valid {
		return rep, fmt.Errorf("throughput: %d measurement problem(s), the report is INVALID: %s", len(rep.Failures), strings.Join(rep.Failures, "; "))
	}
	return rep, nil
}

// RenderThroughput renders T2.
func RenderThroughput(r *ThroughputReport) string {
	var b strings.Builder
	if !r.Valid {
		b.WriteString("# INVALID: measurements are missing or inconsistent\n\n")
		for _, f := range r.Failures {
			b.WriteString("- `" + f + "`\n")
		}
		b.WriteString("\n")
	} else {
		b.WriteString("# T2: throughput at the concurrency limit\n\n")
	}
	b.WriteString("| arm | rubric | concurrency | classifications | accepted | wall s | per minute (all / accepted) | attempts | retries | 429 | 529 | other errors | p50 / p95 ms inside the batch |\n|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, a := range r.Arms {
		fmt.Fprintf(&b, "| %s | %s | %s | %d | %d | %.1f | %.1f / %.1f | %d | %d | %d | %d | %d | %.0f / %.0f |\n", a.Arm, a.Rubric, a.Concurrency, a.Classifications, a.Accepted,
			a.WallSeconds, a.PerMinuteAll, a.PerMinuteOK, a.Attempts, a.Retries, a.HTTP429, a.HTTP529, a.OtherErrors, a.LatencyP50Ms, a.LatencyP95Ms)
	}
	b.WriteString("\n")
	return b.String()
}
