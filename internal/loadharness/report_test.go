package loadharness

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func sampleReport() Report {
	return BuildReport(ReportInput{
		Profile:     "normal",
		StartedAt:   time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC),
		Duration:    2 * time.Second,
		Rate:        10,
		Concurrency: 4,
		Paths: []PathResult{
			{Name: "jwks", Latencies: []time.Duration{ms(1), ms(2), ms(3), ms(4)}},
			{Name: "issue", Latencies: []time.Duration{ms(100), ms(200)}, Errors: 1},
		},
	})
}

func TestReportGolden(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteReport(&buf, sampleReport()); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	for _, want := range []string{
		`"schema_version": 1`,
		`"profile": "normal"`,
		`"started_at": "2026-10-24T12:00:00Z"`,
		`"duration_ms": 2000`,
		`"name": "issue"`,
		`"p95_ms": 200`,
		`"error_rate": 0.5`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing %s in\n%s", want, got)
		}
	}
	var back Report
	if err := json.Unmarshal(buf.Bytes(), &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Paths) != 2 || back.Paths[0].Name != "issue" {
		t.Fatalf("paths not sorted by name: %+v", back.Paths)
	}
}

func TestSLOCheck(t *testing.T) {
	r := sampleReport()
	slos, err := ParseSLOs([]string{"jwks:p99=5ms,err=0", "issue:p95=150ms"})
	if err != nil {
		t.Fatal(err)
	}
	r = ApplySLOs(r, slos)
	if r.SLOOK {
		t.Fatal("issue p95 200ms must breach 150ms")
	}
	var failed, passed int
	for _, c := range r.SLOChecks {
		if c.OK {
			passed++
		} else {
			failed++
		}
	}
	if failed != 1 || passed != 2 {
		t.Fatalf("failed=%d passed=%d %+v", failed, passed, r.SLOChecks)
	}
}

func TestSLOUnknownPathFails(t *testing.T) {
	slos, _ := ParseSLOs([]string{"nope:p95=1ms"})
	r := ApplySLOs(sampleReport(), slos)
	if r.SLOOK {
		t.Fatal("an SLO for a path with no result must fail")
	}
}

func TestParseSLOsRejects(t *testing.T) {
	for _, s := range []string{"", "jwks", "jwks:", "jwks:p95", "jwks:p95=abc", "jwks:p77=1ms", "jwks:err=2", ":p95=1ms"} {
		if _, err := ParseSLOs([]string{s}); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
}
