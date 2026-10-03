package goapiproof

import (
	"os"
	"strings"
	"testing"
)

func orgsMePair() RESTCounterPair {
	return RESTCounterPair{
		Name:                  "http_requests",
		BaselineFamily:        "http_requests_total",
		CandidateFamily:       "dev_health_api_http_requests_total",
		LabelMap:              map[string]string{"handler": "route", "status": "status_class"},
		IgnoreCandidateLabels: []string{"listener"},
		Select:                map[string]string{"route": "/api/v1/orgs/me", "method": "GET"},
	}
}

func parse(t *testing.T, text string, name string) []CounterSample {
	t.Helper()
	s, err := ParseCounterSamples(text, name)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

const pyBefore = "# TYPE http_requests_total counter\nhttp_requests_total{handler=\"/api/v1/orgs/me\",method=\"GET\",status=\"2xx\"} 3.0\nhttp_requests_total{handler=\"/other\",method=\"GET\",status=\"2xx\"} 9.0\n"
const goBefore = "dev_health_api_http_requests_total{listener=\"public\",method=\"GET\",route=\"/api/v1/orgs/me\",status_class=\"2xx\"} 5\n"

func TestCounterParityAgrees(t *testing.T) {
	p := orgsMePair()
	pyAfter := strings.Replace(pyBefore, "2xx\"} 3.0", "2xx\"} 4.0", 1)
	goAfter := strings.Replace(goBefore, "} 5", "} 6", 1)
	f := CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily),
		parse(t, goBefore, p.CandidateFamily), parse(t, goAfter, p.CandidateFamily))
	if len(f) != 0 {
		t.Fatalf("unexpected findings: %v", f)
	}
}

func TestCounterParityGoSilent(t *testing.T) {
	p := orgsMePair()
	pyAfter := strings.Replace(pyBefore, "2xx\"} 3.0", "2xx\"} 4.0", 1)
	g := parse(t, goBefore, p.CandidateFamily)
	f := CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily), g, g)
	if len(f) != 1 || f[0].Kind != CounterKindMissingOnGo {
		t.Fatalf("want missing-on-go, got %v", f)
	}
}

func TestCounterParityGoNoFamilyAtAll(t *testing.T) {
	p := orgsMePair()
	pyAfter := strings.Replace(pyBefore, "2xx\"} 3.0", "2xx\"} 4.0", 1)
	f := CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily), nil, nil)
	if len(f) != 1 || f[0].Kind != CounterKindMissingOnGo {
		t.Fatalf("got %v", f)
	}
}

func TestCounterParityVacuousAndOtherRouteIgnored(t *testing.T) {
	p := orgsMePair()
	// only an unrelated route moves: the selected route is silent on both
	pyAfter := strings.Replace(pyBefore, "9.0", "10.0", 1)
	f := CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily),
		parse(t, goBefore, p.CandidateFamily), parse(t, goBefore, p.CandidateFamily))
	if len(f) != 1 || f[0].Kind != CounterKindVacuous {
		t.Fatalf("got %v", f)
	}
}

func TestCounterParityDeltaDiffersAndReset(t *testing.T) {
	p := orgsMePair()
	pyAfter := strings.Replace(pyBefore, "2xx\"} 3.0", "2xx\"} 4.0", 1)
	goAfter := strings.Replace(goBefore, "} 5", "} 7", 1)
	f := CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily),
		parse(t, goBefore, p.CandidateFamily), parse(t, goAfter, p.CandidateFamily))
	if len(f) != 1 || f[0].Kind != CounterKindDeltaDiffers {
		t.Fatalf("got %v", f)
	}
	goReset := strings.Replace(goBefore, "} 5", "} 1", 1)
	f = CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily),
		parse(t, goBefore, p.CandidateFamily), parse(t, goReset, p.CandidateFamily))
	found := false
	for _, x := range f {
		found = found || x.Kind == CounterKindScrapeReset
	}
	if !found {
		t.Fatalf("reset not reported: %v", f)
	}
}

func TestParseCounterSamplesMalformedWantedLineFails(t *testing.T) {
	if _, err := ParseCounterSamples("x_total{a=\"b} 1\n", "x_total"); err == nil {
		t.Fatal("want error")
	}
	if _, err := ParseCounterSamples("x_total{a=\"b\"} nope\n", "x_total"); err == nil {
		t.Fatal("want error")
	}
	if s, err := ParseCounterSamples("y_total{a=\"b} 1\n", "x_total"); err != nil || len(s) != 0 {
		t.Fatal("unwanted malformed line must be skipped")
	}
}

func TestRealExpositionFixtures(t *testing.T) {
	for _, tc := range []struct{ file, family string }{
		{"testdata/exposition/api.txt", "http_requests_total"},
		{"testdata/exposition/go-api.txt", "dev_health_api_http_requests_total"},
	} {
		b, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		s, err := ParseCounterSamples(string(b), tc.family)
		if err != nil || len(s) == 0 {
			t.Fatalf("%s: %v n=%d", tc.file, err, len(s))
		}
	}
}

func TestValidateRESTCounterPairs(t *testing.T) {
	if err := ValidateRESTCounterPairs([]RESTCounterPair{orgsMePair()}); err != nil {
		t.Fatal(err)
	}
	bad := orgsMePair()
	bad.CandidateFamily = ""
	if ValidateRESTCounterPairs([]RESTCounterPair{bad}) == nil {
		t.Fatal("want error")
	}
	if ValidateRESTCounterPairs([]RESTCounterPair{orgsMePair(), orgsMePair()}) == nil {
		t.Fatal("duplicate must fail")
	}
}

func TestCounterParityCandidateMovesAnExtraSeries(t *testing.T) {
	p := orgsMePair()
	pyAfter := strings.Replace(pyBefore, "2xx\"} 3.0", "2xx\"} 4.0", 1)
	goAfter := strings.Replace(goBefore, "} 5", "} 6", 1) +
		"dev_health_api_http_requests_total{listener=\"public\",method=\"GET\",route=\"/api/v1/orgs/me\",status_class=\"5xx\"} 1\n"
	f := CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily),
		parse(t, goBefore, p.CandidateFamily), parse(t, goAfter, p.CandidateFamily))
	if len(f) != 1 || f[0].Kind != CounterKindCandidateOnly {
		t.Fatalf("want candidate-only, got %v", f)
	}
}

func TestCounterParitySeriesVanishingBetweenScrapesIsFlagged(t *testing.T) {
	p := orgsMePair()
	pyAfter := strings.Replace(pyBefore, "2xx\"} 3.0", "2xx\"} 4.0", 1)
	// the Go series present before is gone after (process restart), while a
	// different series moved by one: the deltas alone would look fine.
	goVanished := "dev_health_api_http_requests_total{listener=\"public\",method=\"GET\",route=\"/api/v1/orgs/me\",status_class=\"3xx\"} 1\n"
	f := CompareCounterPair(p, parse(t, pyBefore, p.BaselineFamily), parse(t, pyAfter, p.BaselineFamily),
		parse(t, goBefore, p.CandidateFamily), parse(t, goVanished, p.CandidateFamily))
	reset := false
	for _, x := range f {
		reset = reset || x.Kind == CounterKindScrapeReset
	}
	if !reset {
		t.Fatalf("vanished series not flagged: %v", f)
	}
}

func TestCounterDeltasVanishedSeries(t *testing.T) {
	_, ok := CounterDeltas(map[string]float64{"a": 5}, map[string]float64{})
	if ok {
		t.Fatal("a series present before and gone after must not be ok")
	}
	if _, ok := CounterDeltas(map[string]float64{"a": 0}, map[string]float64{}); !ok {
		t.Fatal("a zero series that disappears is not a restart signal")
	}
}
