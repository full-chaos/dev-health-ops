package restprove

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

func metricsServer(line string, step float64, start float64) *httptest.Server {
	var n atomic.Int64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		v := start + float64(n.Add(1)-1)*step
		fmt.Fprintf(w, line, v)
	}))
}

func runCounterScrape(t *testing.T, baseStep, candStep float64) string {
	pairs := []goapiproof.RESTCounterPair{{
		Name: "c", BaselineFamily: "b_total", CandidateFamily: "c_total",
	}}
	b := metricsServer("b_total{a=\"x\"} %g\n", baseStep, 3)
	c := metricsServer("c_total{a=\"x\"} %g\n", candStep, 5)
	defer b.Close()
	defer c.Close()
	f := flags{baselineMetricsURL: b.URL, candidateMetricsURL: c.URL}
	cs := newCounterScrape(pairs)
	ctx := context.Background()
	cs.scrape(ctx, goapiproof.NewLegClient(5*time.Second), f, true, true)
	cs.scrape(ctx, goapiproof.NewLegClient(5*time.Second), f, true, false)
	cs.scrape(ctx, goapiproof.NewLegClient(5*time.Second), f, false, true)
	cs.scrape(ctx, goapiproof.NewLegClient(5*time.Second), f, false, false)
	return cs.verdict()
}

func TestCounterScrapeAgreeAndDiffer(t *testing.T) {
	if v := runCounterScrape(t, 1, 1); v != "" {
		t.Fatalf("agree: %s", v)
	}
	if v := runCounterScrape(t, 1, 0); v == "" {
		t.Fatal("silent candidate must fail")
	}
}

func TestCounterScrapeNoURLFails(t *testing.T) {
	cs := newCounterScrape([]goapiproof.RESTCounterPair{{Name: "c", BaselineFamily: "b", CandidateFamily: "c"}})
	cs.scrape(context.Background(), goapiproof.NewLegClient(5*time.Second), flags{}, true, true)
	if cs.verdict() == "" {
		t.Fatal("missing URL must fail")
	}
	if newCounterScrape(nil).verdict() != "" {
		t.Fatal("no pairs = no verdict")
	}
}
