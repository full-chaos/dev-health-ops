package jobruntime

import (
	"strings"
	"testing"
	"time"
)

// The drain of the pending touched days exports one counter for each of its
// events, from the first scrape, and one gauge: the highest age of the oldest
// pending day over the organizations whose last pass left a day pending.
func TestTouchedDaysDrainCountersAndPendingAgeGauge(t *testing.T) {
	collector, err := NewMetricsCollector(MetricDimensions{})
	if err != nil {
		t.Fatal(err)
	}
	text := collector.PrometheusText()
	for _, event := range touchedDaysDrainEvents() {
		if want := `dev_health_touched_days_drain_total{event="` + string(event) + `"} 0`; !strings.Contains(text, want) {
			t.Fatalf("the first scrape lacks %s", want)
		}
	}
	if want := "dev_health_touched_days_oldest_pending_age_seconds 0\n"; !strings.Contains(text, want) {
		t.Fatalf("the first scrape lacks %q", want)
	}

	if err := collector.ObserveTouchedDaysDrain(TouchedDaysDrainDaysStarted, 31); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTouchedDaysDrain(TouchedDaysDrainDaysStarted, 7); err != nil {
		t.Fatal(err)
	}
	if err := collector.ObserveTouchedDaysDrain("not_an_event", 1); err == nil {
		t.Fatal("an event outside the registered set was counted")
	}
	for organization, age := range map[string]time.Duration{"org-a": 90 * time.Second, "org-b": 2 * time.Hour} {
		if err := collector.ObserveTouchedDaysOldestPendingAge(organization, age); err != nil {
			t.Fatal(err)
		}
	}
	if err := collector.ObserveTouchedDaysOldestPendingAge("", time.Second); err == nil {
		t.Fatal("an age without an organization was kept")
	}
	text = collector.PrometheusText()
	for _, want := range []string{
		`dev_health_touched_days_drain_total{event="days_started"} 38`,
		"dev_health_touched_days_oldest_pending_age_seconds 7200\n",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("the scrape lacks %q", want)
		}
	}
	// The organization with the highest age has nothing pending any more: the
	// gauge falls to the next one, and to 0 when none is left.
	if err := collector.ObserveTouchedDaysOldestPendingAge("org-b", 0); err != nil {
		t.Fatal(err)
	}
	if want := "dev_health_touched_days_oldest_pending_age_seconds 90\n"; !strings.Contains(collector.PrometheusText(), want) {
		t.Fatalf("the scrape lacks %q after org-b drained", want)
	}
	if err := collector.ObserveTouchedDaysOldestPendingAge("org-a", 0); err != nil {
		t.Fatal(err)
	}
	if want := "dev_health_touched_days_oldest_pending_age_seconds 0\n"; !strings.Contains(collector.PrometheusText(), want) {
		t.Fatalf("the scrape lacks %q after every organization drained", want)
	}
}
