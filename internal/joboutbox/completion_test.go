package joboutbox

import "testing"

func TestCompletionKeyIsCanonical(t *testing.T) {
	t.Parallel()
	const id = "00000000-0000-4000-8000-000000000099"
	key, err := CompletionKey("daily_metrics_run", id)
	if err != nil || key != "daily_metrics_run:"+id {
		t.Fatalf("key=%q err=%v", key, err)
	}
	for _, invalid := range []string{
		"",
		"DailyMetricsRun:" + id,
		"daily-metrics-run:" + id,
		"daily_metrics_run:not-a-uuid",
		"daily_metrics_run:00000000-0000-4000-8000-000000000099:extra",
		"daily_metrics_run:00000000-0000-4000-8000-00000000009A",
	} {
		if validCompletionKey(invalid) {
			t.Fatalf("accepted invalid completion key %q", invalid)
		}
	}
}
