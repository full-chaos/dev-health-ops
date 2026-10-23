package operational

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/apimetrics"
)

// The instrument is exported through the real provider (apimetrics.InstallWith
// builds it), read back as the Prometheus text /metrics serves: a synthetic
// event type that no handler knows must appear only as "other".
func TestAnUnknownEventTypeIsExportedOnlyAsOther(t *testing.T) {
	source, err := apimetrics.InstallWith(apimetrics.PushOptions{})
	if err != nil {
		t.Fatalf("InstallWith: %v", err)
	}
	const synthetic = "synthetic_unmapped_event_0123456789"
	ctx := context.Background()
	defaultRecordIgnoredWebhookEvent(ctx, "github", "check_run", "delivery-known")
	defaultRecordIgnoredWebhookEvent(ctx, "github", synthetic, "delivery-unknown")
	defaultRecordIgnoredWebhookEvent(ctx, "gitlab", synthetic, "delivery-unknown-2")

	var out bytes.Buffer
	if err := source.WritePrometheus(&out); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}
	var series []string
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "devhealth_operational_webhook_ignored_total") {
			series = append(series, line)
		}
	}
	if len(series) == 0 {
		t.Fatalf("the instrument is not exported at all:\n%s", out.String())
	}
	joined := strings.Join(series, "\n")
	if strings.Contains(joined, synthetic) {
		t.Fatalf("an unknown event type reached the exported series:\n%s", joined)
	}
	for _, want := range []string{`event_type="check_run"`, `event_type="other"`} {
		if !strings.Contains(joined, want) {
			t.Fatalf("exported series lack %s:\n%s", want, joined)
		}
	}
}
