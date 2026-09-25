package billing

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestNotificationHandoffFailureIsAWarning holds that a notification intent
// the handlers fail to queue (here: no database) is a warning, visible at
// the default Info level, not a debug line.
func TestNotificationHandoffFailureIsAWarning(t *testing.T) {
	var logs bytes.Buffer
	h := handlers{logger: slog.New(slog.NewJSONHandler(&logs, nil)), now: func() time.Time { return time.Unix(1790000000, 0) }}
	subscription, err := pyjson.DecodeString(`{"metadata": {"org_id": "88888888-0000-4000-8000-000000000001"}, "trial_end": 1790300000}`)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.trialWillEnd(context.Background(), "evt_trial", subscription); err != nil {
		t.Fatal(err)
	}
	h.subscriptionDeleted(context.Background(), subscription)
	for _, want := range []string{"Failed to enqueue trial expiring email", "Failed to enqueue subscription cancelled email"} {
		found := false
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, want) && strings.Contains(line, `"level":"WARN"`) {
				found = true
			}
		}
		if !found {
			t.Errorf("no WARN line %q:\n%s", want, logs.String())
		}
	}
}
