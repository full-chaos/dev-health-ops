package syncdispatchruntime

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// D4262 (b): no any reaches the logger: an observation map is read by key name and type; an unknown key, or a known key with a
// value of another type, is not logged; a value that is not a plain token is the marker "invalid".
func TestObservationAttrsReadKnownKeysByTypeAndIgnoreTheRest(t *testing.T) {
	const marker = "the planted detail of ticket 7933 observation"
	var out bytes.Buffer
	logger := synclog.New(slog.New(slog.NewJSONHandler(&out, nil)))
	logger.Info(context.Background(), synclog.MsgDispatchSyncRunBudgetGuardAllowed, observationAttrs(readObservation(map[string]any{
		"sync_run_id": "123e4567-e89b-12d3-a456-426614174000", "decision": "allowed", "budget_limit": 5, "dataset_key": marker,
		"error": marker, "reason": errors.New(marker), "unit_id": 7, "estimated_units": marker, "extra": []string{marker},
		"bucket": map[string]any{"provider": "github", "host": "h", "error": marker, "dimension": errors.New(marker)},
	}))...)
	line := out.String()
	if strings.Contains(line, "planted") {
		t.Fatalf("an unknown key, a mistyped value or a sentence was logged: %s", line)
	}
	for _, want := range []string{`"sync_run_id":"123e4567-e89b-12d3-a456-426614174000"`, `"decision":"allowed"`, `"budget_limit":5`, `"dataset_key":"invalid"`, `"bucket":{`, `"provider":"github"`, `"host":"h"`} {
		if !strings.Contains(line, want) {
			t.Fatalf("the line lacks %s: %s", want, line)
		}
	}
}
