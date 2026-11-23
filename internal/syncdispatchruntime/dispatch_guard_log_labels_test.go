package syncdispatchruntime

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

func TestGuardReasonCodesAreValidLogLabels(t *testing.T) {
	for _, code := range []string{guardReasonCodeRunUnitCap, guardReasonCodeUnitConcurrencyCap} {
		var buf bytes.Buffer
		logger := synclog.New(slog.New(slog.NewTextHandler(&buf, nil)))
		logger.Info(context.Background(), synclog.MsgDispatchSyncRunConcurrencyCapped, synclog.Text(synclog.KeyReason, synclog.ParseLabel(code)))
		if !strings.Contains(buf.String(), "reason="+code) {
			t.Fatalf("code %q logged as %q", code, buf.String())
		}
	}
}

func TestBucketDecisionLogCarriesAReadableBucketLabel(t *testing.T) {
	var buf bytes.Buffer
	logger := synclog.New(slog.New(slog.NewTextHandler(&buf, nil)))
	emitBucketDecision(context.Background(), logger, dispatchBucket{orgID: "123e4567-e89b-12d3-a456-426614174000", provider: "github", costClass: "heavy"}, 1, 0, 0, 0, 0, 0, "", heldBackSignalNone)
	line := buf.String()
	if !strings.Contains(line, "bucket=github:heavy") || strings.Contains(line, "bucket=invalid") {
		t.Fatalf("bucket label not readable: %s", line)
	}
}
