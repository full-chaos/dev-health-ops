//go:build integration

package restorefence

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

func TestReApplyLogsNoErrorLine(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := fencePair(t, ctx)
	manifest := exportEnd(t, ctx, end)
	if _, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(logging.NewJSON(&buf, slog.LevelDebug))
	again, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true})
	slog.SetDefault(previous)
	if err != nil || !again.AlreadyApplied {
		t.Fatalf("second run = %+v, %v, want already applied", again, err)
	}
	if strings.Contains(buf.String(), `"level":"ERROR"`) {
		t.Errorf("a re-apply logged at ERROR: %s", buf.String())
	}
}
