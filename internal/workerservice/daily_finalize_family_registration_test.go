package workerservice

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

func TestRegisterNativeFinalizeFamiliesLogsTheRefusedName(t *testing.T) {
	handler, err := daily.NewFinalizeHandler(fakeDailyStoreForRegistrationTest{})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	registerNativeFinalizeFamilies(logger, handler, map[string]daily.NativeFinalizeFamilyExecutor{
		"drifted_family_name": nil,
	})

	out := buf.String()
	for _, want := range []string{"level=ERROR", "native finalize families not registered", "drifted_family_name"} {
		if !strings.Contains(out, want) {
			t.Fatalf("log %q lacks %q", out, want)
		}
	}
}

func TestRegisterNativeFinalizeFamiliesValidMapLogsNothing(t *testing.T) {
	handler, err := daily.NewFinalizeHandler(fakeDailyStoreForRegistrationTest{})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	registerNativeFinalizeFamilies(logger, handler, nil)

	if buf.Len() != 0 {
		t.Fatalf("unexpected log output: %q", buf.String())
	}
}
