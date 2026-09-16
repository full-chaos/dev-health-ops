package llmorgsettings

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// capturingLogHandler captures slog records whole rather than as rendered
// text -- asserting on formatted output would pin the handler, not the
// record, and would pass just as happily if every attribute collapsed into
// one string (see providersync's membershipLogHandler, same shape).
type capturingLogHandler struct {
	records *[]slog.Record
}

func (h *capturingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingLogHandler) Handle(_ context.Context, record slog.Record) error {
	*h.records = append(*h.records, record.Clone())
	return nil
}

func (h *capturingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *capturingLogHandler) WithGroup(string) slog.Handler { return h }

func captureLogs(t *testing.T) *[]slog.Record {
	t.Helper()
	records := []slog.Record{}
	previous := slog.Default()
	slog.SetDefault(slog.New(&capturingLogHandler{records: &records}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &records
}

func logAttrs(record slog.Record) map[string]any {
	attrs := map[string]any{}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	return attrs
}

func counterValue(t *testing.T, name, orgAttr string) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := realMeterReader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("reader.Collect error = %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s data shape = %+v, want an int64 sum", name, m.Data)
			}
			var total int64
			for _, dp := range data.DataPoints {
				if orgAttr == "" {
					total += dp.Value
					continue
				}
				if got, ok := dp.Attributes.Value("org_id"); ok && got.AsString() == orgAttr {
					total += dp.Value
				}
			}
			return total, true
		}
	}
	return 0, false
}

// TestDefaultRecordDecryptFailure_RecordsToRealMeterAndLog exercises the
// sink defaultRecordDecryptFailure ITSELF (not the injection seam) --
// swapping recordDecryptFailure for a spy proves loadRawSettings CALLS the
// hook, but says nothing about what the hook behind it actually does. Per
// root AGENTS.md's verification rule ("sink-level tests assert on the
// production sink's real output bytes"), the real otel meter and a real
// slog handler are the sinks here.
func TestDefaultRecordDecryptFailure_RecordsToRealMeterAndLog(t *testing.T) {
	logs := captureLogs(t)
	before, _ := counterValue(t, "devhealth_llmorgsettings_decrypt_failure_total", "org-record-test")

	defaultRecordDecryptFailure(context.Background(), "org-record-test")

	after, ok := counterValue(t, "devhealth_llmorgsettings_decrypt_failure_total", "org-record-test")
	if !ok {
		t.Fatal("devhealth_llmorgsettings_decrypt_failure_total was never emitted to the real meter")
	}
	if after != before+1 {
		t.Errorf("counter = %d, want %d", after, before+1)
	}

	if len(*logs) != 1 {
		t.Fatalf("got %d log records, want 1: %+v", len(*logs), *logs)
	}
	record := (*logs)[0]
	if record.Level != slog.LevelError {
		t.Errorf("level = %v, want Error", record.Level)
	}
	attrs := logAttrs(record)
	if attrs["component"] != "llmorgsettings" {
		t.Errorf("component attr = %v, want llmorgsettings", attrs["component"])
	}
	if attrs["org_id"] != "org-record-test" {
		t.Errorf("org_id attr = %v, want org-record-test", attrs["org_id"])
	}
}

// TestDefaultCheckKeyOnFirstDecrypt_GatesOnFirstAttemptNotFirstFailure pins
// the "gated on the first ATTEMPT, not the first failure" rule the
// telemetry.go doc comment documents: once a first SUCCESSFUL attempt has
// been observed, a later failure must not fire the elevated
// key-validation-failed signal (that would misreport a good key as broken
// off one later corrupt row); but if the FIRST attempt itself fails, the
// signal fires exactly once, even across many subsequent failures.
func TestDefaultCheckKeyOnFirstDecrypt_GatesOnFirstAttemptNotFirstFailure(t *testing.T) {
	t.Run("first attempt succeeds -> later failure never flags the key", func(t *testing.T) {
		resetFirstDecryptAttemptForTest()
		logs := captureLogs(t)
		before, _ := counterValue(t, "devhealth_llmorgsettings_key_validation_failed_total", "")

		defaultCheckKeyOnFirstDecrypt(context.Background(), "org-a", nil)
		defaultCheckKeyOnFirstDecrypt(context.Background(), "org-b", errors.New("corrupt row"))

		after, _ := counterValue(t, "devhealth_llmorgsettings_key_validation_failed_total", "")
		if after != before {
			t.Errorf("counter = %d, want unchanged at %d (key already proved good)", after, before)
		}
		if len(*logs) != 0 {
			t.Errorf("got %d log records, want 0: %+v", len(*logs), *logs)
		}
	})

	t.Run("first attempt fails -> flags once, not again on later failures", func(t *testing.T) {
		resetFirstDecryptAttemptForTest()
		logs := captureLogs(t)
		before, ok := counterValue(t, "devhealth_llmorgsettings_key_validation_failed_total", "")
		if !ok {
			before = 0
		}

		defaultCheckKeyOnFirstDecrypt(context.Background(), "org-c", errors.New("bad key"))
		defaultCheckKeyOnFirstDecrypt(context.Background(), "org-d", errors.New("bad key"))

		after, ok := counterValue(t, "devhealth_llmorgsettings_key_validation_failed_total", "")
		if !ok || after != before+1 {
			t.Errorf("counter = %d (present=%v), want %d", after, ok, before+1)
		}
		if len(*logs) != 1 {
			t.Fatalf("got %d log records, want 1: %+v", len(*logs), *logs)
		}
		if (*logs)[0].Level != slog.LevelError {
			t.Errorf("level = %v, want Error", (*logs)[0].Level)
		}
		if got := logAttrs((*logs)[0])["org_id"]; got != "org-c" {
			t.Errorf("org_id attr = %v, want org-c (the FIRST attempt)", got)
		}
	})
}
