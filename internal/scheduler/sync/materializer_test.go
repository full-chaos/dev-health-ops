package sync

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRequestedDatasetKeysPreservesLiveConfigRoutingSemantics(t *testing.T) {
	child := "00000000-0000-4000-8000-000000000001"
	if got := requestedDatasetKeys("jira", []string{"work-items"}, nil); got != nil {
		t.Fatalf("parent dataset scope=%v, want all enabled (nil)", got)
	}
	wantChild := map[string]bool{
		"work-items": true, "work-item-labels": true, "work-item-projects": true,
		"work-item-history": true, "work-item-comments": true,
	}
	if got := requestedDatasetKeys("jira", []string{"work-items"}, &child); !reflect.DeepEqual(got, wantChild) {
		t.Fatalf("recognized child dataset scope=%v, want %v", got, wantChild)
	}
	if got := requestedDatasetKeys("jira", []string{"not-a-provider-target"}, &child); got != nil {
		t.Fatalf("unrecognized child dataset scope=%v, want all enabled (nil)", got)
	}
}

func TestNewNativeMaterializerPortsPythonEnvironmentBounds(t *testing.T) {
	t.Setenv("SYNC_WATERMARK_OVERLAP", "-10")
	t.Setenv("SYNC_RUN_MAX_UNITS", "0")
	materializer, err := NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	if materializer.watermarkOverlap != 0 || materializer.defaultUnitCap != 1 {
		t.Fatalf("bounded settings: overlap=%s cap=%d", materializer.watermarkOverlap, materializer.defaultUnitCap)
	}

	t.Setenv("SYNC_WATERMARK_OVERLAP", "invalid")
	t.Setenv("SYNC_RUN_MAX_UNITS", "invalid")
	materializer, err = NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	if materializer.watermarkOverlap != 0*time.Second || materializer.defaultUnitCap != 1000 {
		t.Fatalf("fallback settings: overlap=%s cap=%d", materializer.watermarkOverlap, materializer.defaultUnitCap)
	}
}

// TestNewNativeMaterializerCapsExecutedProofRefreshIntervalAgainstOverflow is
// the codex-review regression case: boundedEnvInt only lower-bounds
// SYNC_EXECUTED_PROOF_REFRESH_SECONDS, so an operator-supplied (or typo'd)
// huge value multiplied by time.Second would overflow time.Duration's
// int64-nanosecond range and go negative -- making maybeRefreshExecutedProof
// treat every Materialize call as overdue for a refresh, the opposite of
// what a refresh INTERVAL is for. The 24h cap must hold even for a value
// that would otherwise genuinely overflow.
func TestNewNativeMaterializerCapsExecutedProofRefreshIntervalAgainstOverflow(t *testing.T) {
	t.Setenv("SYNC_EXECUTED_PROOF_REFRESH_SECONDS", "9223372037")
	materializer, err := NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	want := 24 * time.Hour
	if materializer.executedProofRefreshInterval != want {
		t.Fatalf(
			"executedProofRefreshInterval=%s, want %s (capped, not overflowed)",
			materializer.executedProofRefreshInterval, want,
		)
	}
	if materializer.executedProofRefreshInterval <= 0 {
		t.Fatalf(
			"executedProofRefreshInterval=%s must be positive: a zero or negative "+
				"interval makes every refresh check read as immediately overdue",
			materializer.executedProofRefreshInterval,
		)
	}

	t.Setenv("SYNC_EXECUTED_PROOF_REFRESH_SECONDS", "")
	materializer, err = NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	if want := 300 * time.Second; materializer.executedProofRefreshInterval != want {
		t.Fatalf("default executedProofRefreshInterval=%s, want %s", materializer.executedProofRefreshInterval, want)
	}
}

// TestNewNativeMaterializerMirrorsPythonIntWhitespaceGrammar pins the parse
// GRAMMAR, not just the bounds. Python reads both settings through int():
// _watermark_overlap_seconds does max(0, int(os.getenv("SYNC_WATERMARK_OVERLAP",
// "0"))) (src/dev_health_ops/sync/watermarks.py:113-122) and _env_int does
// max(1, int(raw)) (src/dev_health_ops/sync/guard.py:296-304). int() STRIPS
// surrounding whitespace before parsing, so every value below is accepted by
// production Python; a bare strconv.Atoi rejects all of them and silently
// takes the fallback instead.
//
// The overlap case is the one with teeth: an operator whose deployment renders
// SYNC_WATERMARK_OVERLAP with padding (a YAML block scalar, a here-doc, a
// trailing newline from a secret file) gets the Python worker's real overlap
// and the Go worker's 0 -- different incremental window coverage from the same
// configuration, and a HEAVY ratchet that skips the C8 overlap clamp entirely.
// See TestPaddedOverlapStillReachesTheHeavyRatchetClamp for that consequence.
//
// Expectations are pinned against the real interpreter, not from memory:
//
//	python3 -c "print(int('  604800  '), int('\t604800\n'), int('+7'), int(' -10 '))"
//	604800 604800 7 -10
func TestNewNativeMaterializerMirrorsPythonIntWhitespaceGrammar(t *testing.T) {
	for _, test := range []struct {
		name        string
		overlapRaw  string
		capRaw      string
		wantOverlap time.Duration
		wantCap     int
	}{
		{
			name: "space padded", overlapRaw: "  604800  ", capRaw: " 250 ",
			wantOverlap: 604800 * time.Second, wantCap: 250,
		},
		{
			name: "tab and newline padded", overlapRaw: "\t604800\n", capRaw: "\t250\n",
			wantOverlap: 604800 * time.Second, wantCap: 250,
		},
		{
			name: "carriage return padded", overlapRaw: "\r\n86400\r\n", capRaw: "\r\n250\r\n",
			wantOverlap: 86400 * time.Second, wantCap: 250,
		},
		{
			name: "explicit plus sign", overlapRaw: "+3600", capRaw: "+250",
			wantOverlap: 3600 * time.Second, wantCap: 250,
		},
		// Whitespace stripping must not defeat the bounds: Python clamps a
		// padded negative to 0 (overlap) and to 1 (unit cap), exactly as it
		// clamps an unpadded one.
		{
			name: "padded negative still clamps to the bound", overlapRaw: " -10 ", capRaw: " -10 ",
			wantOverlap: 0, wantCap: 1,
		},
		// Interior whitespace is NOT stripped by int() and must stay a
		// fallback on both sides.
		{
			name: "interior space is not an integer", overlapRaw: "60 48", capRaw: "2 5",
			wantOverlap: 0, wantCap: 1000,
		},
		{
			name: "whitespace only is not an integer", overlapRaw: "   ", capRaw: "   ",
			wantOverlap: 0, wantCap: 1000,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("SYNC_WATERMARK_OVERLAP", test.overlapRaw)
			t.Setenv("SYNC_RUN_MAX_UNITS", test.capRaw)
			materializer, err := NewNativeMaterializer(&pgxpool.Pool{})
			if err != nil {
				t.Fatal(err)
			}
			if materializer.watermarkOverlap != test.wantOverlap {
				t.Errorf("SYNC_WATERMARK_OVERLAP=%q parsed to %s, want %s (Python int() strips whitespace)",
					test.overlapRaw, materializer.watermarkOverlap, test.wantOverlap)
			}
			if materializer.defaultUnitCap != test.wantCap {
				t.Errorf("SYNC_RUN_MAX_UNITS=%q parsed to %d, want %d (Python int() strips whitespace)",
					test.capRaw, materializer.defaultUnitCap, test.wantCap)
			}
		})
	}
}

// TestPaddedOverlapStillReachesTheHeavyRatchetClamp states the MEDIUM finding
// as the behaviour it breaks rather than as a parse detail. A 7-day overlap
// written with padding must still trigger the CHAOS-3412 clause C8 clamp
// (effective cap = floor(overlap_days)+1 = 8 days) against the 7-day default
// cap. When the parse drops the value to 0 the clamp is skipped silently and
// the window is capped at 7 days: end == W - 0 + 7d relative to a watermark
// read at W - 7d, i.e. exactly the non-advancing window the clamp exists to
// prevent.
func TestPaddedOverlapStillReachesTheHeavyRatchetClamp(t *testing.T) {
	t.Setenv("SYNC_WATERMARK_OVERLAP", " 604800 ")
	// Set explicitly rather than relying on the ambient default so the case is
	// self-describing and cannot be silenced by an inherited env value.
	t.Setenv(incrementalHeavyMaxWindowDaysEnv, strconv.Itoa(defaultIncrementalHeavyMaxWindowDays))
	materializer, err := NewNativeMaterializer(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}
	warnedCapClamps.Clear()
	got := effectiveHeavyMaxWindow(materializer.watermarkOverlap)
	want := 8 * 24 * time.Hour
	if got != want {
		t.Fatalf("effectiveHeavyMaxWindow(parsed %s) = %s, want %s: "+
			"the padded overlap must reach the C8 clamp, not be parsed away to zero",
			materializer.watermarkOverlap, got, want)
	}
	if got <= materializer.watermarkOverlap {
		t.Fatalf("effective cap %s does not strictly exceed overlap %s",
			got, materializer.watermarkOverlap)
	}
}

// TestRejectedEnvIntWarnsWithTheRawValue pins the accepted grammar
// restriction: Go's Atoi rejects two things Python's int() accepts --
// underscore digit separators ("3_0") and non-ASCII decimal digits ("٣٠") --
// and this worker deliberately does not port them. The restriction is only
// defensible if it is LOUD: a silent fallback is how a Python-valid setting
// turns into unexplained window drift on the Go side. The warning must name
// the raw text, or an operator reading the log cannot tell which value was
// ignored.
func TestRejectedEnvIntWarnsWithTheRawValue(t *testing.T) {
	for _, test := range []struct {
		name    string
		setting string
		raw     string
		inForce int
		apply   func(t *testing.T)
	}{
		{
			name: "underscore separator in the overlap", setting: "SYNC_WATERMARK_OVERLAP",
			raw: "604_800", inForce: 0,
			apply: func(t *testing.T) {
				t.Setenv("SYNC_WATERMARK_OVERLAP", "604_800")
				if _, err := NewNativeMaterializer(&pgxpool.Pool{}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "non-ASCII digits in the unit cap", setting: "SYNC_RUN_MAX_UNITS",
			raw: "٣٠", inForce: 1000,
			apply: func(t *testing.T) {
				t.Setenv("SYNC_RUN_MAX_UNITS", "٣٠")
				if _, err := NewNativeMaterializer(&pgxpool.Pool{}); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "underscore separator in the heavy window cap", setting: incrementalHeavyMaxWindowDaysEnv,
			raw: "1_4", inForce: defaultIncrementalHeavyMaxWindowDays,
			apply: func(t *testing.T) {
				t.Setenv(incrementalHeavyMaxWindowDaysEnv, "1_4")
				if got := incrementalHeavyMaxWindowDays(); got != defaultIncrementalHeavyMaxWindowDays {
					t.Fatalf("cap = %d, want the %d-day default", got, defaultIncrementalHeavyMaxWindowDays)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			restore := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
			t.Cleanup(func() { slog.SetDefault(restore) })
			warnedEnvIntRejections.Clear()

			test.apply(t)

			var record struct {
				Msg          string `json:"msg"`
				Level        string `json:"level"`
				Setting      string `json:"setting"`
				RawValue     string `json:"raw_value"`
				ValueInForce int    `json:"value_in_force"`
			}
			line := strings.TrimSpace(logs.String())
			if line == "" {
				t.Fatalf("%s=%q was rejected SILENTLY; the divergence must warn", test.setting, test.raw)
			}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatalf("decode warning %q: %v", line, err)
			}
			if record.Msg != "sync.settings.env_int_rejected" || record.Level != "WARN" {
				t.Errorf("warning = %s/%s, want WARN/sync.settings.env_int_rejected", record.Level, record.Msg)
			}
			if record.Setting != test.setting || record.RawValue != test.raw {
				t.Errorf("warning named setting=%q raw_value=%q, want %q/%q",
					record.Setting, record.RawValue, test.setting, test.raw)
			}
			if record.ValueInForce != test.inForce {
				t.Errorf("warning reported value_in_force=%d, want %d", record.ValueInForce, test.inForce)
			}

			// Deduped, not merely rate-limited by luck: the heavy-window cap
			// resolves once per (source x heavy dataset), so a repeat must add
			// no second line for the same raw value.
			before := logs.Len()
			test.apply(t)
			if logs.Len() != before {
				t.Errorf("repeat rejection of %s=%q emitted another line: %s",
					test.setting, test.raw, logs.String()[before:])
			}
		})
	}
}

func TestDeterministicMaterializationIDsAreStableAndPartitioned(t *testing.T) {
	first, err := deterministicMaterializationIDs("occurrence-v1:abc")
	if err != nil {
		t.Fatal(err)
	}
	second, err := deterministicMaterializationIDs("occurrence-v1:abc")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("replay changed graph identities: first=%+v second=%+v", first, second)
	}
	seen := map[string]bool{}
	for _, value := range []string{
		first.JobRunID,
		first.SyncRunID,
		first.ReferenceDiscoveryID,
		first.DispatchOutboxID,
	} {
		if _, err := uuid.Parse(value); err != nil {
			t.Fatalf("derived invalid UUID %q: %v", value, err)
		}
		if seen[value] {
			t.Fatalf("different graph rows share deterministic ID %q", value)
		}
		seen[value] = true
	}

	other, err := deterministicMaterializationIDs("occurrence-v1:def")
	if err != nil {
		t.Fatal(err)
	}
	if first.SyncRunID == other.SyncRunID {
		t.Fatal("different occurrences share a sync-run identity")
	}
}

func TestDeterministicUnitIDsUseRunAndOrdinal(t *testing.T) {
	ids, err := deterministicMaterializationIDs("occurrence-v1:abc")
	if err != nil {
		t.Fatal(err)
	}
	first, err := deterministicUnitID(ids.SyncRunID, 0)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := deterministicUnitID(ids.SyncRunID, 0)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deterministicUnitID(ids.SyncRunID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first != replay || first == second {
		t.Fatalf("unit identities are not stable and ordinal-partitioned: first=%s replay=%s second=%s", first, replay, second)
	}
}
