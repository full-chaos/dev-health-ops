package syncdispatchruntime

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-8201: the start line says where the clamp and each cap come from, so a reader can rebuild the decision from the
// line alone. Every source value is pinned for every class and for the clamp's three states (env, unset, invalid).
func TestAdmissionCapSourcesPerClassAndClampState(t *testing.T) {
	for _, tc := range []struct {
		name, env            string
		set                  bool
		clampSource          string
		light, medium, heavy string
	}{
		{"unset: default 8, no class is clamped", "", false, "default", "table", "table", "table"},
		{"env 8 above every limit", "8", true, "env", "table", "table", "table"},
		{"env 3: only light (limit 4) is clamped", "3", true, "env", "clamp", "table", "table"},
		{"env 2: light clamped, medium equal to its limit is table", "2", true, "env", "clamp", "table", "table"},
		{"env 1: light and medium clamped, heavy equal to its limit is table", "1", true, "env", "clamp", "clamp", "table"},
		{"env not an integer falls back to the default", "abc", true, "default", "table", "table", "table"},
		{"env below 1 falls back to the default", "0", true, "default", "table", "table", "table"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", tc.env)
			} else {
				t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "")
			}
			clampSource, sources := EffectiveAdmissionCapSources()
			if clampSource != tc.clampSource || sources["light"] != tc.light ||
				sources["medium"] != tc.medium || sources["heavy"] != tc.heavy {
				t.Fatalf("sources = clamp %q light %q medium %q heavy %q; want clamp %q light %q medium %q heavy %q",
					clampSource, sources["light"], sources["medium"], sources["heavy"],
					tc.clampSource, tc.light, tc.medium, tc.heavy)
			}
			// The source must agree with the enforced number: a "table" cap equals the class's budget limit, a "clamp"
			// cap equals the clamp.
			clamp, caps := EffectiveAdmissionCaps()
			for class, source := range sources {
				limit := map[string]int{"light": 4, "medium": 2, "heavy": 1}[class]
				want := clamp
				if source == "table" {
					want = limit
				}
				if caps[class] != want {
					t.Fatalf("class %s: cap %d with source %q; want %d", class, caps[class], source, want)
				}
			}
		})
	}
}

// A class outside the table keeps the clamp, so its source is the clamp.
func TestAdmissionCapSourceOfAClassOutsideTheTableIsTheClamp(t *testing.T) {
	t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", "5")
	capValue, source := concurrencyCapAndSourceForCostClass("not-a-class")
	if capValue != 5 || source != "clamp" {
		t.Fatalf("cap = %d source = %q; want 5 and clamp", capValue, source)
	}
}

// The service-construction line carries the four source attributes with the values above, and nothing tenant-shaped.
func TestDispatchServiceConstructionLogsTheAdmissionCapSources(t *testing.T) {
	for _, tc := range []struct {
		env  string
		want map[string]string
	}{
		{"3", map[string]string{"admission_clamp_source": "env", "admission_cap_light_source": "clamp", "admission_cap_medium_source": "table", "admission_cap_heavy_source": "table"}},
		{"", map[string]string{"admission_clamp_source": "default", "admission_cap_light_source": "table", "admission_cap_medium_source": "table", "admission_cap_heavy_source": "table"}},
	} {
		t.Setenv("SYNC_UNIT_CONCURRENCY_PER_BUCKET", tc.env)
		var logs bytes.Buffer
		if _, err := NewNativeDispatchSyncRunService(
			&pgxpool.Pool{}, synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))),
			noopBudgetEstimator{}, &joboutbox.Producer{}, noopPolicyRegistry{},
		); err != nil {
			t.Fatal(err)
		}
		var found map[string]any
		for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var record map[string]any
			if json.Unmarshal([]byte(line), &record) == nil && record["msg"] == "sync_dispatch_admission_caps" {
				found = record
			}
		}
		if found == nil {
			t.Fatalf("env %q: no sync_dispatch_admission_caps line in:\n%s", tc.env, logs.String())
		}
		for key, value := range tc.want {
			if found[key] != value {
				t.Fatalf("env %q: %s = %v; want %q in %v", tc.env, key, found[key], value, found)
			}
		}
	}
}
