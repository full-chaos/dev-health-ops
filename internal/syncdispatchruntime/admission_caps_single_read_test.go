package syncdispatchruntime

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
)

// The start line is ONE read of SYNC_UNIT_CONCURRENCY_PER_BUCKET: its clamp, its three caps and their sources must agree even when the
// variable changes while the line is built (r1 of the change that added the sources: the value and the source were separate reads, so
// a toggling variable gave a cap derived with a clamp of 1 next to a reported clamp of 8). The invariant, as the line states it: every
// class's cap is the class's budget limit when that is <= the clamp (source "table"), else the clamp (source "clamp"), for the clamp
// printed in the SAME line.
func TestTheAdmissionStartLineIsOneReadOfTheClampVariable(t *testing.T) {
	const key = "SYNC_UNIT_CONCURRENCY_PER_BUCKET"
	previous, had := os.LookupEnv(key)
	t.Cleanup(func() {
		if had {
			os.Setenv(key, previous)
		} else {
			os.Unsetenv(key)
		}
	})
	os.Setenv(key, "1")
	stop := make(chan struct{})
	var toggler sync.WaitGroup
	toggler.Add(1)
	go func() {
		defer toggler.Done()
		for flip := 0; ; flip++ {
			select {
			case <-stop:
				return
			default:
			}
			if flip%2 == 0 {
				os.Setenv(key, "8")
			} else {
				os.Setenv(key, "1")
			}
		}
	}()
	defer func() { close(stop); toggler.Wait() }()

	limits := map[string]float64{"light": 4, "medium": 2, "heavy": 1}
	for iteration := 0; iteration < 3000; iteration++ {
		var logs bytes.Buffer
		logAdmissionCaps(synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))))
		var line map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(logs.String())), &line); err != nil {
			t.Fatalf("iteration %d: %v: %q", iteration, err, logs.String())
		}
		clamp := line["admission_clamp"].(float64)
		for class, limit := range limits {
			wantCap, wantSource := clamp, "clamp"
			if limit <= clamp {
				wantCap, wantSource = limit, "table"
			}
			if line["admission_cap_"+class] != wantCap || line["admission_cap_"+class+"_source"] != wantSource {
				t.Fatalf("iteration %d: class %s cap=%v source=%v, but the clamp in the same line is %v (want cap %v source %q): %s",
					iteration, class, line["admission_cap_"+class], line["admission_cap_"+class+"_source"], clamp, wantCap, wantSource, logs.String())
			}
		}
	}
}

// The clamp source is part of the same read: with the variable toggling between "1" and unset, a clamp of 1 can only come from the
// environment, and a source of "default" can only mean the built-in default of 8.
func TestTheAdmissionStartLineClampSourceIsFromTheSameReadAsTheClamp(t *testing.T) {
	const key = "SYNC_UNIT_CONCURRENCY_PER_BUCKET"
	previous, had := os.LookupEnv(key)
	t.Cleanup(func() {
		if had {
			os.Setenv(key, previous)
		} else {
			os.Unsetenv(key)
		}
	})
	os.Setenv(key, "1")
	stop := make(chan struct{})
	var toggler sync.WaitGroup
	toggler.Add(1)
	go func() {
		defer toggler.Done()
		for flip := 0; ; flip++ {
			select {
			case <-stop:
				return
			default:
			}
			if flip%2 == 0 {
				os.Unsetenv(key)
			} else {
				os.Setenv(key, "1")
			}
		}
	}()
	defer func() { close(stop); toggler.Wait() }()
	for iteration := 0; iteration < 3000; iteration++ {
		var logs bytes.Buffer
		logAdmissionCaps(synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))))
		var line map[string]any
		if err := json.Unmarshal([]byte(strings.TrimSpace(logs.String())), &line); err != nil {
			t.Fatalf("iteration %d: %v", iteration, err)
		}
		clamp, source := line["admission_clamp"].(float64), line["admission_clamp_source"]
		if (clamp == 1 && source != "env") || (source == "default" && clamp != 8) {
			t.Fatalf("iteration %d: clamp %v with source %v are not one read: %s", iteration, clamp, source, logs.String())
		}
	}
}
