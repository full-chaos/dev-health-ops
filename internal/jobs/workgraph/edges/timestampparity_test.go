package edges

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// knownTimestampDivergences are the two `fromisoformat` shapes this port does
// NOT model, recorded rather than left to be rediscovered.
//
// Both are calendar features rather than formatting ones, and modelling them
// means hand-rolling ISO week-date arithmetic and a 24:00 rollover — a wrong
// parser would be worse than a narrow one with a counted fallback. Neither can
// arrive on the production path: ReadDependencies formats the value with
// RFC3339Nano itself, so the only strings reaching the parser are this
// package's own. Tracked as CHAOS-4818.
var knownTimestampDivergences = map[string]string{
	"2026-W36-2":          "ISO week date; needs week-date arithmetic Go's time package does not parse",
	"2026-09-01T24:00:00": "hour 24 rolls into the next day in Python; Go rejects the hour outright",
}

// TestEventTimestampMatchesPython drives the port's real parser with the
// reference's own answers.
//
// The corpus varies separator, extended vs basic format, precision from
// date-only to nine fractional digits, offset shape, and `Z` placement —
// because the frozen 6,531-row golden contains exactly one of these shapes,
// so it could never have found the gap codex round 2 reported.
func TestEventTimestampMatchesPython(t *testing.T) {
	corpus := loadTimestampParity(t)
	buildClock, err := time.Parse(time.RFC3339, corpus.BuildClock)
	if err != nil {
		t.Fatalf("build clock: %v", err)
	}
	divergences := 0

	for _, observation := range corpus.Observations {
		t.Run(observation.Input, func(t *testing.T) {
			got, gotErr := eventTimestamp(observation.Input, buildClock)
			want, err := time.Parse(time.RFC3339Nano, observation.EventTs)
			if err != nil {
				t.Fatalf("decode expected event_ts %q: %v", observation.EventTs, err)
			}

			if reason, known := knownTimestampDivergences[observation.Input]; known {
				divergences++
				// A known divergence must still be SAFE: the port may fall back
				// to the build clock, but it must never silently produce a
				// different real instant, and the fallback must be counted.
				if gotErr == nil {
					t.Fatalf("this input is recorded as an unmodelled divergence (%s) but the "+
						"port parsed it — either the model improved and the record is stale, "+
						"or it is parsing it WRONG", reason)
				}
				if !got.Equal(buildClock) {
					t.Fatalf("unmodelled input produced %v rather than the build clock", got)
				}
				return
			}

			if observation.Outcome == "fallback_to_build_clock" {
				if gotErr == nil {
					t.Fatalf("Python could not parse this and fell back; this port parsed it as %v", got)
				}
				return
			}
			if gotErr != nil {
				t.Fatalf("Python parsed this as %v; this port fell back to the build clock (%v)",
					want, gotErr)
			}
			if !got.Equal(want) {
				t.Fatalf("got %v, Python gave %v", got.UTC(), want.UTC())
			}
		})
	}

	if divergences != len(knownTimestampDivergences) {
		t.Errorf("the corpus exercises %d of %d recorded divergences; a divergence that left "+
			"the corpus is no longer being watched", divergences, len(knownTimestampDivergences))
	}
}

// TestTheGoldenNeverFallsBack. Every one of the 6,531 frozen rows must parse.
// A fallback there would mean this port silently substituted the build clock
// for a real event time on data Python read correctly.
func TestTheGoldenNeverFallsBack(t *testing.T) {
	document := loadGolden(t)
	buildClock, err := parseGoldenInstant(document.FrozenNow)
	if err != nil {
		t.Fatalf("frozen_now: %v", err)
	}
	result, err := DeriveIssueIssueEdges(goldenDependencyRows(t, document), buildClock)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if result.TimestampFallbacks != 0 {
		t.Fatalf("%d of the frozen rows fell back to the build clock; Python parsed all of them",
			result.TimestampFallbacks)
	}
}

type timestampParity struct {
	Schema       string `json:"schema"`
	BuildClock   string `json:"build_clock"`
	Observations []struct {
		Input   string `json:"input"`
		Outcome string `json:"outcome"`
		EventTs string `json:"event_ts"`
	} `json:"observations"`
}

func loadTimestampParity(t *testing.T) timestampParity {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(
		repositoryRootPath(t), "tests", "fixtures", "event_timestamp_parity.json"))
	if err != nil {
		t.Fatalf("read timestamp parity corpus: %v", err)
	}
	var corpus timestampParity
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("decode timestamp parity corpus: %v", err)
	}
	if corpus.Schema != "event_timestamp_parity.v1" {
		t.Fatalf("unexpected corpus schema %q", corpus.Schema)
	}
	if len(corpus.Observations) == 0 {
		t.Fatal("timestamp parity corpus decoded to zero observations")
	}
	return corpus
}
