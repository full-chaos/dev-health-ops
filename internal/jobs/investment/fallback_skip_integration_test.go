//go:build integration

package investment

import (
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// CHAOS-9270: the fixed insufficient_evidence row of a unit under the evidence
// gate is not written again for the same (input hash, stamp). Both provider
// paths. A changed hash or stamp, or a unit that gains evidence, is processed.
func TestAnInsufficientEvidenceFallbackRowIsNotRewrittenForTheSameHashAndStamp(t *testing.T) {
	const gatedRows = `SELECT count() FROM work_unit_investments WHERE org_id = ? AND categorization_status = 'insufficient_evidence'`
	for _, path := range []string{"generative", "served"} {
		t.Run(path, func(t *testing.T) {
			h := newShadowHarness(t)
			logs := &syncBuffer{}
			fake := newFakeJev(t, func(int, []byte) jevReply { return okReply() })
			hour := 0
			run := func(name string) Stats {
				t.Helper()
				hour++
				cfg := h.config(name, h.within.Add(time.Duration(hour)*time.Hour))
				cfg.Force = false
				var m *Materializer
				if path == "served" {
					m = h.servedMaterializer(t, fake, logs)
				} else {
					m = h.plainMaterializer(t, logs)
				}
				stats, err := m.Run(h.ctx, cfg)
				if err != nil {
					t.Fatalf("%s: %v\n%s", name, err, logs.String())
				}
				return stats
			}
			wantRows := func(name string, want uint64) {
				t.Helper()
				if got := h.count(t, gatedRows, hierarchyCascadeTestOrg); got != want {
					t.Fatalf("%s: %d insufficient_evidence rows, want %d", name, got, want)
				}
			}

			first := run("first")
			wantRows("first", 1)
			second := run("second")
			wantRows("second (same hash, same stamp)", 1)
			if first.SkippedExisting != 0 || second.SkippedExisting != shadowServedUnits {
				t.Errorf("skipped: first=%d second=%d, want 0 and %d (the gated unit included)", first.SkippedExisting, second.SkippedExisting, shadowServedUnits)
			}
			if !strings.Contains(logs.String(), "skipped_fallback=1") {
				t.Errorf("the skipped fallback is not in the run log line:\n%s", lastLines(logs.String(), 6))
			}

			// Stamp change: the unit's latest row carries another stamp.
			other := "provider=typesafe;api=systemone;model=jev-other;prompt=other@000000000000"
			if err := h.conn.Exec(h.ctx, `INSERT INTO work_unit_investments SELECT * REPLACE (? AS categorization_model_version, computed_at + toIntervalMinute(1) AS computed_at)
				FROM work_unit_investments FINAL WHERE org_id = ? AND categorization_status = 'insufficient_evidence'`, other, hierarchyCascadeTestOrg); err != nil {
				t.Fatal(err)
			}
			run("stamp changed")
			wantRows("stamp changed", 3)
			run("steady after the stamp change")
			wantRows("steady after the stamp change", 3)

			// Hash change: the unit's title changes but stays under the gate.
			if err := h.conn.Exec(h.ctx, `ALTER TABLE work_items UPDATE title = 'D1 renamed' WHERE work_item_id = 'D1' SETTINGS mutations_sync = 2`); err != nil {
				t.Fatal(err)
			}
			run("hash changed")
			wantRows("hash changed", 4)
			run("steady after the hash change")
			wantRows("steady after the hash change", 4)

			// The unit gains enough evidence: it leaves the fallback.
			text := strings.Repeat("Rework the retry path of the importer after the outage review "+shadowSourceSentinel+". ", 8)
			if err := h.conn.Exec(h.ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id IN ('D1','D2') SETTINGS mutations_sync = 2`, text); err != nil {
				t.Fatal(err)
			}
			run("gains evidence")
			var status string
			if err := h.conn.QueryRow(h.ctx, `SELECT argMax(categorization_status, computed_at) FROM work_unit_investments WHERE org_id = ? AND position(structural_evidence_json, '"D1"') > 0`, hierarchyCascadeTestOrg).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if status == categorize.StatusInsufficientChars {
				t.Errorf("a unit with enough evidence kept the fallback status")
			}
		})
	}
}

func (h *shadowHarness) plainMaterializer(t *testing.T, logs *syncBuffer) *Materializer {
	t.Helper()
	return h.materializer(t, nil, logs)
}
