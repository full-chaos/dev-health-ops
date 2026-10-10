//go:build integration

package investment

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// CHAOS-9147: a deterministic served outcome (zero_support, evidence_none) is
// terminal for its (input hash, stamp). The unit is asked again only when its
// evidence, the stamp (rubric, model) changed, or when its last request failed.
func TestAServedLowQualityOutcomeIsTerminalUntilEvidenceStampOrModelChange(t *testing.T) {
	h := newShadowHarness(t)
	logs := &syncBuffer{}
	failB := false
	fake := newFakeJev(t, func(_ int, body []byte) jevReply {
		switch servedUnitOf(body) {
		case "B":
			if failB {
				return jevReply{status: http.StatusUnprocessableEntity, body: []byte(`{"error":"bad"}`)}
			}
			return jevReply{body: withLevelMaps(t, jevAnswerBody(body, jevReply{supported: map[string]int{}, inputTokens: 2000}), map[string][]float64{
				"risk.security": {0.60, 0.40, 0, 0}, "quality.testing": {0.55, 0.45, 0, 0},
			})}
		case "C":
			reply := okReply()
			reply.evidenceNone = true
			return reply
		default:
			return okReply()
		}
	})
	hour := 0
	run := func(name string) (requests, skipped int) {
		t.Helper()
		hour++
		cfg := h.config(name, h.within.Add(time.Duration(hour)*time.Hour))
		cfg.Force = false
		before := fake.count()
		stats, err := h.servedMaterializer(t, fake, logs).Run(h.ctx, cfg)
		if err != nil {
			t.Fatalf("%s: %v\n%s", name, err, logs.String())
		}
		return fake.count() - before, stats.SkippedExisting
	}
	expect := func(name string, wantRequests, wantSkipped int) {
		t.Helper()
		if requests, skipped := run(name); requests != wantRequests || skipped != wantSkipped {
			t.Fatalf("%s: requests=%d skipped=%d, want requests=%d skipped=%d", name, requests, skipped, wantRequests, wantSkipped)
		}
	}

	expect("first", shadowGatePassUnits, 0)
	expect("steady", 0, shadowGatePassUnits)
	expect("steady again", 0, shadowGatePassUnits)
	if text := logs.String(); !strings.Contains(text, "units_skipped_terminal=2") || !strings.Contains(text, "skipped_terminal=2") {
		t.Errorf("a skipped terminal unit is not visible in the log lines:\n%s", lastLines(text, 8))
	}

	// Evidence change of B: only B is asked.
	if err := h.conn.Exec(h.ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id = 'B1' SETTINGS mutations_sync = 2`,
		strings.Repeat("A different, long enough account of the same retry path after review "+shadowSourceSentinel+". ", 8)); err != nil {
		t.Fatal(err)
	}
	expect("evidence changed", 1, shadowGatePassUnits-1)
	expect("steady after the evidence change", 0, shadowGatePassUnits)

	// Stamp (rubric or model) change of C: its latest row carries another stamp.
	other := "provider=typesafe;api=systemone;model=jev-other;prompt=other@000000000000"
	if err := h.conn.Exec(h.ctx, `INSERT INTO work_unit_investments SELECT * REPLACE (? AS categorization_model_version, computed_at + toIntervalMinute(1) AS computed_at)
		FROM work_unit_investments FINAL WHERE org_id = ? AND position(structural_evidence_json, '"C1"') > 0`, other, hierarchyCascadeTestOrg); err != nil {
		t.Fatal(err)
	}
	expect("stamp changed", 1, shadowGatePassUnits-1)
	expect("steady after the stamp change", 0, shadowGatePassUnits)

	// A generative or refused invalid_llm_output row has no served terminal
	// code: it is asked again.
	if err := h.conn.Exec(h.ctx, `INSERT INTO work_unit_investments SELECT * REPLACE ('invalid_llm_output' AS categorization_status,
		'["invalid_llm_output", "decision_invalid_answer"]' AS categorization_errors_json, computed_at + toIntervalMinute(2) AS computed_at)
		FROM work_unit_investments FINAL WHERE org_id = ? AND position(structural_evidence_json, '"A1"') > 0`, hierarchyCascadeTestOrg); err != nil {
		t.Fatal(err)
	}
	expect("not terminal", 1, shadowGatePassUnits-1)

	// A unit whose last request failed has no new row and is asked again.
	if err := h.conn.Exec(h.ctx, `ALTER TABLE work_items UPDATE description = ? WHERE work_item_id = 'B1' SETTINGS mutations_sync = 2`,
		strings.Repeat("Yet another long enough account of the retry path "+shadowSourceSentinel+". ", 8)); err != nil {
		t.Fatal(err)
	}
	failB = true
	expect("transport failure", 1, shadowGatePassUnits-1)
	expect("transport failure again", 1, shadowGatePassUnits-1)
	failB = false
	expect("recovered", 1, shadowGatePassUnits-1)
	expect("steady after recovery", 0, shadowGatePassUnits)
}
