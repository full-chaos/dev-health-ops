package syncreconciler

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

// The re-arm log line prints the source dedupe key read from a queued row; a
// credential-shaped key is omitted.
func TestRearmLogAttributesOmitACredentialShapedSourceKey(t *testing.T) {
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	sentinel := probes[0]
	record := rearmRecord{
		unitID: "00000000-0000-4000-8000-000000000003", runID: "00000000-0000-4000-8000-000000000004",
		sourceOutboxID: "00000000-0000-4000-8000-000000000010", sourceDedupeKey: sentinel,
		sourceRiverJobID: 9, dedupeKey: sentinel, evidence: "delivery_missing",
	}
	attrs := attributeMap(rearmLogAttributes(record))
	text := fmt.Sprint(attrs)
	if strings.Contains(text, sentinel) || strings.Contains(text, sentinel[len(sentinel)/2:]) {
		t.Fatalf("attributes carry the key: %s", text)
	}
	if attrs["arguments_refused"] != true || attrs["source_outbox_id"] != record.sourceOutboxID {
		t.Fatalf("attributes lost the identity or the refusal flag: %s", text)
	}
	benign := rearmRecord{unitID: record.unitID, runID: record.runID, sourceOutboxID: record.sourceOutboxID,
		sourceDedupeKey: "sync.provider_unit:" + record.unitID, dedupeKey: "sync.provider_unit:reclaim:" + record.unitID}
	got := attributeMap(rearmLogAttributes(benign))
	if got["source_dedupe_key"] != benign.sourceDedupeKey || got["dedupe_key"] != benign.dedupeKey || got["arguments_refused"] != false {
		t.Fatalf("a benign key was changed: %v", got)
	}
}

func attributeMap(attrs []any) map[string]any {
	out := map[string]any{}
	for index := 0; index+1 < len(attrs); index += 2 {
		out[attrs[index].(string)] = attrs[index+1]
	}
	return out
}
