package providersync

import (
	"errors"
	"testing"
)

// The claim SQL coalesces result, source metadata, dataset options, and
// integration config to '{}', so on every fresh unit all four documents are
// byte-identical. Decoding must still populate every target: pairing raw
// text with targets through a map literal collapses duplicate raw values and
// leaves all but one target as a nil map (CHAOS-3950).
func TestDecodeClaimDocumentsPopulatesEveryTargetForIdenticalJSON(t *testing.T) {
	var claim Claim
	document := []byte(`{}`)
	if err := decodeClaimDocuments(&claim, document, document, document, document); err != nil {
		t.Fatal(err)
	}
	if claim.DatasetOptions == nil || claim.Result == nil ||
		claim.SourceMetadata == nil || claim.IntegrationConfig == nil {
		t.Fatalf(
			"identical documents left targets nil: datasetOptions=%v result=%v sourceMetadata=%v integrationConfig=%v",
			claim.DatasetOptions == nil, claim.Result == nil,
			claim.SourceMetadata == nil, claim.IntegrationConfig == nil,
		)
	}
}

func TestDecodeClaimDocumentsKeepsEachDocumentOnItsOwnTarget(t *testing.T) {
	var claim Claim
	err := decodeClaimDocuments(&claim,
		[]byte(`{"which":"dataset_options"}`),
		[]byte(`{"which":"result"}`),
		[]byte(`{"which":"source_metadata"}`),
		[]byte(`{"which":"integration_config"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, got := range map[string]map[string]any{
		"dataset_options":    claim.DatasetOptions,
		"result":             claim.Result,
		"source_metadata":    claim.SourceMetadata,
		"integration_config": claim.IntegrationConfig,
	} {
		if got["which"] != name {
			t.Fatalf("%s decoded from the wrong column: %v", name, got)
		}
	}
}

// The duplicate-key collapse is data-dependent: it fires whenever ANY subset
// of the four columns is byte-identical, not only when all four are. Iterate
// every assignment of four payload labels to the four columns — a superset of
// all 15 set partitions, partial overlaps included — and assert each claim
// field decodes exactly its own column's content.
func TestDecodeClaimDocumentsEveryOverlapPartitionKeepsFieldContent(t *testing.T) {
	documents := [4][]byte{
		[]byte(`{"label":"0"}`), []byte(`{"label":"1"}`),
		[]byte(`{"label":"2"}`), []byte(`{"label":"3"}`),
	}
	for assignment := 0; assignment < 256; assignment++ {
		labels := [4]int{
			assignment & 3, (assignment >> 2) & 3,
			(assignment >> 4) & 3, (assignment >> 6) & 3,
		}
		var claim Claim
		if err := decodeClaimDocuments(&claim,
			documents[labels[0]], documents[labels[1]],
			documents[labels[2]], documents[labels[3]],
		); err != nil {
			t.Fatalf("assignment %v: %v", labels, err)
		}
		for position, decoded := range []map[string]any{
			claim.DatasetOptions, claim.Result,
			claim.SourceMetadata, claim.IntegrationConfig,
		} {
			want := string(rune('0' + labels[position]))
			if decoded == nil || decoded["label"] != want {
				t.Fatalf(
					"assignment %v position %d: decoded=%v want label %q",
					labels, position, decoded, want,
				)
			}
		}
	}
}

func TestDecodeClaimDocumentsStillFailsClosedOnMalformedJSON(t *testing.T) {
	var claim Claim
	document := []byte(`{}`)
	if err := decodeClaimDocuments(
		&claim, document, []byte(`{not json`), document, document,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("malformed document accepted: error=%v", err)
	}
}
