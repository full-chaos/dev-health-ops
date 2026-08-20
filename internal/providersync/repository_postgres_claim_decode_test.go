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

func TestDecodeClaimDocumentsStillFailsClosedOnMalformedJSON(t *testing.T) {
	var claim Claim
	document := []byte(`{}`)
	if err := decodeClaimDocuments(
		&claim, document, []byte(`{not json`), document, document,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("malformed document accepted: error=%v", err)
	}
}
