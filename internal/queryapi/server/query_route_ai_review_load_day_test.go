package server

import (
	"strings"
	"testing"
)

// CHAOS-14638: the registered aiReviewLoad text selects daily.day; the text from before stays accepted.
const (
	aiReviewLoadDigest   = "3c0b56d86d8d3fe263849885ef5a652aba7948e2fc5fb1bba22b6f9bc95eca41"
	aiReviewLoadV1Digest = "4a160089d82bb30f486e81dcc4fb0ef20b97efac29166f0d04e8b03cf06a1697"
)

func TestAiReviewLoad_RegisteredTextsArePinnedByDigest(t *testing.T) {
	if got := digestHex(registeredAiReviewLoadDocument); got != aiReviewLoadDigest {
		t.Errorf("digest of the registered aiReviewLoad text = %s, want %s", got, aiReviewLoadDigest)
	}
	if got := digestHex(registeredAiReviewLoadV1Document); got != aiReviewLoadV1Digest {
		t.Errorf("digest of the legacy aiReviewLoad text = %s, want %s", got, aiReviewLoadV1Digest)
	}
	if got := legacyDigestsByOperation["aiReviewLoad"]; len(got) != 1 || got[0] != aiReviewLoadV1Digest {
		t.Errorf("legacyDigestsByOperation[aiReviewLoad] = %v, want [%s]", got, aiReviewLoadV1Digest)
	}
}

func TestAiReviewLoad_AcceptsTheOldAndTheNewText(t *testing.T) {
	byDigest, err := buildOperationByDigest(
		map[string]string{"aiReviewLoad": digestHex(registeredAiReviewLoadDocument)},
		map[string][]string{"aiReviewLoad": legacyDigestsByOperation["aiReviewLoad"]},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"new text": registeredAiReviewLoadDocument, "old text": registeredAiReviewLoadV1Document} {
		if op, ok := operationForDocument(text, byDigest); !ok || op != "aiReviewLoad" {
			t.Errorf("%s resolves to %q, %v; want aiReviewLoad, true", name, op, ok)
		}
	}
	block := func(doc, name string) string {
		s := doc[strings.Index(doc, "    "+name+" {"):]
		return s[:strings.Index(s, "}")]
	}
	if strings.Contains(registeredAiReviewLoadV1Document, "      day\n") {
		t.Error("the legacy text requests day; it must be the text from before")
	}
	if !strings.Contains(block(registeredAiReviewLoadDocument, "daily"), "\n      day\n") {
		t.Error("the current text does not request daily.day")
	}
	if strings.Contains(block(registeredAiReviewLoadDocument, "byBucket"), "day") {
		t.Error("the current text requests byBucket.day; only daily carries a day")
	}
}
