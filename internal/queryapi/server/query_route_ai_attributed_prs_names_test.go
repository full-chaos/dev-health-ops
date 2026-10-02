package server

import (
	"os"
	"strings"
	"testing"
)

// CHAOS-7982: the web PR Evidence list asks aiAttributedPrs for `repoName` (the field the Go API
// added with CHAOS-7773). query-api matches a request to its operation by the digest of the EXACT
// text, so the registered document and the captured wire fixture must carry that selection, and the
// wire-parity pair (web branch of the same name) must agree.
func TestRegisteredAiAttributedPrsDocument_RequestsRepoNameInRows(t *testing.T) {
	raw, err := os.ReadFile("testdata/wire_capture/aiattributedprs_captured.graphql")
	if err != nil {
		t.Fatal(err)
	}
	for name, doc := range map[string]string{
		"registered const":      registeredAiAttributedPrsDocument,
		"captured wire fixture": string(raw),
	} {
		rows := doc[strings.Index(doc, "rows {"):]
		rows = rows[:strings.Index(rows, "}")]
		if !strings.Contains(rows, "repoName") {
			t.Errorf("%s: the rows selection does not request repoName:\n%s", name, rows)
		}
		if strings.Contains(rows, "teamName") {
			t.Errorf("%s: teamName is not requested by the web list; it must not be added silently", name)
		}
	}
}

// CHAOS-8000 dual accept: the OLD text (no repoName) is the operation's legacy text, so a web build still on it keeps
// resolving to aiAttributedPrs while the new one rolls out.
func TestAiAttributedPrs_AcceptsTheOldAndTheNewText(t *testing.T) {
	byDigest, err := buildOperationByDigest(
		map[string]string{"aiAttributedPrs": digestHex(registeredAiAttributedPrsDocument)},
		legacyDigestsByOperation,
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{
		"new text": registeredAiAttributedPrsDocument,
		"old text": registeredAiAttributedPrsV1Document,
	} {
		if op, ok := operationForDocument(text, byDigest); !ok || op != "aiAttributedPrs" {
			t.Errorf("%s resolves to %q, %v; want aiAttributedPrs, true", name, op, ok)
		}
	}
	rows := func(doc string) string {
		s := doc[strings.Index(doc, "rows {"):]
		return s[:strings.Index(s, "}")]
	}
	if strings.Contains(rows(registeredAiAttributedPrsV1Document), "repoName") {
		t.Error("the legacy text requests repoName; it must be the text from before")
	}
	if !strings.Contains(rows(registeredAiAttributedPrsDocument), "repoName") {
		t.Error("the current text does not request repoName")
	}
	if got := legacyDigestsByOperation["aiAttributedPrs"]; len(got) != 1 || got[0] != digestHex(registeredAiAttributedPrsV1Document) {
		t.Errorf("legacyDigestsByOperation[aiAttributedPrs] = %v, want the digest of the V1 text", got)
	}
}
