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
