//go:build integration

package local

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

type corpusRow struct {
	Name   string `json:"name"`
	Status int    `json:"status"`
	Body   string `json:"body"`
}

// corpusAuthFlowRows reads the frozen Python answers of the A3 auth-flow
// corpus, by row name.
func corpusAuthFlowRows(t *testing.T) map[string]corpusRow {
	t.Helper()
	_, file, _, _ := moduleroot.Caller(0)
	path := filepath.Join(filepath.Dir(file), "..", "..", "..", "apiservice", "authflowvenue", "testdata", "golden", "auth-flow-venue-oracle.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Requests []corpusRow `json:"requests"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	rows := map[string]corpusRow{}
	for _, r := range doc.Requests {
		rows[r.Name] = r
	}
	return rows
}

func readAll(resp *http.Response) string {
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}
