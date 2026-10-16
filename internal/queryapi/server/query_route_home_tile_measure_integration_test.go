//go:build integration

package server

import (
	"context"
	"crypto/ed25519"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

type statementRecordingHomeCH struct {
	mu         sync.Mutex
	statements []string
}

func (c *statementRecordingHomeCH) Query(ctx context.Context, query string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	c.mu.Lock()
	c.statements = append(c.statements, query)
	c.mu.Unlock()
	return emptyHomeCHClient{}.Query(ctx, query, bindings)
}

func (c *statementRecordingHomeCH) readsIssueSize() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, statement := range c.statements {
		if strings.Contains(statement, "unit_size_weight") {
			return true
		}
	}
	return false
}

// CHAOS-9624 (D6027): the Home investment tile is read in ISSUE_SIZE only for a document that selects the tile's
// measure and its whole sizing coverage; every older accepted Home text, which cannot label an issue-size share,
// is still read in COUNT.
func TestHomeInvestmentTileIsIssueSizeOnlyForADocumentThatAsksForItsCoverage(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	token := signTestEnvelope(t, priv, "org-1")

	for name, tc := range map[string]struct {
		document  string
		issueSize bool
	}{
		"current text selects measure and coverage": {registeredHomeDocument, true},
		"V12 text selects neither":                  {registeredHomeV12Document, false},
		"V11 text selects neither":                  {registeredHomeV11Document, false},
	} {
		t.Run(name, func(t *testing.T) {
			ch := &statementRecordingHomeCH{}
			handler, _, _, _, _ := newQueryHandler(ch, pool, verifier, itTestSchemaDigest, os.Getenv)
			rec := postGraphQLWithVariables(t, handler, tc.document, token, homeVariables())
			if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"errors"`) {
				t.Fatalf("got %d %s", rec.Code, rec.Body.String())
			}
			if got := ch.readsIssueSize(); got != tc.issueSize {
				t.Fatalf("tile read in issue size = %t, want %t", got, tc.issueSize)
			}
		})
	}
}
