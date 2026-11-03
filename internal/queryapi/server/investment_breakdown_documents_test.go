package server

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vektah/gqlparser/v2"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// The web InvestmentBreakdown document asks for uncategorizedCount on evidenceQualityStats (the work units the theme
// mix leaves out for too little text). The current text is that document; the text registered before stays accepted as
// a legacy text, so a web build or a client still holding its digest keeps working while the new web rolls out.

const (
	investmentBreakdownV1DigestPrefix = "964b2e867a1e"
	// investmentBreakdownV2DigestPrefix is the wire digest `pnpm graphql:wire-parity:check` prints for the web document.
	investmentBreakdownV2DigestPrefix = "91d44e116c48"
)

func TestInvestmentBreakdownDocuments_DigestsArePinnedToTheWebWireForms(t *testing.T) {
	if got := digestHex(registeredInvestmentBreakdownDocument); !strings.HasPrefix(got, investmentBreakdownV2DigestPrefix) {
		t.Errorf("the current investmentBreakdown document digests to %s, want the web wire digest %s...", got, investmentBreakdownV2DigestPrefix)
	}
	if got := digestHex(registeredInvestmentBreakdownV1Document); !strings.HasPrefix(got, investmentBreakdownV1DigestPrefix) {
		t.Errorf("the legacy investmentBreakdown document digests to %s, want %s...", got, investmentBreakdownV1DigestPrefix)
	}
}

func TestInvestmentBreakdownCurrentIsTheLegacyTextPlusUncategorizedCount(t *testing.T) {
	if !strings.Contains(registeredInvestmentBreakdownDocument, "uncategorizedCount") {
		t.Error("the current investmentBreakdown document does not select uncategorizedCount")
	}
	if strings.Contains(registeredInvestmentBreakdownV1Document, "uncategorizedCount") {
		t.Error("the legacy investmentBreakdown document selects uncategorizedCount: it is not the old text")
	}
	if got := strings.Replace(registeredInvestmentBreakdownDocument, "      uncategorizedCount\n", "", 1); got != registeredInvestmentBreakdownV1Document {
		t.Errorf("the current document is not the legacy text plus the uncategorizedCount selection:\n%s", got)
	}
	if digestHex(registeredInvestmentBreakdownDocument) == digestHex(registeredInvestmentBreakdownV1Document) {
		t.Fatal("the current and the legacy text have the same digest: nothing would be dual-accepted")
	}
}

func TestInvestmentBreakdown_BothTextsResolveToTheOneOperation(t *testing.T) {
	legacy := legacyDigestsByOperation["investmentBreakdown"]
	if len(legacy) != 1 || legacy[0] != digestHex(registeredInvestmentBreakdownV1Document) {
		t.Fatalf("legacyDigestsByOperation[investmentBreakdown] = %v, want exactly the V1 digest", legacy)
	}
	byDigest, err := buildOperationByDigest(
		map[string]string{
			"investmentBreakdown": digestHex(registeredInvestmentBreakdownDocument),
			"investmentFull":      digestHex(registeredInvestmentFullDocument),
		},
		map[string][]string{"investmentBreakdown": legacy},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, text := range map[string]string{"current": registeredInvestmentBreakdownDocument, "legacy": registeredInvestmentBreakdownV1Document} {
		operation, ok := operationForDocument(text, byDigest)
		if !ok || operation != "investmentBreakdown" {
			t.Errorf("the %s text resolves to %q, %v; want investmentBreakdown, true", name, operation, ok)
		}
	}
}

// CHAOS-12698: acr serves a team-scoped and a repository-scoped investment breakdown as separate policy entries, and
// maps each operation it serves to one row of this registry by name. So the investmentBreakdown body is registered
// twice more, under its own operation name each: the operation key, the GraphQL operation name and the text.
var scopedInvestmentBreakdownDocuments = []struct{ operation, name, text string }{
	{"investmentBreakdownTeam", "InvestmentBreakdownTeam", registeredInvestmentBreakdownTeamDocument},
	{"investmentBreakdownRepository", "InvestmentBreakdownRepository", registeredInvestmentBreakdownRepositoryDocument},
}

// Each scoped text is the CURRENT investmentBreakdown text with only the operation name changed, so the three bodies
// cannot drift; the name is the GraphQL operation name the schema parses; and every text has a digest of its own.
func TestInvestmentBreakdownScopedDocuments_AreTheBreakdownTextUnderTheirOwnName(t *testing.T) {
	const breakdownHead = "query InvestmentBreakdown("
	if !strings.HasPrefix(registeredInvestmentBreakdownDocument, breakdownHead) {
		t.Fatalf("the investmentBreakdown document does not start with %q: this test compares nothing", breakdownHead)
	}
	body := strings.TrimPrefix(registeredInvestmentBreakdownDocument, breakdownHead)
	schema := graph.NewExecutableSchema(graph.Config{}).Schema()
	byDigest := map[string]string{
		digestHex(registeredInvestmentBreakdownDocument):   "investmentBreakdown",
		digestHex(registeredInvestmentBreakdownV1Document): "investmentBreakdown (legacy)",
	}
	for _, c := range scopedInvestmentBreakdownDocuments {
		head := "query " + c.name + "("
		if !strings.HasPrefix(c.text, head) {
			t.Errorf("%s: the text does not start with %q", c.operation, head)
			continue
		}
		if got := strings.TrimPrefix(c.text, head); got != body {
			t.Errorf("%s: the body after the operation name differs from the investmentBreakdown body:\n%s", c.operation, got)
		}
		doc, errs := gqlparser.LoadQuery(schema, c.text)
		if len(errs) > 0 || len(doc.Operations) != 1 {
			t.Errorf("%s: the text does not load against the schema: %v", c.operation, errs)
			continue
		}
		if got := doc.Operations[0].Name; got != c.name {
			t.Errorf("%s: GraphQL operation name = %q, want %q", c.operation, got, c.name)
		}
		digest := digestHex(c.text)
		if other, dup := byDigest[digest]; dup {
			t.Errorf("%s: digest %s is also the digest of %s: the registry could not tell them apart", c.operation, digest, other)
		}
		byDigest[digest] = c.operation
	}
}

// The running registry (GET /registry, built from the route's own digestByOperation) serves each scoped name with the
// digest of its own text, and the route's digest index resolves each text to its own name. Neither name has a legacy
// text.
func TestInvestmentBreakdownScopedDocuments_RegistryServesEachNameWithItsOwnDigest(t *testing.T) {
	pgPool, err := pgxpool.New(context.Background(), "postgres://nobody:nobody@127.0.0.1:1/nonexistent")
	if err != nil {
		t.Fatalf("pgxpool.New: unexpected error (pgxpool.New must not dial eagerly): %v", err)
	}
	defer pgPool.Close()
	verifier, err := principal.NewVerifier(writeMountedLogTestJWKS(t), "dev-health-ops-edge", "query-api")
	if err != nil {
		t.Fatalf("principal.NewVerifier: %v", err)
	}
	_, _, _, registry, err := newQueryHandler(nopQueryClient{}, pgPool, verifier, "sha256:test-schema-digest", os.Getenv)
	if err != nil {
		t.Fatalf("newQueryHandler: %v", err)
	}
	served := map[string]string{}
	for _, entry := range fetchRegistryPayload(t, registry).Operations {
		served[entry.Operation] = entry.DocumentDigest
	}

	want := map[string]string{"investmentBreakdown": registeredInvestmentBreakdownDocument}
	for _, c := range scopedInvestmentBreakdownDocuments {
		want[c.operation] = c.text
	}
	for operation, text := range want {
		if got := served[operation]; got != digestHex(text) {
			t.Errorf("GET /registry serves %s with digest %q, want %q", operation, got, digestHex(text))
		}
	}

	byDigest, err := buildOperationByDigest(served, legacyDigestsByOperation)
	if err != nil {
		t.Fatal(err)
	}
	for operation, text := range want {
		if got, ok := operationForDocument(text, byDigest); !ok || got != operation {
			t.Errorf("the %s text resolves to %q, %v; want %s, true", operation, got, ok, operation)
		}
	}
	if got, ok := operationForDocument(registeredInvestmentBreakdownV1Document, byDigest); !ok || got != "investmentBreakdown" {
		t.Errorf("the legacy investmentBreakdown text resolves to %q, %v; want investmentBreakdown, true", got, ok)
	}
	for _, c := range scopedInvestmentBreakdownDocuments {
		if legacy, ok := legacyDigestsByOperation[c.operation]; ok {
			t.Errorf("legacyDigestsByOperation[%s] = %v: no client ever sent an older text under this name", c.operation, legacy)
		}
	}
}
