package server

import (
	"strings"
	"testing"
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
