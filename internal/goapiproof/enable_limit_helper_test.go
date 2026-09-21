package goapiproof

import (
	"encoding/json"
	"testing"
)

const testEnableLimitReason = "the proof principal can never run this route at a production build; the written limit is reviewed with the ledger"

// ledgerNamingLimits is a go-served ledger whose entries name a written
// enable limit for exactly operations, so a test can enable an operation
// with no store proof without touching the compiled ledger.
func ledgerNamingLimits(t testing.TB, operations ...string) *GoServedLedger {
	t.Helper()
	type guard struct {
		File string `json:"file"`
		Test string `json:"test"`
	}
	type entry struct {
		Operation string  `json:"operation"`
		Unproven  string  `json:"unproven_reason"`
		Guards    []guard `json:"guards"`
	}
	document := struct {
		Message string  `json:"deletion_error_message"`
		Entries []entry `json:"entries"`
	}{Message: "{operation} is served by query-api"}
	for _, operation := range operations {
		document.Entries = append(document.Entries, entry{
			Operation: operation,
			Unproven:  testEnableLimitReason,
			Guards:    []guard{{File: "internal/goapiproof/enable_limit_helper_test.go", Test: "TestEnableLimitHelper"}},
		})
	}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := ParseGoServedLedger(raw)
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}
