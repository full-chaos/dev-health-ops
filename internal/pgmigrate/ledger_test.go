package pgmigrate

import (
	"reflect"
	"strings"
	"testing"
)

func TestDecideJudgesTheLedger(t *testing.T) {
	baseline, err := LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain := []ChainFile{
		{Revision: "0163", Name: "0163_probe.sql", SQL: "-- a\n"},
		{Revision: "0164", Name: "0164" + ledgerFileSuffix, SQL: "-- ledger\n"},
		{Revision: "0165", Name: "0165_probe.sql", SQL: "-- b\n"},
	}
	known := map[string]bool{cutoverRevision: true, applicationHead(baseline): true, "0163": true, "0164": true, "0165": true}
	full := map[string]LedgerRow{
		"0163": {Seed: true},
		"0164": {SHA256: FileSHA256(chain[1])},
		"0165": {SHA256: FileSHA256(chain[2])},
	}
	without := func(revision string) map[string]LedgerRow {
		out := map[string]LedgerRow{}
		for k, v := range full {
			if k != revision {
				out[k] = v
			}
		}
		return out
	}
	with := func(revision string, row LedgerRow) map[string]LedgerRow {
		out := without(revision)
		out[revision] = row
		return out
	}
	at := func(head string, hasLedger bool, ledger map[string]LedgerRow) Observation {
		return Observation{HasVersionTable: true, Versions: []string{cutoverRevision, head}, HasLedger: hasLedger, Ledger: ledger}
	}
	cells := []struct {
		name        string
		observation Observation
		chain       []ChainFile
		state       State
		unapplied   []string
		mismatch    []string
		absent      bool
	}{
		{"below the ledger revision: no ledger to judge", at("0163", false, nil), chain, StateAtHead, nil, nil, false},
		{"a chain without the ledger revision: no ledger to judge", at("0163", false, nil), chain[:1], StateAtHead, nil, nil, false},
		{"two-row alembic_version at the head, full ledger", at("0165", true, full), chain, StateAtHead, nil, nil, false},
		{"two-row alembic_version below the head, ledger through it", at("0164", true, without("0165")), chain, StateAtHead, nil, nil, false},
		{"a revision below the head has no row", at("0165", true, without("0163")), chain, StateChainRevisionUnapplied, []string{"0163"}, nil, false},
		{"the head itself has no row", at("0165", true, without("0165")), chain, StateChainRevisionUnapplied, []string{"0165"}, nil, false},
		{"the ledger table is absent", at("0164", false, nil), chain, StateChainRevisionUnapplied, []string{"0163", "0164"}, nil, true},
		{"an apply row records another file", at("0165", true, with("0164", LedgerRow{SHA256: FileSHA256(ChainFile{SQL: "other"})})), chain, StateChainRevisionSHAMismatch, nil, []string{"0164"}, false},
		{"a seed row is never compared", at("0165", true, with("0163", LedgerRow{Seed: true})), chain, StateAtHead, nil, nil, false},
		{"a missing row wins over a sha mismatch", at("0165", true, map[string]LedgerRow{"0164": full["0164"], "0165": {SHA256: "x"}}), chain, StateChainRevisionUnapplied, []string{"0163"}, nil, false},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			plan := Decide(cell.observation, baseline, cell.chain, known)
			if plan.State != cell.state || !reflect.DeepEqual(plan.Unapplied, cell.unapplied) || !reflect.DeepEqual(plan.SHAMismatch, cell.mismatch) || plan.LedgerAbsent != cell.absent {
				t.Fatalf("plan = %+v, want state %d unapplied %v mismatch %v absent %v", plan, cell.state, cell.unapplied, cell.mismatch, cell.absent)
			}
		})
	}
}

func TestTheLiveChainCarriesOneLedgerRevision(t *testing.T) {
	chain, err := LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, file := range chain {
		if strings.HasSuffix(file.Name, ledgerFileSuffix) {
			count++
		}
	}
	if count != 1 || ledgerIndex(chain) < 0 {
		t.Fatalf("the live chain has %d ledger revisions, want exactly 1", count)
	}
}

// appliedLedger is the ledger dho writes for a database it walked through every revision of chain.
func appliedLedger(chain []ChainFile) map[string]LedgerRow {
	ledger := map[string]LedgerRow{}
	for _, file := range chain {
		ledger[file.Revision] = LedgerRow{SHA256: FileSHA256(file)}
	}
	return ledger
}
