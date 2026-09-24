// Package chmigrate is `dho migrate clickhouse`: it brings a ClickHouse
// database to the schema head of the analytics store.
//
// The head is not rebuilt from the historical chain. That chain
// (src/dev_health_ops/migrations/clickhouse) mixes .sql files with procedural
// Python migrations that rebuild tables and move rows, and only a database
// below the head needs them. So the head is a BASELINE: the end state of a
// fresh database after the real Python chain has run, captured by executing
// that chain (baseline_capture_integration_test.go) and checked in as
// baseline/contract<N>.json -- every table and view with its CREATE statement,
// the rows the chain seeds, and the schema_migrations versions it records.
// There is one baseline per operational ordering contract, because migration
// 067 builds a different table shape (and records itself) only under
// contract 2.
//
// Upgrade decides from the database's own state:
//   - no applied versions: apply the baseline for the configured contract.
//     Each object is created only if absent (an existing object must match
//     the baseline exactly), seed rows go only into empty tables, and the
//     versions are recorded last, so an interrupted run resumes;
//   - every baseline version applied: apply the .sql files after the head
//     (sql/), each recorded only after all its statements succeed;
//   - some versions applied but not all baseline versions: the database is
//     below the head and is refused, naming what is missing. It needs the
//     Python chain, which stays until the Python CLI is deleted.
package chmigrate

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strings"
)

// SchemaMigrationsTable records applied versions. Its shape is the one the
// Python runner creates; the baseline carries its CREATE statement.
const SchemaMigrationsTable = "schema_migrations"

// OrderingContractEnv selects the baseline, as it selects migration 067's
// behaviour in the Python chain: unset or "1" is the legacy contract, "2" the
// current one, anything else a configuration error.
const OrderingContractEnv = "OPERATIONAL_ORDERING_CONTRACT"

//go:embed baseline/*.json
var baselineFiles embed.FS

//go:embed sql
var chainFiles embed.FS

// Object is one table or view of the head.
type Object struct {
	Name   string `json:"name"`
	Engine string `json:"engine"`
	Create string `json:"create"`
}

// IsView reports whether the object is a view (plain, materialized or live).
func (o Object) IsView() bool { return strings.HasSuffix(o.Engine, "View") }

// Baseline is the head of one ordering contract.
type Baseline struct {
	Contract int               `json:"operational_ordering_contract"`
	Versions []string          `json:"versions"`
	Objects  []Object          `json:"objects"`
	Rows     map[string]string `json:"rows"`
}

// ChainFile is one migration after the head.
type ChainFile struct {
	Version string
	SQL     string
}

// ParseContract reads OPERATIONAL_ORDERING_CONTRACT the way the Python chain
// does (storage/operational_ordering_guard.py).
func ParseContract(raw string, present bool) (int, error) {
	if !present {
		return 1, nil
	}
	switch raw {
	case "1":
		return 1, nil
	case "2":
		return 2, nil
	}
	return 0, fmt.Errorf("%s must be 1 or 2, got %q", OrderingContractEnv, raw)
}

// LoadBaseline returns the checked-in head for contract.
func LoadBaseline(contract int) (Baseline, error) {
	data, err := baselineFiles.ReadFile(fmt.Sprintf("baseline/contract%d.json", contract))
	if err != nil {
		return Baseline{}, fmt.Errorf("no baseline for ordering contract %d: %w", contract, err)
	}
	var baseline Baseline
	if err := json.Unmarshal(data, &baseline); err != nil {
		return Baseline{}, fmt.Errorf("decode baseline for ordering contract %d: %w", contract, err)
	}
	if baseline.Contract != contract || len(baseline.Versions) == 0 || len(baseline.Objects) == 0 {
		return Baseline{}, fmt.Errorf("baseline for ordering contract %d is malformed (contract %d, %d versions, %d objects)",
			contract, baseline.Contract, len(baseline.Versions), len(baseline.Objects))
	}
	return baseline, nil
}

// LoadChain returns the migrations after the head, in apply order (file name
// order, the Python runner's order).
func LoadChain() ([]ChainFile, error) {
	entries, err := fs.ReadDir(chainFiles, "sql")
	if err != nil {
		return nil, err
	}
	var chain []ChainFile
	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".sql" {
			continue
		}
		data, err := chainFiles.ReadFile("sql/" + entry.Name())
		if err != nil {
			return nil, err
		}
		chain = append(chain, ChainFile{Version: entry.Name(), SQL: string(data)})
	}
	sort.Slice(chain, func(i, j int) bool { return chain[i].Version < chain[j].Version })
	return chain, nil
}

// State is what a database holds before an upgrade.
type State int

const (
	// StateEmpty: no version is applied. The baseline is applied (or resumed).
	StateEmpty State = iota
	// StateAtHead: every baseline version is applied. Chain files after the
	// head that are not applied yet are applied.
	StateAtHead
	// StateBelowHead: some versions are applied but not every baseline
	// version. Refused.
	StateBelowHead
)

// Plan is the decision Upgrade acts on.
type Plan struct {
	State State
	// Missing lists the baseline versions a below-head database lacks.
	Missing []string
	// Pending lists the chain files to apply, in order.
	Pending []ChainFile
}

// Decide classifies a database from its applied versions.
func Decide(applied map[string]bool, baseline Baseline, chain []ChainFile) Plan {
	if len(applied) == 0 {
		return Plan{State: StateEmpty, Pending: chain}
	}
	var missing []string
	for _, version := range baseline.Versions {
		if !applied[version] {
			missing = append(missing, version)
		}
	}
	if len(missing) > 0 {
		return Plan{State: StateBelowHead, Missing: missing}
	}
	var pending []ChainFile
	for _, file := range chain {
		if !applied[file.Version] {
			pending = append(pending, file)
		}
	}
	return Plan{State: StateAtHead, Pending: pending}
}

// BelowHeadError is the refusal for a database below the head.
type BelowHeadError struct {
	Contract int
	Missing  []string
}

func (e BelowHeadError) Error() string {
	shown := e.Missing
	suffix := ""
	if len(shown) > 5 {
		shown, suffix = shown[:5], fmt.Sprintf(" and %d more", len(e.Missing)-5)
	}
	return fmt.Sprintf("the ClickHouse schema is below the head for ordering contract %d: %d baseline version(s) are not applied (%s%s). "+
		"dho applies the head only to an empty database; run the Python chain (`dev-hops migrate clickhouse upgrade`) with %s=%d first",
		e.Contract, len(e.Missing), strings.Join(shown, ", "), suffix, OrderingContractEnv, e.Contract)
}
