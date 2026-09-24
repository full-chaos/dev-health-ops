// Package chmigrate is `dho migrate clickhouse`: it brings a ClickHouse
// database to the schema head of the analytics store.
//
// The head is not rebuilt from the historical chain. That chain
// (src/dev_health_ops/migrations/clickhouse) mixes .sql files with procedural
// Python migrations that rebuild tables and move rows, and only a database
// below the head needs them. So the head is a BASELINE: the end state of a
// fresh database after the real Python chain has run, captured by executing
// that chain (baseline_integration_test.go) and checked in as
// baseline/head.json -- every table and view with its CREATE statement, the
// rows the chain seeds, and the schema_migrations versions it records.
//
// The chain's end state depends on OPERATIONAL_ORDERING_CONTRACT: migration
// 067 builds a different table shape, and records itself, only under contract
// 2. The baseline is the combination production runs (read from prod,
// 2026-09-24): contract 2. Any other environment runs contract 2 too or is
// re-created from the baseline; dho refuses another contract, naming it,
// before it touches a database.
//
// Upgrade decides from the database's own state:
//   - no applied versions and no object the baseline does not create: apply
//     the baseline.
//     Each object is created only if absent (an existing object must match
//     the baseline exactly), seed rows go only into empty tables, and the
//     versions are recorded last, so an interrupted run resumes;
//   - every baseline version applied: apply the .sql files after the head
//     (sql/), each recorded only after all its statements succeed;
//   - some versions applied but not all baseline versions: the database is
//     below the head and is refused, naming what is missing. It needs the
//     Python chain, which stays until the Python CLI is deleted;
//   - no applied versions but an object the baseline does not create: a
//     database this migrator did not create, refused.
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

// OrderingContractEnv selects migration 067's behaviour in the Python chain:
// unset or "1" is the legacy contract, "2" the current one, anything else a
// configuration error. The baseline is contract 2.
const OrderingContractEnv = "OPERATIONAL_ORDERING_CONTRACT"

//go:embed baseline/head.json
var baselineFile []byte

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

// LoadBaseline returns the checked-in head.
func LoadBaseline() (Baseline, error) {
	var baseline Baseline
	if err := json.Unmarshal(baselineFile, &baseline); err != nil {
		return Baseline{}, fmt.Errorf("decode the baseline: %w", err)
	}
	if baseline.Contract == 0 || len(baseline.Versions) == 0 || len(baseline.Objects) == 0 {
		return Baseline{}, fmt.Errorf("the baseline is malformed (contract %d, %d versions, %d objects)",
			baseline.Contract, len(baseline.Versions), len(baseline.Objects))
	}
	return baseline, nil
}

// ContractMismatchError is the refusal for an environment whose ordering
// contract differs from the baseline's.
type ContractMismatchError struct{ Expected, Found int }

func (e ContractMismatchError) Error() string {
	return fmt.Sprintf("the environment does not match the ClickHouse head baseline (production's settings): "+
		"operational ordering contract %d expected, %s=%d found. Run with production's settings, or re-create this database from the baseline",
		e.Expected, OrderingContractEnv, e.Found)
}

// CheckContract refuses a contract other than the baseline's.
func CheckContract(contract int, baseline Baseline) error {
	if contract != baseline.Contract {
		return ContractMismatchError{Expected: baseline.Contract, Found: contract}
	}
	return nil
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
	// StateEmpty: no version is applied and no object is foreign. The
	// baseline is applied (or resumed).
	StateEmpty State = iota
	// StateAtHead: every baseline version is applied. Chain files after the
	// head that are not applied yet are applied.
	StateAtHead
	// StateBelowHead: some versions are applied but not every baseline
	// version. Refused.
	StateBelowHead
	// StateForeign: no version is applied but the database holds an object
	// the baseline does not create. Refused.
	StateForeign
	// StateSchemaMismatch: every baseline version and no chain file is
	// recorded, but objects the baseline creates are absent. Refused.
	StateSchemaMismatch
)

// Plan is the decision Upgrade acts on.
type Plan struct {
	State State
	// Missing lists the baseline versions a below-head database lacks.
	Missing []string
	// Foreign lists the objects of an unversioned database the baseline does
	// not create.
	Foreign []string
	// Pending lists the chain files to apply, in order.
	Pending []ChainFile
	// MissingObjects lists the baseline objects a schema-mismatch database
	// lacks.
	MissingObjects []string
}

// Decide classifies a database from its applied versions and the objects it
// holds.
func Decide(applied map[string]bool, objects []string, baseline Baseline, chain []ChainFile) Plan {
	if len(applied) == 0 {
		known := map[string]bool{}
		for _, object := range baseline.Objects {
			known[object.Name] = true
		}
		var foreign []string
		for _, name := range objects {
			if !known[name] {
				foreign = append(foreign, name)
			}
		}
		if len(foreign) > 0 {
			sort.Strings(foreign)
			return Plan{State: StateForeign, Foreign: foreign}
		}
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
	chainApplied := false
	for _, file := range chain {
		if applied[file.Version] {
			chainApplied = true
		} else {
			pending = append(pending, file)
		}
	}
	// With no chain file recorded, the database claims exactly the baseline,
	// so its objects must be there: version rows alone do not prove the
	// schema. Once a chain file is recorded, it may have dropped a baseline
	// object, and schema_migrations is trusted, as the Python runner trusts it.
	if !chainApplied {
		present := map[string]bool{}
		for _, name := range objects {
			present[name] = true
		}
		var absent []string
		for _, object := range baseline.Objects {
			if !present[object.Name] {
				absent = append(absent, object.Name)
			}
		}
		if len(absent) > 0 {
			sort.Strings(absent)
			return Plan{State: StateSchemaMismatch, MissingObjects: absent}
		}
	}
	return Plan{State: StateAtHead, Pending: pending}
}

// SchemaMismatchError is the refusal for a database whose schema_migrations
// records the head while objects the baseline creates are absent.
type SchemaMismatchError struct{ MissingObjects []string }

func (e SchemaMismatchError) Error() string {
	shown, suffix := e.MissingObjects, ""
	if len(shown) > 5 {
		shown, suffix = shown[:5], fmt.Sprintf(" and %d more", len(e.MissingObjects)-5)
	}
	return fmt.Sprintf("schema_migrations records the head, but %d object(s) the head creates are absent (%s%s); "+
		"refusing to treat this database as at the head", len(e.MissingObjects), strings.Join(shown, ", "), suffix)
}

// ForeignDatabaseError is the refusal for an unversioned database that holds
// objects the baseline does not create.
type ForeignDatabaseError struct{ Objects []string }

func (e ForeignDatabaseError) Error() string {
	shown, suffix := e.Objects, ""
	if len(shown) > 5 {
		shown, suffix = shown[:5], fmt.Sprintf(" and %d more", len(e.Objects)-5)
	}
	return fmt.Sprintf("the database records no schema_migrations version but holds %d object(s) the baseline does not create (%s%s); "+
		"refusing to apply the head over a database this migrator did not create", len(e.Objects), strings.Join(shown, ", "), suffix)
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
	return fmt.Sprintf("the ClickHouse schema is below the head (ordering contract %d): %d baseline version(s) are not applied (%s%s). "+
		"dho applies the head only to an empty database; run the Python chain (`dev-hops migrate clickhouse upgrade`) with %s=%d first",
		e.Contract, len(e.Missing), strings.Join(shown, ", "), suffix, OrderingContractEnv, e.Contract)
}
