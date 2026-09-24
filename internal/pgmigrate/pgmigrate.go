// Package pgmigrate is `dho migrate postgres`: it brings the application's
// PostgreSQL database (the public schema alembic owns) to its head.
//
// The head is not rebuilt from the historical alembic chain. That chain
// (src/dev_health_ops/alembic/versions) is Python, and many of its revisions
// run SQL through the connection and read data, so alembic cannot render it
// as SQL; only a database below the head needs it. So the head is a
// BASELINE: the end state of a fresh database after the real Python upgrade
// (`dev-hops migrate postgres`) has run, captured by executing it
// (baseline_capture_integration_test.go) and checked in as
// baseline/<variant>.json -- the schema and the rows the chain seeds, both as
// pg_dump SQL, and the alembic heads it records. There are two variants,
// because revision 0066 (the Celery-to-River job-route cutover) runs, and
// becomes a second head, only when DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1.
//
// Upgrade decides from the database's own state:
//   - no alembic_version table and no relation in public: apply the baseline,
//     schema then data, in ONE transaction, so an interrupted run leaves
//     nothing behind;
//   - alembic_version holds every baseline head: apply the .sql revisions
//     after the head (sql/), each in its own transaction together with the
//     alembic_version update;
//   - alembic_version without every baseline head: below the head, refused,
//     naming what the database holds and what is required;
//   - relations in public but no alembic_version: a database this migrator
//     did not create, refused.
package pgmigrate

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"
)

// CutoverEnv selects the baseline, as it gates revision 0066 in the Python
// upgrade (src/dev_health_ops/migrate.py): exactly "1" authorizes the cutover.
const CutoverEnv = "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"

//go:embed baseline/*.json
var baselineFiles embed.FS

//go:embed sql
var chainFiles embed.FS

// Baseline is the head of one variant.
type Baseline struct {
	Cutover bool `json:"cutover"`
	// Heads are the alembic_version rows a fresh upgrade records.
	Heads []string `json:"heads"`
	// Schema and Data are pg_dump plain-format SQL (schema only; data only as
	// column INSERTs), sanitized by SanitizeDump.
	Schema string `json:"schema"`
	Data   string `json:"data"`
}

// Variant names the baseline file for a cutover setting.
func Variant(cutover bool) string {
	if cutover {
		return "cutover"
	}
	return "application"
}

// CutoverAuthorized reads CutoverEnv the way the Python upgrade does.
func CutoverAuthorized(raw string, present bool) bool {
	return present && raw == "1"
}

// LoadBaseline returns the checked-in head for a cutover setting.
func LoadBaseline(cutover bool) (Baseline, error) {
	name := Variant(cutover)
	data, err := baselineFiles.ReadFile("baseline/" + name + ".json")
	if err != nil {
		return Baseline{}, fmt.Errorf("no %s baseline: %w", name, err)
	}
	var baseline Baseline
	if err := json.Unmarshal(data, &baseline); err != nil {
		return Baseline{}, fmt.Errorf("decode the %s baseline: %w", name, err)
	}
	if baseline.Cutover != cutover || len(baseline.Heads) == 0 || !strings.Contains(baseline.Schema, "CREATE TABLE") {
		return Baseline{}, fmt.Errorf("the %s baseline is malformed (cutover %v, %d heads)", name, baseline.Cutover, len(baseline.Heads))
	}
	return baseline, nil
}

// ChainFile is one revision after the head. Its file name is
// "<revision>_<slug>.sql"; revision is what alembic_version records.
type ChainFile struct {
	Revision string
	Name     string
	SQL      string
}

var chainName = regexp.MustCompile(`^([0-9]{4,})_[a-z0-9_]+\.sql$`)

// LoadChain returns the revisions after the head, in order.
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
		match := chainName.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf("sql/%s is not named <revision>_<slug>.sql", entry.Name())
		}
		data, err := chainFiles.ReadFile("sql/" + entry.Name())
		if err != nil {
			return nil, err
		}
		chain = append(chain, ChainFile{Revision: match[1], Name: entry.Name(), SQL: string(data)})
	}
	sort.Slice(chain, func(i, j int) bool { return chain[i].Revision < chain[j].Revision })
	return chain, nil
}

// State is what a database holds before an upgrade.
type State int

const (
	// StateEmpty: no alembic_version and no relation in public.
	StateEmpty State = iota
	// StateAtHead: every baseline head (or a chain revision after it) is recorded.
	StateAtHead
	// StateBelowHead: alembic_version lacks a baseline head.
	StateBelowHead
	// StateForeign: relations in public but no alembic_version.
	StateForeign
)

// Observation is what Decide needs to know about a database.
type Observation struct {
	HasVersionTable bool
	Versions        []string
	PublicRelations int
}

// Plan is the decision Upgrade acts on.
type Plan struct {
	State   State
	Missing []string
	// ApplicationHead is the revision the chain continues from.
	ApplicationHead string
	Pending         []ChainFile
}

// applicationHead is the baseline head the chain continues: the one that is
// not the cutover revision.
func applicationHead(baseline Baseline) string {
	for _, head := range baseline.Heads {
		if head != cutoverRevision {
			return head
		}
	}
	return ""
}

// cutoverRevision is the alembic revision the cutover variant adds as a
// second head.
const cutoverRevision = "0066"

// Decide classifies a database.
func Decide(observation Observation, baseline Baseline, chain []ChainFile) Plan {
	if !observation.HasVersionTable {
		if observation.PublicRelations == 0 {
			return Plan{State: StateEmpty, ApplicationHead: applicationHead(baseline), Pending: chain}
		}
		return Plan{State: StateForeign}
	}
	recorded := map[string]bool{}
	for _, version := range observation.Versions {
		recorded[version] = true
	}
	// A recorded chain revision stands for the application head it replaced.
	current := applicationHead(baseline)
	position := -1
	for index, file := range chain {
		if recorded[file.Revision] {
			current, position = file.Revision, index
		}
	}
	var missing []string
	for _, head := range baseline.Heads {
		if head == applicationHead(baseline) && position >= 0 {
			continue
		}
		if !recorded[head] {
			missing = append(missing, head)
		}
	}
	if len(missing) > 0 {
		return Plan{State: StateBelowHead, Missing: missing}
	}
	return Plan{State: StateAtHead, ApplicationHead: current, Pending: chain[position+1:]}
}

// BelowHeadError is the refusal for a database below the head.
type BelowHeadError struct {
	Cutover  bool
	Recorded []string
	Missing  []string
}

func (e BelowHeadError) Error() string {
	return fmt.Sprintf("the PostgreSQL schema is below the %s head: alembic_version holds %v and lacks %v. "+
		"dho applies the head only to an empty database; run the Python upgrade (`dev-hops migrate postgres`) with %s=%s first",
		Variant(e.Cutover), e.Recorded, e.Missing, CutoverEnv, map[bool]string{true: "1", false: "unset"}[e.Cutover])
}

// ForeignDatabaseError is the refusal for a database with relations but no
// alembic_version.
type ForeignDatabaseError struct{ Relations int }

func (e ForeignDatabaseError) Error() string {
	return fmt.Sprintf("the public schema holds %d relation(s) but no alembic_version table; refusing to apply the head over a database this migrator did not create", e.Relations)
}

// SanitizeDump turns pg_dump plain output into SQL a driver can execute. It
// removes only what a driver cannot run or what differs between two dumps of
// the same database: the psql meta-commands \restrict and \unrestrict (their
// key is random per dump), and `SET transaction_timeout`, which only
// PostgreSQL 17+ accepts. Everything else, comments included, stays: a line
// that starts with "--" can be part of a function body or a string value.
func SanitizeDump(dump string) string {
	var out []string
	for _, line := range strings.Split(dump, "\n") {
		switch {
		case strings.HasPrefix(line, `\restrict `), strings.HasPrefix(line, `\unrestrict `),
			strings.HasPrefix(line, "SET transaction_timeout = "):
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}
