// Package pgmigrate is `dho migrate postgres`: it brings the application's
// PostgreSQL database (the public schema alembic owns) to its head.
//
// The head is not rebuilt from the historical alembic chain. That chain
// (src/dev_health_ops/alembic/versions) is Python, and many of its revisions
// run SQL through the connection and read data, so alembic cannot render it
// as SQL. So the migrator is a BASELINE plus a numbered SQL CHAIN: the baseline
// is the end state of a fresh database at the baseline revision (0138, the
// oldest revision a production database is upgraded from) after the real Python
// upgrade has run, captured by executing it (baseline_integration_test.go) and
// checked in as baseline/head.json -- the schema and the rows the chain seeds,
// both as pg_dump SQL, and the alembic heads it records; every revision after it
// is a sql/<revision>_<slug>.sql file, rendered from its Alembic revision and
// verified by executing both. The baseline never moves: adding an Alembic
// revision adds its .sql file (TestChainCoversEveryAlembicRevision fails until it
// does), so a database at any revision from the baseline up reaches the head.
//
// The chain's end state depends on two settings, and the baseline is the
// combination production runs (read from prod, 2026-09-24): revision 0066,
// the Celery-to-River job-route cutover, applied
// (DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1, so 0066 is a second head), and the
// River schema named "river" (RIVER_DATABASE_SCHEMA, which revision 0110
// reads). Any other environment either runs that same combination or is
// re-created from this baseline: dho refuses a different setting, naming it,
// before it touches a database.
//
// Upgrade decides from the database's own state:
//   - no alembic_version table and no object (relation, function or type)
//     in any non-system schema: apply the baseline,
//     schema then data, in ONE transaction, so an interrupted run leaves
//     nothing behind;
//   - alembic_version holds every baseline head and no later revision, but
//     tables the baseline creates are absent: schema_mismatch, refused;
//   - alembic_version holds every baseline head (or a chain revision that
//     continues it): apply the .sql revisions after the one it holds (sql/), each
//     in its own transaction together with the alembic_version update;
//   - alembic_version records a revision this build does not know (its embedded
//     Alembic walk, its baseline heads and its chain are the ones it knows), however
//     many baseline heads it also records: refused as ahead of the build, naming the
//     unknown revisions, before anything else is decided. A newer build migrated the
//     database (an image was rolled back onto it), and dho never downgrades;
//   - alembic_version without every baseline head (below the baseline revision):
//     refused as below the head, naming what the database holds and what is
//     required;
//   - objects but no alembic_version: a database this migrator
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

// CutoverEnv gates revision 0066 in the Python upgrade
// (src/dev_health_ops/migrate.py): exactly "1" authorizes the cutover.
const CutoverEnv = "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"

// RiverSchemaEnv names the River schema; revision 0110 reads it.
const RiverSchemaEnv = "RIVER_DATABASE_SCHEMA"

// defaultRiverSchema is what 0110 (and internal/platform/config) use when
// RiverSchemaEnv is blank.
const defaultRiverSchema = "river"

//go:embed baseline/head.json
var baselineFile []byte

//go:embed sql
var chainFiles embed.FS

// Baseline is the head: the combination production runs.
type Baseline struct {
	// Cutover and RiverSchema are the settings the baseline was captured
	// with; dho refuses an environment that sets them differently.
	Cutover     bool   `json:"cutover"`
	RiverSchema string `json:"river_schema"`
	// Heads are the alembic_version rows a fresh upgrade records.
	Heads []string `json:"heads"`
	// Schema and Data are pg_dump plain-format SQL (schema only; data only as
	// column INSERTs), sanitized by SanitizeDump.
	Schema string `json:"schema"`
	Data   string `json:"data"`
}

// LoadBaseline returns the checked-in head.
func LoadBaseline() (Baseline, error) {
	var baseline Baseline
	if err := json.Unmarshal(baselineFile, &baseline); err != nil {
		return Baseline{}, fmt.Errorf("decode the baseline: %w", err)
	}
	if len(baseline.Heads) == 0 || baseline.RiverSchema == "" || !strings.Contains(baseline.Schema, "CREATE TABLE") {
		return Baseline{}, fmt.Errorf("the baseline is malformed (%d heads, river schema %q)", len(baseline.Heads), baseline.RiverSchema)
	}
	return baseline, nil
}

// Settings are the two environment values the chain's end state depends on,
// read the way the Python upgrade reads them.
type Settings struct {
	Cutover     bool
	RiverSchema string
}

// ReadSettings reads CutoverEnv (exactly "1" authorizes) and RiverSchemaEnv
// (blank falls back to "river", as 0110 does).
func ReadSettings(lookup func(string) (string, bool)) Settings {
	raw, present := lookup(CutoverEnv)
	schema, _ := lookup(RiverSchemaEnv)
	if strings.TrimSpace(schema) == "" {
		schema = defaultRiverSchema
	}
	return Settings{Cutover: present && raw == "1", RiverSchema: schema}
}

// SettingsMismatchError is the refusal for an environment whose settings
// differ from the ones the baseline was captured with.
type SettingsMismatchError struct{ Mismatches []string }

func (e SettingsMismatchError) Error() string {
	return "the environment does not match the PostgreSQL head baseline (production's settings): " +
		strings.Join(e.Mismatches, "; ") +
		". Run with production's settings, or re-create this database from the baseline"
}

// CheckSettings refuses settings that differ from the baseline's.
func CheckSettings(settings Settings, baseline Baseline) error {
	var mismatches []string
	if settings.Cutover != baseline.Cutover {
		mismatches = append(mismatches, fmt.Sprintf("the River cutover (revision 0066) %s expected, %s=%s found",
			map[bool]string{true: "applied is", false: "not applied is"}[baseline.Cutover], CutoverEnv,
			map[bool]string{true: "1", false: "unset"}[settings.Cutover]))
	}
	if settings.RiverSchema != baseline.RiverSchema {
		mismatches = append(mismatches, fmt.Sprintf("River schema %q expected, %q found (%s)", baseline.RiverSchema, settings.RiverSchema, RiverSchemaEnv))
	}
	if len(mismatches) > 0 {
		return SettingsMismatchError{Mismatches: mismatches}
	}
	return nil
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
	// StateEmpty: no alembic_version and no object in a non-system schema.
	StateEmpty State = iota
	// StateAtHead: every baseline head (or a chain revision after it) is recorded.
	StateAtHead
	// StateBelowHead: alembic_version lacks a baseline head.
	StateBelowHead
	// StateForeign: objects but no alembic_version.
	StateForeign
	// StateSchemaMismatch: alembic_version records the baseline heads and no
	// later revision, but tables the baseline creates are absent.
	StateSchemaMismatch
	// StateAheadOfBuild: alembic_version records a revision this build does not
	// know: a newer build migrated the database (an image was rolled back onto it).
	StateAheadOfBuild
)

// Observation is what Decide needs to know about a database.
type Observation struct {
	HasVersionTable bool
	Versions        []string
	// Objects counts the relations, functions and types outside the system
	// schemas and outside extensions; an index, a sequence, a function, an
	// enum or a River table each makes the database not empty.
	Objects int
	// PublicTables names the ordinary and partitioned tables in public.
	PublicTables []string
}

// Plan is the decision Upgrade acts on.
type Plan struct {
	State   State
	Missing []string
	// MissingTables are the baseline tables a StateSchemaMismatch database lacks.
	MissingTables []string
	// ApplicationHead is the revision the chain continues from.
	ApplicationHead string
	Pending         []ChainFile
	// Unknown are the recorded revisions this build does not know
	// (StateAheadOfBuild), sorted.
	Unknown []string
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

// Decide classifies a database. known is every revision this build knows (see
// KnownRevisions); a recorded revision outside it makes the database ahead of this
// build and is refused before anything else is decided: dho never downgrades, and
// applying the chain over a database a newer build migrated is not an upgrade.
func Decide(observation Observation, baseline Baseline, chain []ChainFile, known map[string]bool) Plan {
	if !observation.HasVersionTable {
		if observation.Objects == 0 {
			return Plan{State: StateEmpty, ApplicationHead: applicationHead(baseline), Pending: chain}
		}
		return Plan{State: StateForeign}
	}
	if unknown := unknownRevisions(observation.Versions, known); len(unknown) > 0 {
		return Plan{State: StateAheadOfBuild, Unknown: unknown}
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
	// With no chain revision recorded, the database claims exactly the
	// baseline, so the baseline's tables must be there: alembic_version rows
	// alone do not prove the schema. Once a chain revision is recorded, a
	// revision may have dropped a baseline table, and alembic_version is
	// trusted as Alembic trusts it.
	if position < 0 {
		present := map[string]bool{}
		for _, table := range observation.PublicTables {
			present[table] = true
		}
		var absent []string
		for _, table := range baseline.Tables() {
			if !present[table] {
				absent = append(absent, table)
			}
		}
		if len(absent) > 0 {
			return Plan{State: StateSchemaMismatch, MissingTables: absent}
		}
	}
	return Plan{State: StateAtHead, ApplicationHead: current, Pending: chain[position+1:]}
}

// unknownRevisions are the recorded revisions outside known, sorted. A nil known
// is a programming error, not "everything is known": it would silently switch the
// refusal off.
func unknownRevisions(recorded []string, known map[string]bool) []string {
	if known == nil {
		panic("pgmigrate: Decide needs the set of revisions this build knows")
	}
	var unknown []string
	for _, revision := range recorded {
		if !known[revision] {
			unknown = append(unknown, revision)
		}
	}
	sort.Strings(unknown)
	return unknown
}

// AheadOfBuildError is the refusal for a database that records a revision this build
// does not know.
type AheadOfBuildError struct {
	Recorded []string
	Unknown  []string
}

func (e AheadOfBuildError) Error() string {
	return fmt.Sprintf("the database records revision(s) %v that this build does not know (alembic_version holds %v): "+
		"it was migrated by a newer build (an image rolled back?). dho never downgrades: run the build that knows them, "+
		"or restore the database from a backup. Nothing was changed", e.Unknown, e.Recorded)
}

// BelowHeadError is the refusal for a database below the head.
type BelowHeadError struct {
	Recorded []string
	Missing  []string
}

func (e BelowHeadError) Error() string {
	detail := ""
	for _, missing := range e.Missing {
		if missing == cutoverRevision {
			detail = " The River cutover (revision 0066) is expected, as in production, and is not applied."
		}
	}
	return fmt.Sprintf("the PostgreSQL schema is below the head: alembic_version holds %v and lacks %v.%s "+
		"dho upgrades an empty database and one at the baseline revision or later; run the Python upgrade (`dev-hops migrate postgres`) with production's settings first",
		e.Recorded, e.Missing, detail)
}

// ForeignDatabaseError is the refusal for a database with objects but no
// alembic_version.
type ForeignDatabaseError struct{ Objects int }

func (e ForeignDatabaseError) Error() string {
	return fmt.Sprintf("the database holds %d object(s) (relations, functions or types, in any schema but the system ones) but no public.alembic_version table; refusing to apply the head over a database this migrator did not create", e.Objects)
}

// SchemaMismatchError is the refusal for a database whose alembic_version
// records the baseline heads while tables the baseline creates are absent.
type SchemaMismatchError struct {
	Recorded      []string
	MissingTables []string
}

func (e SchemaMismatchError) Error() string {
	shown := e.MissingTables
	more := ""
	if len(shown) > 10 {
		shown, more = shown[:10], fmt.Sprintf(" and %d more", len(e.MissingTables)-10)
	}
	return fmt.Sprintf("alembic_version records %v, but %d table(s) the head creates are absent: %v%s; "+
		"refusing to treat this database as at the head", e.Recorded, len(e.MissingTables), shown, more)
}

// baselineTable matches a table the baseline schema creates.
var baselineTable = regexp.MustCompile(`(?m)^CREATE TABLE public\.(\w+) \(`)

// Tables names the tables the baseline schema creates, in dump order.
func (b Baseline) Tables() []string {
	var tables []string
	for _, match := range baselineTable.FindAllStringSubmatch(b.Schema, -1) {
		tables = append(tables, match[1])
	}
	return tables
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
