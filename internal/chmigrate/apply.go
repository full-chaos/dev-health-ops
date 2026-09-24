package chmigrate

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// DB is what the migrator needs from a ClickHouse connection whose default
// database is the one being migrated.
type DB interface {
	// Exec runs one statement.
	Exec(ctx context.Context, statement string) error
	// AppliedVersions returns the versions recorded in schema_migrations,
	// or an empty set when the table does not exist.
	AppliedVersions(ctx context.Context) (map[string]bool, error)
	// Objects lists the tables and views of the database, without a
	// materialized view's .inner table.
	Objects(ctx context.Context) ([]string, error)
	// ObjectCreate returns an object's CREATE statement with the current
	// database's name removed, and whether the object exists.
	ObjectCreate(ctx context.Context, name string) (string, bool, error)
	// RowCount returns the number of rows in a table.
	RowCount(ctx context.Context, table string) (uint64, error)
}

// Result is what one upgrade did.
type Result struct {
	Contract int      `json:"operational_ordering_contract"`
	Action   string   `json:"action"`
	Head     string   `json:"head"`
	Created  []string `json:"created,omitempty"`
	Seeded   []string `json:"seeded,omitempty"`
	Applied  []string `json:"applied,omitempty"`
}

// Upgrade brings the database behind db to the head for contract, then
// applies every chain file after the head.
func Upgrade(ctx context.Context, db DB, baseline Baseline, chain []ChainFile) (Result, error) {
	result := Result{Contract: baseline.Contract, Head: HeadVersion(baseline, chain)}
	plan, err := readPlan(ctx, db, baseline, chain)
	if err != nil {
		return result, err
	}
	switch plan.State {
	case StateBelowHead:
		return result, BelowHeadError{Contract: baseline.Contract, Missing: plan.Missing}
	case StateForeign:
		return result, ForeignDatabaseError{Objects: plan.Foreign}
	case StateSchemaMismatch:
		return result, SchemaMismatchError{MissingObjects: plan.MissingObjects}
	case StateEmpty:
		created, seeded, err := applyBaseline(ctx, db, baseline)
		result.Created, result.Seeded = created, seeded
		if err != nil {
			return result, err
		}
		result.Action = "baseline_applied"
	default:
		result.Action = "up_to_date"
	}
	for _, file := range plan.Pending {
		if err := applyChainFile(ctx, db, file); err != nil {
			return result, err
		}
		result.Applied = append(result.Applied, file.Version)
	}
	if result.Action == "up_to_date" && len(result.Applied) > 0 {
		result.Action = "chain_applied"
	}
	return result, nil
}

// HeadVersion is the last migration: the last chain file after the head
// when there is one, else the last one the baseline records. The Python
// runner also records "__init__.py", which sorts after every numbered name,
// so it is skipped.
func HeadVersion(baseline Baseline, chain []ChainFile) string {
	head := ""
	for _, version := range baseline.Versions {
		if version != "__init__.py" && version > head {
			head = version
		}
	}
	for _, file := range chain {
		if file.Version > head {
			head = file.Version
		}
	}
	return head
}

// readPlan reads the database's state and decides.
func readPlan(ctx context.Context, db DB, baseline Baseline, chain []ChainFile) (Plan, error) {
	applied, err := db.AppliedVersions(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("read applied versions: %w", err)
	}
	objects, err := db.Objects(ctx)
	if err != nil {
		return Plan{}, fmt.Errorf("list objects: %w", err)
	}
	return Decide(applied, objects, baseline, chain), nil
}

// applyBaseline creates the head in an empty (or partly baselined) database.
// Order: tables, then their seed rows, then views -- a materialized view that
// exists while rows land would re-emit them into its target. Within a phase a
// statement that fails is retried after the others (a view over a view needs
// its source first); a round with no progress is an error. The versions are
// recorded last, so a run interrupted before them leaves no version and the
// next run resumes here.
func applyBaseline(ctx context.Context, db DB, baseline Baseline) (created, seeded []string, err error) {
	var tables, views []Object
	for _, object := range baseline.Objects {
		if object.IsView() {
			views = append(views, object)
		} else {
			tables = append(tables, object)
		}
	}
	made, err := createAll(ctx, db, tables)
	created = append(created, made...)
	if err != nil {
		return created, seeded, err
	}
	names := make([]string, 0, len(baseline.Rows))
	for name := range baseline.Rows {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		count, err := db.RowCount(ctx, name)
		if err != nil {
			return created, seeded, fmt.Errorf("count rows of %s: %w", name, err)
		}
		if count != 0 {
			continue
		}
		if err := db.Exec(ctx, "INSERT INTO "+quoteIdentifier(name)+" FORMAT JSONEachRow\n"+baseline.Rows[name]); err != nil {
			return created, seeded, fmt.Errorf("seed rows of %s: %w", name, err)
		}
		seeded = append(seeded, name)
	}
	made, err = createAll(ctx, db, views)
	created = append(created, made...)
	if err != nil {
		return created, seeded, err
	}
	if err := recordVersions(ctx, db, baseline.Versions); err != nil {
		return created, seeded, err
	}
	return created, seeded, nil
}

// createAll creates every object that does not exist yet. An object that
// exists must have exactly the baseline's CREATE statement; anything else is
// a database this migrator does not understand, and it stops.
func createAll(ctx context.Context, db DB, objects []Object) ([]string, error) {
	var created []string
	var pending []Object
	for _, object := range objects {
		existing, exists, err := db.ObjectCreate(ctx, object.Name)
		if err != nil {
			return created, fmt.Errorf("read %s: %w", object.Name, err)
		}
		if !exists {
			pending = append(pending, object)
			continue
		}
		if existing != object.Create {
			return created, fmt.Errorf("%s exists with a different definition than the baseline; refusing to continue on a database this migrator did not create", object.Name)
		}
	}
	for len(pending) > 0 {
		var retry []Object
		var first error
		for _, object := range pending {
			if err := db.Exec(ctx, object.Create); err != nil {
				retry = append(retry, object)
				if first == nil {
					first = fmt.Errorf("create %s: %w", object.Name, err)
				}
				continue
			}
			created = append(created, object.Name)
		}
		if len(retry) == len(pending) {
			return created, first
		}
		pending = retry
	}
	return created, nil
}

// applyChainFile runs one migration after the head and records it only after
// every statement succeeded. ClickHouse has no DDL transactions, so a failure
// part-way leaves the version unrecorded and the next run starts the file
// again; chain files must therefore be re-runnable (IF NOT EXISTS), the same
// rule the Python runner imposes.
func applyChainFile(ctx context.Context, db DB, file ChainFile) error {
	for index, statement := range SplitStatements(file.SQL) {
		if err := db.Exec(ctx, statement); err != nil {
			return fmt.Errorf("%s statement %d: %w", file.Version, index+1, err)
		}
	}
	return recordVersions(ctx, db, []string{file.Version})
}

func recordVersions(ctx context.Context, db DB, versions []string) error {
	if len(versions) == 0 {
		return nil
	}
	values := make([]string, 0, len(versions))
	for _, version := range versions {
		values = append(values, "("+quoteString(version)+", now())")
	}
	if err := db.Exec(ctx, "INSERT INTO "+SchemaMigrationsTable+" (version, applied_at) VALUES "+strings.Join(values, ", ")); err != nil {
		return fmt.Errorf("record versions: %w", err)
	}
	return nil
}

// Status is the read-only view of a database.
type Status struct {
	Contract int      `json:"operational_ordering_contract"`
	State    string   `json:"state"`
	Head     string   `json:"head"`
	Applied  int      `json:"applied"`
	Missing  []string `json:"missing,omitempty"`
	Foreign  []string `json:"foreign,omitempty"`
	// MissingObjects are the baseline objects a schema_mismatch database lacks.
	MissingObjects []string `json:"missing_objects,omitempty"`
	Pending        []string `json:"pending,omitempty"`
}

// ReadStatus reports where the database stands without changing it.
func ReadStatus(ctx context.Context, db DB, baseline Baseline, chain []ChainFile) (Status, error) {
	status := Status{Contract: baseline.Contract, Head: HeadVersion(baseline, chain)}
	applied, err := db.AppliedVersions(ctx)
	if err != nil {
		return status, fmt.Errorf("read applied versions: %w", err)
	}
	status.Applied = len(applied)
	plan, err := readPlan(ctx, db, baseline, chain)
	if err != nil {
		return status, err
	}
	status.Missing, status.Foreign, status.MissingObjects = plan.Missing, plan.Foreign, plan.MissingObjects
	for _, file := range plan.Pending {
		status.Pending = append(status.Pending, file.Version)
	}
	switch plan.State {
	case StateEmpty:
		status.State = "empty"
	case StateBelowHead:
		status.State = "below_head"
	case StateForeign:
		status.State = "foreign"
	case StateSchemaMismatch:
		status.State = "schema_mismatch"
	default:
		status.State = "at_head"
	}
	return status, nil
}

func quoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func quoteString(value string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(value) + "'"
}
