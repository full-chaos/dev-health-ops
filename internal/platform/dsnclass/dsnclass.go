// Package dsnclass is the closed table of database DSN secrets each worker
// process class may hold. The deployment manifest validator, the rendered-chart
// checks and the process start-up guard all read it, so a class's allowed set
// has one source.
package dsnclass

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Class names. The manifest runtimes "river", "control" and "stream" are
// classes; "operator" and "migration" are the operator CLI and the migration job.
const (
	River     = "river"
	Control   = "control"
	Stream    = "stream"
	Operator  = "operator"
	Migration = "migration"
)

var allowed = map[string][]string{
	River:     {"POSTGRES_URI", "WORKER_DATABASE_URI"},
	Control:   {"COORDINATOR_DATABASE_URI", "POSTGRES_URI", "WORKER_DATABASE_URI"},
	Stream:    {"POSTGRES_URI"},
	Operator:  {"COORDINATOR_DATABASE_URI", "POSTGRES_URI", "WORKER_DATABASE_URI"},
	Migration: {"MIGRATION_DATABASE_URI"},
}

// Allowed returns the database DSN secret names a class may carry, sorted, or
// nil for a class that is not in the table.
func Allowed(class string) []string {
	return slices.Clone(allowed[class])
}

// Secret names the DSN secret an environment variable name stands for, and
// reports whether it stands for one. The pattern is exactly: the name, with an
// optional "_FILE" suffix removed, ends in "_URI" or "_URL" and contains
// DATABASE or POSTGRES. A new connection secret therefore falls under the table until it is
// added to it. Component-form variables (DEV_HEALTH_PG_*) are not matched.
func Secret(name string) (string, bool) {
	base := strings.TrimSuffix(name, "_FILE")
	if (strings.HasSuffix(base, "_URI") || strings.HasSuffix(base, "_URL")) && (strings.Contains(base, "DATABASE") || strings.Contains(base, "POSTGRES")) {
		return base, true
	}
	return "", false
}

// Foreign returns the names that stand for a DSN secret outside the class's
// allowed set.
func Foreign(class string, names []string) []string {
	var foreign []string
	for _, name := range names {
		if secret, ok := Secret(name); ok && !slices.Contains(allowed[class], secret) {
			foreign = append(foreign, name)
		}
	}
	return foreign
}

// Refusal is returned by CheckEnviron. It carries variable NAMES and the class,
// never a value.
type Refusal struct {
	Class string
	Names []string
}

func (refusal *Refusal) Error() string {
	return fmt.Sprintf("process class %s holds database DSN variables outside its class: %s",
		refusal.Class, strings.Join(refusal.Names, ", "))
}

var validName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)

// CheckEnviron refuses an environment (os.Environ form, "NAME=value") in which a
// variable that stands for a DSN secret outside the class's set has a non-empty
// value. An empty value counts as absent. A name that is not a plain identifier
// cannot stand for a DSN secret and is ignored, so no free text reaches a log.
func CheckEnviron(class string, environ []string) error {
	var names []string
	for _, entry := range environ {
		name, value, found := strings.Cut(entry, "=")
		if !found || value == "" {
			continue
		}
		if !validName.MatchString(name) {
			continue
		}
		names = append(names, name)
	}
	foreign := Foreign(class, names)
	if len(foreign) == 0 {
		return nil
	}
	sort.Strings(foreign)
	return &Refusal{Class: class, Names: slices.Compact(foreign)}
}
