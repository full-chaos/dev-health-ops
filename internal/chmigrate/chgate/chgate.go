// Package chgate refuses to start a ClickHouse-writing process whose ClickHouse schema is
// behind the build: schema_migrations must hold every version the build's baseline and
// chain carry. A schema ahead of the build (a rollback) starts with a WARN, and a read
// that cannot happen (unreachable, no grant, no table) is skipped with a WARN. The read is
// one plain SELECT, so the gate does not depend on the ClickHouse server version.
package chgate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const readTimeout = 10 * time.Second

// Recorder is the optional capability of a process storage that can read the versions
// schema_migrations records.
type Recorder interface {
	AppliedVersions(ctx context.Context) ([]string, error)
}

// Build is the set of versions this build brings a ClickHouse database to.
type Build struct {
	versions []string
	known    map[string]bool
	head     string
}

// LoadBuild reads the embedded baseline and chain.
func LoadBuild() (Build, error) {
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		return Build{}, fmt.Errorf("load the baseline: %w", err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		return Build{}, fmt.Errorf("load the chain: %w", err)
	}
	build := Build{known: map[string]bool{}}
	for _, version := range baseline.Versions {
		build.add(version)
	}
	for _, file := range chain {
		build.add(file.Version)
		build.head = file.Version
	}
	if build.head == "" && len(build.versions) > 0 {
		build.head = build.versions[len(build.versions)-1]
	}
	return build, nil
}

func (b *Build) add(version string) {
	if b.known[version] {
		return
	}
	b.known[version] = true
	b.versions = append(b.versions, version)
}

// Versions lists every version the build needs.
func (b Build) Versions() []string { return append([]string(nil), b.versions...) }

// Head is the last chain version (the last baseline version for an empty chain).
func (b Build) Head() string { return b.head }

// Compare classifies the applied versions. A version this build does not know is
// HeadsAhead before anything else (as pgmigrate.BuildHeads.CompareHeads does).
func (b Build) Compare(applied []string) pgmigrate.HeadVerdict {
	have := make(map[string]bool, len(applied))
	for _, version := range applied {
		have[version] = true
		if !b.known[version] {
			return pgmigrate.HeadsAhead
		}
	}
	for _, version := range b.versions {
		if !have[version] {
			return pgmigrate.HeadsBehind
		}
	}
	return pgmigrate.HeadsEqual
}

// BehindError is the refusal for a ClickHouse schema behind the build. Its text holds version names only.
type BehindError struct {
	Missing   []string
	BuildHead string
}

func (e *BehindError) Error() string {
	return fmt.Sprintf("the ClickHouse schema is behind this build: schema_migrations lacks %d version(s) %v, the build head is %s", len(e.Missing), e.Missing, e.BuildHead)
}

// ReadError is a failed read of schema_migrations, reduced to a class: it never carries the driver text.
type ReadError struct{ Class string }

func (e *ReadError) Error() string { return "read schema_migrations: " + e.Class }

// ConnApplied reads the applied versions through conn.
func ConnApplied(ctx context.Context, conn driver.Conn) ([]string, error) {
	if conn == nil {
		return nil, &ReadError{Class: "no_connection"}
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()
	rows, err := conn.Query(ctx, "SELECT version FROM "+chmigrate.SchemaMigrationsTable)
	if err != nil {
		return nil, &ReadError{Class: ReadClass(err)}
	}
	defer rows.Close()
	var versions []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, &ReadError{Class: ReadClass(err)}
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, &ReadError{Class: ReadClass(err)}
	}
	sort.Strings(versions)
	return versions, nil
}

// ReadClass reduces a read failure to a fixed class.
func ReadClass(err error) string {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		switch exception.Code {
		case 497:
			return "permission_denied"
		case 60, 81:
			return "table_missing"
		}
		return "database_error"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	return "unreachable"
}

// Enforce applies the gate to a process storage. A storage that cannot read its versions
// (a test fake, no Recorder) is not gated. It returns a *BehindError when the schema is
// behind this build, and nil in every other case.
func Enforce(ctx context.Context, logger *slog.Logger, process string, storage any) error {
	recorder, ok := storage.(Recorder)
	if !ok {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	build, err := LoadBuild()
	if err != nil {
		logger.WarnContext(ctx, "clickhouse schema gate skipped", "process", process, "class", "build_unavailable")
		return nil
	}
	return Check(ctx, logger, process, recorder, build)
}

// Check compares the applied versions with build.
func Check(ctx context.Context, logger *slog.Logger, process string, recorder Recorder, build Build) error {
	applied, err := recorder.AppliedVersions(ctx)
	if err != nil {
		class := "unreachable"
		var read *ReadError
		if errors.As(err, &read) {
			class = read.Class
		}
		logger.WarnContext(ctx, "clickhouse schema gate skipped", "process", process, "class", class)
		return nil
	}
	if len(applied) == 0 {
		logger.WarnContext(ctx, "clickhouse schema gate skipped", "process", process, "class", "no_versions_recorded")
		return nil
	}
	switch build.Compare(applied) {
	case pgmigrate.HeadsBehind:
		have := make(map[string]bool, len(applied))
		for _, version := range applied {
			have[version] = true
		}
		var missing []string
		for _, version := range build.versions {
			if !have[version] {
				missing = append(missing, version)
			}
		}
		refusal := &BehindError{Missing: missing, BuildHead: build.head}
		logger.ErrorContext(ctx, "clickhouse schema gate: refusing to start, the ClickHouse schema is behind this build; run the migration",
			"process", process, "missing_versions", refusal.Missing, "build_head", refusal.BuildHead)
		return refusal
	case pgmigrate.HeadsAhead:
		logger.WarnContext(ctx, "clickhouse schema gate: the ClickHouse schema is ahead of this build (rollback?); starting",
			"process", process, "build_head", build.head)
	}
	return nil
}
