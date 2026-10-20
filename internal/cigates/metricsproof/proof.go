// Package metricsproof is the executed-proof readback for the metrics.daily families.
package metricsproof

import (
	"context"
	"io"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

type Opener func(ctx context.Context, uri string) (Conn, func() error, error)

type FamilyRowCount struct {
	Rows             uint64
	LatestComputedAt time.Time
}

type IDSet map[string]struct{}

var (
	RepoDayFamilies           = map[string]string{}
	TeamDayFamilies           = map[string]string{}
	ScopeIDRepoFamilies       = map[string]string{}
	ScopeIDTeamFamilies       = map[string]string{}
	ScopeKeyFamilies          = map[string]string{}
	SynthesizedRepoIDFamilies = map[string][2]string{}
)

func SynthesizedRepoID(orgID, identityID string) string { return "" }

func LiveRepoIDs(ctx context.Context, c Conn, orgID string) (IDSet, error) { return nil, nil }

func FamilyReadback(ctx context.Context, c Conn, table, orgID string, repoIDs IDSet, runStart time.Time) (map[string]FamilyRowCount, error) {
	return nil, nil
}

func UnscopedRepoIDs(ctx context.Context, c Conn, table string, runStart time.Time) (IDSet, error) {
	return nil, nil
}

func TeamReadback(ctx context.Context, c Conn, table, orgID string, runStart time.Time) (map[string]FamilyRowCount, error) {
	return nil, nil
}

func ScopeKeyRepoReadback(ctx context.Context, c Conn, table, orgID string, repoIDs IDSet, runStart time.Time) (map[string]FamilyRowCount, error) {
	return nil, nil
}

func ScopeIDRepoReadback(ctx context.Context, c Conn, table, orgID string, repoIDs IDSet, runStart time.Time) (map[string]FamilyRowCount, error) {
	return nil, nil
}

func ScopeIDTeamReadback(ctx context.Context, c Conn, table, orgID string, runStart time.Time) (map[string]FamilyRowCount, error) {
	return nil, nil
}

func UnscopedScopeIDs(ctx context.Context, c Conn, table string, runStart time.Time) (IDSet, error) {
	return nil, nil
}

func SynthesizedRepoReadback(ctx context.Context, c Conn, table, identityColumn, orgID string, repoIDs IDSet, runStart time.Time) (uint64, IDSet, error) {
	return 0, nil, nil
}

func Run(args []string, getenv func(string) string, open Opener, stdout, stderr io.Writer) int {
	return 99
}

func Known(family string) bool { return false }
