// Package teamcreated carries teams.created_at from the version of a team a
// write replaces. teams is a ReplacingMergeTree(updated_at): the newest
// version wins, so the creation time survives only if every writer copies it
// onto the version it inserts. Every Go writer of the teams table goes through this
// package or carries the column itself (see the census test).
package teamcreated

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ErrInvalidArguments is a nil connection or an empty org id.
var ErrInvalidArguments = errors.New("teamcreated: connection and org id are required")

// Column is the column name; Insert writers append it last.
const Column = "created_at"

// Query reads the oldest creation evidence of each team: the minimum, over
// every stored version, of coalesce(created_at, updated_at). It is not FINAL
// on purpose: unmerged old versions are evidence of an earlier time, and a
// FINAL read would hide them.
const Query = "SELECT id, min(coalesce(created_at, updated_at)) FROM teams " +
	"WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)} GROUP BY id"

// Querier is the one connection method Carry needs; driver.Conn and the
// narrower connection interfaces of the sinks satisfy it.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Carry returns the creation time to write for each team that already has a
// stored row. A team with no row has no entry: the caller stamps it with the
// time of its own first write (For). A query failure returns an error and the
// caller MUST abort its write before it prepares any INSERT.
func Carry(ctx context.Context, conn Querier, orgID string, teamIDs []string) (map[string]time.Time, error) {
	if isNil(conn) || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidArguments
	}
	if len(teamIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx, Query,
		clickhouse.Named("org_id", orgID), clickhouse.Named("team_ids", teamIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	existing := make(map[string]time.Time, len(teamIDs))
	for rows.Next() {
		var id string
		var created time.Time
		if err := rows.Scan(&id, &created); err != nil {
			return nil, err
		}
		existing[id] = created
	}
	return existing, rows.Err()
}

// For is the created_at to append for a team: the carried time, else
// firstWrite for a team that has no stored row yet, cut to the microseconds the column stores.
func For(carried map[string]time.Time, teamID string, firstWrite time.Time) time.Time {
	if created, ok := carried[teamID]; ok {
		return created
	}
	return firstWrite.Truncate(time.Microsecond)
}

func isNil(conn Querier) bool {
	if conn == nil {
		return true
	}
	v := reflect.ValueOf(conn)
	return v.Kind() == reflect.Ptr && v.IsNil()
}
