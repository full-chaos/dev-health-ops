// Package teamprovider carries teams.provider_team_id, teams.provider_name and
// teams.gone_at from the versions of a team that a write replaces. teams is a
// ReplacingMergeTree(updated_at): the newest version wins, so these columns
// survive only if every writer copies them onto the version it inserts. Every Go
// writer of the teams table writes through Insert, which reads the stored
// values inside its own INSERT..SELECT, or copies the columns itself in an
// INSERT..SELECT (see the census test). A read before a separate insert would
// erase a value another writer stored between the two (CHAOS-9999).
//
//   - provider_team_id and provider_name are the newest NON-EMPTY values over
//     every stored version: a writer that does not know the provider's id (an
//     admin edit, a push) never erases it.
//   - gone_at is the value of the newest version: a writer that lists the team
//     sets it NULL itself (the team is listed again); every other writer keeps it.
package teamprovider

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
var ErrInvalidArguments = errors.New("teamprovider: connection and org id are required")

// The three column names; Insert writers append them last, in this order.
const (
	ColumnProviderTeamID = "provider_team_id"
	ColumnProviderName   = "provider_name"
	ColumnGoneAt         = "gone_at"
)

// Columns is the column list fragment a statement that writes teams ends with.
const Columns = ColumnProviderTeamID + ", " + ColumnProviderName + ", " + ColumnGoneAt

// Query reads the three values of each team. It is not FINAL on purpose: the
// newest version is taken with argMax over every stored version, merged or not.
// argMax skips a NULL argument, so a NULL gone_at is read through a zero time:
// the newest version's NULL must win over an older version's time.
const Query = "SELECT id, " +
	"argMaxIf(provider_team_id, (updated_at, last_synced), provider_team_id != ''), " +
	"argMaxIf(provider_name, (updated_at, last_synced), provider_name != ''), " +
	"nullIf(argMax(ifNull(gone_at, toDateTime64(0, 6)), (updated_at, last_synced)), toDateTime64(0, 6)) " +
	"FROM teams WHERE org_id = {org_id:String} AND id IN {team_ids:Array(String)} GROUP BY id"

// Stored is what the stored versions of one team say.
type Stored struct {
	ProviderTeamID string
	ProviderName   string
	GoneAt         *time.Time
}

// Querier is the one connection method Carry needs; driver.Conn and the
// narrower connection interfaces of the sinks satisfy it.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

// Carry returns the stored values for each team that already has a row. A team
// with no row has no entry (the zero Stored: no id, no name, not gone). It is a
// read for telemetry (Tally): a write takes the values inside Insert.
func Carry(ctx context.Context, conn Querier, orgID string, teamIDs []string) (map[string]Stored, error) {
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
	stored := make(map[string]Stored, len(teamIDs))
	for rows.Next() {
		var id string
		var value Stored
		if err := rows.Scan(&id, &value.ProviderTeamID, &value.ProviderName, &value.GoneAt); err != nil {
			return nil, err
		}
		stored[id] = value
	}
	return stored, rows.Err()
}

func isNil(conn Querier) bool {
	if conn == nil {
		return true
	}
	v := reflect.ValueOf(conn)
	return v.Kind() == reflect.Ptr && v.IsNil()
}

// Tally counts, for one catalog write, how the provider's id of each listed
// team compares with the stored one. Counts only: no team id, key or name
// leaves the store through a log line.
type Tally struct {
	Filled    int // the provider id was empty and is now stored
	Unchanged int // the stored id equals the provider's
	Changed   int // the stored id differs: the key is now held by another team
	Unread    int // the run did not read the provider's id
}

// Add compares the provider's id for teamID with the stored one.
func (tally *Tally) Add(stored map[string]Stored, teamID, providerTeamID string) {
	providerTeamID = strings.TrimSpace(providerTeamID)
	previous := stored[teamID].ProviderTeamID
	switch {
	case providerTeamID == "":
		tally.Unread++
	case previous == "":
		tally.Filled++
	case previous == providerTeamID:
		tally.Unchanged++
	default:
		tally.Changed++
	}
}
