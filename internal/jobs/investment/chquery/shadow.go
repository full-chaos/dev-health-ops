package chquery

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ErrShadowTableMissing marks a shadow read that failed because
// work_unit_investment_shadow does not exist (migration 109 not applied). The
// shadow phase stops on it; it is never a run failure.
var ErrShadowTableMissing = errors.New("chquery: work_unit_investment_shadow is missing (migration 109 not applied)")

// clickHouseUnknownTable is ClickHouse server error code 60, UNKNOWN_TABLE.
const clickHouseUnknownTable = 60

// shadowTerminalStates are the states of a shadow row that answer the unit: a
// later run with the same input and the same configuration does not ask again.
// Every other state (a failed request, a missing or invalid answer, a defect)
// is asked again.
var shadowTerminalStates = []string{"ok", "zero_support"}

// FetchExistingShadowKeys returns the (work unit, input hash) pairs among keys
// that already have a shadow row of the configuration shadowConfig whose LATEST
// state is terminal. It is the shadow phase's own skip-existing; it reads
// work_unit_investment_shadow only and has no part in the served skip-existing.
//
// The status filter is OUTSIDE the argMax, as in FetchExistingInvestmentKeys: a
// stale ok row must not outrank a newer failed one.
func (reader *Reader) FetchExistingShadowKeys(
	ctx context.Context, organizationID string, keys []InvestmentKey, shadowConfig string,
) (map[InvestmentKey]struct{}, error) {
	if reader == nil || reader.conn == nil {
		return nil, ErrUnavailable
	}
	if organizationID == "" {
		return nil, fmt.Errorf("FetchExistingShadowKeys: %w", ErrOrganizationIDRequired)
	}
	workUnitIDs := make([]string, 0, len(keys))
	inputHashes := make([]string, 0, len(keys))
	seenUnit := make(map[string]struct{}, len(keys))
	seenHash := make(map[string]struct{}, len(keys))
	wanted := make(map[InvestmentKey]struct{}, len(keys))
	for _, key := range keys {
		wanted[key] = struct{}{}
		if _, ok := seenUnit[key.WorkUnitID]; !ok {
			seenUnit[key.WorkUnitID] = struct{}{}
			workUnitIDs = append(workUnitIDs, key.WorkUnitID)
		}
		if _, ok := seenHash[key.InputHash]; !ok {
			seenHash[key.InputHash] = struct{}{}
			inputHashes = append(inputHashes, key.InputHash)
		}
	}
	if len(wanted) == 0 {
		return map[InvestmentKey]struct{}{}, nil
	}
	sort.Strings(workUnitIDs)
	sort.Strings(inputHashes)

	rows, err := reader.conn.Query(ctx, `
        SELECT work_unit_id, categorization_input_hash
        FROM (
            SELECT
                work_unit_id,
                categorization_input_hash,
                argMax(state, computed_at) AS latest_state
            FROM work_unit_investment_shadow
            WHERE org_id = {org_id:String}
              AND work_unit_id IN {work_unit_ids:Array(String)}
              AND categorization_input_hash IN {input_hashes:Array(String)}
              AND shadow_config = {shadow_config:String}
            GROUP BY work_unit_id, categorization_input_hash
        )
        WHERE latest_state IN {terminal_states:Array(String)}
    `,
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("work_unit_ids", workUnitIDs),
		clickhouse.Named("input_hashes", inputHashes),
		clickhouse.Named("shadow_config", shadowConfig),
		clickhouse.Named("terminal_states", shadowTerminalStates),
	)
	if err != nil {
		return nil, shadowReadError("query existing shadow keys", err)
	}
	defer func() { _ = rows.Close() }()

	existing := make(map[InvestmentKey]struct{}, len(wanted))
	for rows.Next() {
		var key InvestmentKey
		if err := rows.Scan(&key.WorkUnitID, &key.InputHash); err != nil {
			return nil, fmt.Errorf("scan existing shadow key: %w", err)
		}
		// The query asks for the cross product of the two id sets; keep only
		// the pairs that were asked for.
		if _, ok := wanted[key]; ok {
			existing[key] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, shadowReadError("read existing shadow keys", err)
	}
	return existing, nil
}

func shadowReadError(what string, err error) error {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) && exception.Code == clickHouseUnknownTable {
		return fmt.Errorf("%s: %w: %w", what, ErrShadowTableMissing, err)
	}
	return fmt.Errorf("%s: %w", what, err)
}
