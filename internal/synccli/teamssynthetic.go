package synccli

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/api/teamsidentity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/fixturesgen"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// syntheticTeamCount is providers/teams.py:574's fixed
// `generator.generate_teams(count=8)` -- the CLI verb never varies it (no
// --repo-name/--seed/--count flag exists on either plane's teams command).
const syntheticTeamCount = 8

// runSyntheticTeams is providers/teams.py's `sync_teams` synthetic branch
// (providers/teams.py:570-577), ported (CHAOS-7037). It generates the same
// 8 deterministic demo teams (fixturesgen.GenerateSyntheticTeams, oracle-
// verified against the real SyntheticDataGenerator.generate_teams in
// internal/fixturesgen/oracle_test.go) and writes each one through the
// same ClickHouse admin team store the org-admin team CRUD surface uses
// (teamsidentity.Store.CreateOrUpdateTeam, ClickHouseTeamAdminService's
// port) -- NOT Python's older generic `insert_teams` writer, which this
// port deliberately does not carry forward (see the file doc comment on
// PINNED DIVERGENCE below).
//
// PINNED DIVERGENCE (design ruling D2865): Python's synthetic branch calls
// `run_with_store(db_uri, db_type, _handler, org_id=None)` -- the written
// rows carry org_id="" unless SettingsService itself defaults one. dho's
// `sync teams` requires --org on every provider (synccli.go:180-184,
// unconditional), so this branch is no exception: the generated teams are
// always written under the caller's --org, never an empty/global scope.
// The oracle compares every GENERATOR field (id, name, members) -- it does
// not and cannot compare org_id, since Go always has one and Python's
// synthetic path never does.
func runSyntheticTeams(ctx context.Context, env cli.Env, d deps, orgID string, allowEmpty bool, dsn string) int {
	teams := fixturesgen.GenerateSyntheticTeams("", nil, syntheticTeamCount)

	conn, err := d.openStore(ctx, dsn)
	if err != nil {
		return writeError(env.Stderr, cli.ExitFailure, "clickhouse_unavailable", secrets.NewBoundary(dsn).Redact(err).Error())
	}
	defer func() { _ = conn.Close() }()
	store := teamsidentity.Store{Conn: conn}

	written := 0
	for _, team := range teams {
		members := append([]string(nil), team.Members...)
		if _, err := store.CreateOrUpdateTeam(ctx, orgID, teamsidentity.TeamWrite{
			TeamID:  team.ID,
			Name:    team.Name,
			Members: &members,
		}); err != nil {
			return writeError(env.Stderr, cli.ExitFailure, "sync_failed", secrets.NewBoundary(dsn).Redact(err).Error())
		}
		written++
	}

	if written == 0 && !allowEmpty {
		return writeError(env.Stderr, cli.ExitFailure, "empty_result",
			"No teams found/generated. Pass --allow-empty to exit successfully on an empty sync.")
	}

	logger := logging.NewJSON(env.Stderr, slog.LevelInfo)
	logger.Info("synthetic team catalog synced", "org_id", orgID, "teams", written)
	if _, err := fmt.Fprintf(env.Stdout, "provider=synthetic teams=%d\n", written); err != nil {
		return cli.ExitFailure
	}
	return cli.ExitOK
}
