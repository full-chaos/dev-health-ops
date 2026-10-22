//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
)

// plantedPosture is one state the admin connection puts a runtime login in,
// and the statements that return it to the provisioned posture.
type plantedPosture struct {
	name    string
	plant   []string
	restore []string
}

func execAll(t *testing.T, ctx context.Context, admin *pgxpool.Pool, statements []string) {
	t.Helper()
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

// runPlantedPostures plants each posture in turn and requires the readiness
// check to refuse it, and to accept the login again once it is restored. When
// priorRevision is set it is also required to answer "authorized" for every
// planted posture: that is what makes these cases fail on a tree without the
// predicates under test, within this same run.
func runPlantedPostures(
	t *testing.T, ctx context.Context, admin *pgxpool.Pool, check func() error,
	priorRevision func() (bool, error), cases []plantedPosture,
) {
	t.Helper()
	if err := check(); err != nil {
		t.Fatalf("readiness rejected the grants the migration emitted: %v", err)
	}
	if priorRevision != nil {
		if answer, err := priorRevision(); err != nil || !answer {
			t.Fatalf("prior revision rejected the provisioned grants: answer=%v err=%v", answer, err)
		}
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			execAll(t, ctx, admin, test.plant)
			if priorRevision != nil {
				answer, err := priorRevision()
				if err != nil {
					t.Errorf("prior revision did not answer: %v", err)
				} else if !answer {
					t.Error("prior revision already refuses this posture, so the case does not show what the current predicates add")
				}
			}
			err := check()
			if !errors.Is(err, ErrUnavailable) || !errors.Is(err, ErrPostureRefused) {
				t.Errorf("readiness error = %v, want a refusal (ErrUnavailable wrapping ErrPostureRefused)", err)
			}
			execAll(t, ctx, admin, test.restore)
			if err := check(); err != nil {
				t.Errorf("readiness did not recover after restoring the provisioned posture: %v", err)
			}
		})
	}
}

func queuePlantedPostures(role, dbName string) []plantedPosture {
	allowCreate := "GRANT CREATE ON SCHEMA public TO " + role
	denyCreate := "REVOKE CREATE ON SCHEMA public FROM " + role
	return []plantedPosture{
		{
			name:    "owned schema with every privilege revoked",
			plant:   []string{"CREATE SCHEMA owned_probe AUTHORIZATION " + role, "REVOKE ALL ON SCHEMA owned_probe FROM " + role},
			restore: []string{"DROP SCHEMA owned_probe"},
		},
		{
			name: "owned table with every privilege revoked",
			plant: []string{
				allowCreate,
				"CREATE TABLE public.owned_probe_table (id integer)",
				"ALTER TABLE public.owned_probe_table OWNER TO " + role,
				denyCreate,
				"REVOKE ALL ON TABLE public.owned_probe_table FROM " + role,
			},
			restore: []string{"DROP TABLE public.owned_probe_table"},
		},
		{
			name: "owned sequence with every privilege revoked",
			plant: []string{
				allowCreate,
				"CREATE SEQUENCE public.owned_probe_sequence",
				"ALTER SEQUENCE public.owned_probe_sequence OWNER TO " + role,
				denyCreate,
				"REVOKE ALL ON SEQUENCE public.owned_probe_sequence FROM " + role,
			},
			restore: []string{"DROP SEQUENCE public.owned_probe_sequence"},
		},
		{
			name: "owned function with every privilege revoked",
			plant: []string{
				"CREATE FUNCTION public.owned_probe_function() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
				allowCreate,
				"ALTER FUNCTION public.owned_probe_function() OWNER TO " + role,
				denyCreate,
				"REVOKE ALL ON FUNCTION public.owned_probe_function() FROM PUBLIC, " + role,
			},
			restore: []string{"DROP FUNCTION public.owned_probe_function()"},
		},
		{
			name:    "SELECT with grant option on a required read-only table",
			plant:   []string{"GRANT SELECT ON TABLE public.sync_runs TO " + role + " WITH GRANT OPTION"},
			restore: []string{"REVOKE GRANT OPTION FOR SELECT ON TABLE public.sync_runs FROM " + role},
		},
		{
			name:    "UPDATE with grant option on a required read-write table",
			plant:   []string{"GRANT UPDATE ON TABLE public.worker_job_outbox TO " + role + " WITH GRANT OPTION"},
			restore: []string{"REVOKE GRANT OPTION FOR UPDATE ON TABLE public.worker_job_outbox FROM " + role},
		},
		{
			name:    "column SELECT with grant option on a required read-only table",
			plant:   []string{"GRANT SELECT (id) ON TABLE public.sync_runs TO " + role + " WITH GRANT OPTION"},
			restore: []string{"REVOKE GRANT OPTION FOR SELECT (id) ON TABLE public.sync_runs FROM " + role, "REVOKE SELECT (id) ON TABLE public.sync_runs FROM " + role},
		},
		{
			name: "SELECT with grant option on a River table",
			plant: []string{
				"CREATE TABLE " + grantSchema + ".posture_probe_table (id integer)",
				"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE " + grantSchema + ".posture_probe_table TO " + role,
				"GRANT SELECT ON TABLE " + grantSchema + ".posture_probe_table TO " + role + " WITH GRANT OPTION",
			},
			restore: []string{"DROP TABLE " + grantSchema + ".posture_probe_table"},
		},
		{
			name: "USAGE with grant option on a River sequence",
			plant: []string{
				"CREATE SEQUENCE " + grantSchema + ".posture_probe_sequence",
				"GRANT USAGE, SELECT, UPDATE ON SEQUENCE " + grantSchema + ".posture_probe_sequence TO " + role,
				"GRANT USAGE ON SEQUENCE " + grantSchema + ".posture_probe_sequence TO " + role + " WITH GRANT OPTION",
			},
			restore: []string{"DROP SEQUENCE " + grantSchema + ".posture_probe_sequence"},
		},
		{
			name: "EXECUTE with grant option on a River function",
			plant: []string{
				"CREATE FUNCTION " + grantSchema + ".posture_probe_function() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
				"REVOKE ALL ON FUNCTION " + grantSchema + ".posture_probe_function() FROM PUBLIC",
				"GRANT EXECUTE ON FUNCTION " + grantSchema + ".posture_probe_function() TO " + role + " WITH GRANT OPTION",
			},
			restore: []string{"DROP FUNCTION " + grantSchema + ".posture_probe_function()"},
		},
		{
			name:    "CONNECT with grant option on the database",
			plant:   []string{"GRANT CONNECT ON DATABASE " + dbName + " TO " + role + " WITH GRANT OPTION"},
			restore: []string{"REVOKE GRANT OPTION FOR CONNECT ON DATABASE " + dbName + " FROM " + role},
		},
		{
			name:    "USAGE with grant option on the public schema",
			plant:   []string{"GRANT USAGE ON SCHEMA public TO " + role + " WITH GRANT OPTION"},
			restore: []string{"REVOKE GRANT OPTION FOR USAGE ON SCHEMA public FROM " + role},
		},
		{
			name:    "USAGE with grant option on the River schema",
			plant:   []string{"GRANT USAGE ON SCHEMA " + grantSchema + " TO " + role + " WITH GRANT OPTION"},
			restore: []string{"REVOKE GRANT OPTION FOR USAGE ON SCHEMA " + grantSchema + " FROM " + role},
		},
	}
}

// Each planted posture is answered "authorized" by the prior revision of the
// queue query and refused by the current one, in this one run.
func TestQueueAuthorizationRefusesOwnershipAndGrantableHeldPrivileges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	queue := connectAs(t, ctx, uri, roles.queue, grantQueuePass)
	dbName, err := containers.DatabaseName(uri)
	if err != nil {
		t.Fatal(err)
	}

	runPlantedPostures(t, ctx, admin,
		func() error { return CheckQueueAuthorization(ctx, queue, roles.queue, grantSchema) },
		func() (bool, error) {
			return queryPostureAnswer(ctx, queue, rolePostureStatementTimeout, queueAuthorizationQueryPriorRevision, roles.queue, grantSchema)
		},
		queuePlantedPostures(roles.queue, dbName),
	)
}
