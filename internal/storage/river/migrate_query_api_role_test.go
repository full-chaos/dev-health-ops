package riverstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func queryAPIOptions() MigrationOptions {
	options := apiOptions()
	options.QueryAPIRole = "dev_health_query_api"
	options.QueryAPIWriteGrants = []TableGrant{
		{TableName: "saved_reports", AllowInsert: true, AllowUpdate: true, AllowDelete: true},
		{TableName: "scheduled_jobs", AllowInsert: true, AllowUpdate: true},
		{TableName: "report_runs", AllowInsert: true},
		{TableName: "worker_job_outbox", AllowInsert: true},
	}
	return options
}

func TestMigrationOptionsValidateTheQueryAPIRoleLeg(t *testing.T) {
	t.Parallel()
	if err := ValidateMigrationOptions(queryAPIOptions()); err != nil {
		t.Fatalf("valid query-api leg refused: %v", err)
	}
	cases := map[string]func(*MigrationOptions){
		"grants without role":       func(o *MigrationOptions) { o.QueryAPIRole = "" },
		"role without grants":       func(o *MigrationOptions) { o.QueryAPIWriteGrants = nil },
		"collides with domain":      func(o *MigrationOptions) { o.QueryAPIRole = o.DomainRole },
		"collides with queue":       func(o *MigrationOptions) { o.QueryAPIRole = o.QueueRole },
		"collides with coordinator": func(o *MigrationOptions) { o.QueryAPIRole = o.CoordinatorRole },
		"collides with api":         func(o *MigrationOptions) { o.QueryAPIRole = o.APIRole },
		"unsafe role":               func(o *MigrationOptions) { o.QueryAPIRole = "q; DROP SCHEMA public" },
		"unsafe table": func(o *MigrationOptions) {
			o.QueryAPIWriteGrants = []TableGrant{{TableName: "saved_reports; DROP TABLE x", AllowInsert: true}}
		},
		"duplicate table": func(o *MigrationOptions) {
			o.QueryAPIWriteGrants = []TableGrant{{TableName: "saved_reports"}, {TableName: "saved_reports"}}
		},
	}
	for name, mutate := range cases {
		options := queryAPIOptions()
		mutate(&options)
		if err := ValidateMigrationOptions(options); !errors.Is(err, ErrMigrationConfiguration) {
			t.Errorf("%s: error %v, want ErrMigrationConfiguration", name, err)
		}
	}
}

func TestQueryAPIGrantStatementsAreAdditiveOnly(t *testing.T) {
	t.Parallel()
	none := queryAPIOptions()
	none.QueryAPIRole, none.QueryAPIWriteGrants = "", nil
	if statements := queryAPIGrantStatements(none); statements != nil {
		t.Fatalf("query-api statements without a role: %v", statements)
	}

	statements := queryAPIGrantStatements(queryAPIOptions())
	want := []string{
		`DO $$ BEGIN IF to_regclass('public.saved_reports') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE "public"."saved_reports" TO "dev_health_query_api"; END IF; END $$`,
		`DO $$ BEGIN IF to_regclass('public.scheduled_jobs') IS NOT NULL THEN GRANT SELECT, INSERT, UPDATE ON TABLE "public"."scheduled_jobs" TO "dev_health_query_api"; END IF; END $$`,
		`DO $$ BEGIN IF to_regclass('public.report_runs') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE "public"."report_runs" TO "dev_health_query_api"; END IF; END $$`,
		`DO $$ BEGIN IF to_regclass('public.worker_job_outbox') IS NOT NULL THEN GRANT SELECT, INSERT ON TABLE "public"."worker_job_outbox" TO "dev_health_query_api"; END IF; END $$`,
	}
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("statements:\n%s\nwant:\n%s", strings.Join(statements, "\n"), strings.Join(want, "\n"))
	}
	// The whole point of the additive leg: no statement removes a privilege,
	// so a role that already reads the whole query plane keeps reading it.
	for _, statement := range statements {
		upper := strings.ToUpper(statement)
		if strings.Contains(upper, "REVOKE") || !strings.Contains(upper, "GRANT") {
			t.Fatalf("a query-api statement is not a plain GRANT: %s", statement)
		}
	}
}

func TestQueryAPILegRidesLastAndChangesNothingWhenAbsent(t *testing.T) {
	t.Parallel()
	with := queryAPIOptions()
	without := queryAPIOptions()
	without.QueryAPIRole, without.QueryAPIWriteGrants = "", nil

	base := runtimeGrantStatements(without)
	all := runtimeGrantStatements(with)
	if !reflect.DeepEqual(all[:len(base)], base) {
		t.Fatal("configuring the query-api role changed a statement of another role's policy")
	}
	if !reflect.DeepEqual(all[len(base):], queryAPIGrantStatements(with)) {
		t.Fatal("the query-api leg is not the trailing block of the runtime grant statements")
	}
	// An unconfigured deployment gets byte-identical statements to the
	// pre-existing api-only options.
	if !reflect.DeepEqual(base, runtimeGrantStatements(apiOptions())) {
		t.Fatal("options without a query-api role differ from the pre-existing statements")
	}
}

func TestResolveQueryAPIRoleDecisionTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		role     string
		exists   bool
		wantRole string
	}{
		{name: "no role configured", role: "", exists: false, wantRole: ""},
		{name: "role absent: skipped", role: "dev_health_query_api", exists: false, wantRole: ""},
		{name: "role present", role: "dev_health_query_api", exists: true, wantRole: "dev_health_query_api"},
	}
	for _, test := range cases {
		options := queryAPIOptions()
		options.QueryAPIRole = test.role
		if test.role == "" {
			options.QueryAPIWriteGrants = nil
		}
		got := resolveQueryAPIRole(context.Background(), options, test.exists)
		if got.QueryAPIRole != test.wantRole {
			t.Errorf("%s: role %q, want %q", test.name, got.QueryAPIRole, test.wantRole)
		}
		if got.QueryAPIRole == "" && (got.QueryAPIWriteGrants != nil || queryAPIGrantStatements(got) != nil) {
			t.Errorf("%s: a skipped role still carries grants", test.name)
		}
		if err := ValidateMigrationOptions(got); err != nil {
			t.Errorf("%s: resolved options no longer validate: %v", test.name, err)
		}
	}
}
