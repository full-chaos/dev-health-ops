package postgres

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"
)

// sqlLineComment matches a SQL "--" line comment. Comments are prose, never
// executed, so they have no business being scanned for mutating SQL at all —
// and English contractions in that prose ("PostgreSQL's", "worker's") plant
// unpaired apostrophes that would otherwise desync singleQuotedLiteral's
// pairing for everything after them. Stripping comments first removes that
// hazard at the source instead of requiring every future comment to avoid
// contractions.
var sqlLineComment = regexp.MustCompile(`--[^\n]*`)

// singleQuotedLiteral matches a SQL single-quoted string literal, applied
// only after sqlLineComment has removed comment text. Privilege types passed
// to has_table_privilege / has_column_privilege / etc — plain ("SELECT") or
// with the option modifier ("SELECT WITH GRANT OPTION") — are always
// literals, so stripping every one of them before the mutating-SQL scan
// below is correct by construction for any current or future privilege-type
// string, rather than relying on an argument about which specific word
// sequences can only appear inside a literal.
var singleQuotedLiteral = regexp.MustCompile(`'[^']*'`)

func TestDomainAuthorizationRejectsMissingOrUnavailablePool(t *testing.T) {
	t.Parallel()

	if err := CheckDomainAuthorization(context.Background(), nil, "domain_role", "river"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckDomainAuthorization(nil) error = %v", err)
	}
	if err := CheckDomainAuthorization(context.Background(), nil, "Domain-Bad", "river"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckDomainAuthorization(invalid role) error = %v", err)
	}

	const secret = "domain-readiness-secret"
	config := DefaultConfig("postgres://domain:" + secret + "@127.0.0.1:1/app")
	config.ConnectTimeout = time.Millisecond
	pool, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err = CheckDomainAuthorization(ctx, pool, "domain_role", "river")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckDomainAuthorization() error = %v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), config.URI) {
		t.Fatalf("authorization readiness exposed connection material: %v", err)
	}
}

func TestRolePostureQueryIsReadOnlyAndChecksExactPrivilegeBoundary(t *testing.T) {
	t.Parallel()

	upperQuery := strings.ToUpper(rolePostureQuery)
	mutatingScanQuery := sqlLineComment.ReplaceAllString(upperQuery, "")
	mutatingScanQuery = singleQuotedLiteral.ReplaceAllString(mutatingScanQuery, "")
	for _, forbidden := range []string{"INSERT INTO", "UPDATE ", "DELETE FROM", "CREATE ", "ALTER ", "DROP ", "GRANT ", "REVOKE "} {
		if strings.Contains(mutatingScanQuery, forbidden) {
			t.Fatalf("role posture query contains mutating SQL %q", forbidden)
		}
	}
	for _, required := range []string{
		"CURRENT_USER = $1",
		"ROLCANLOGIN",
		"ROLSUPER",
		"ROLCREATEDB",
		"ROLCREATEROLE",
		"ROLREPLICATION",
		"ROLBYPASSRLS",
		"HAS_SCHEMA_PRIVILEGE",
		"HAS_TABLE_PRIVILEGE",
		"HAS_ANY_COLUMN_PRIVILEGE",
		"HAS_SEQUENCE_PRIVILEGE",
		"HAS_DATABASE_PRIVILEGE",
		"PG_HAS_ROLE",
		"'MEMBER'",
		"'MAINTAIN'",
		"RIVER_SEQUENCES",
		"RIVER_FUNCTIONS",
		"PUBLIC_FUNCTIONS",
		"OTHER_PUBLIC_RELATIONS",
		"PUBLIC_SEQUENCES",
		"'SELECT'",
		"'INSERT'",
		"'UPDATE'",
		"'DELETE'",
		"'USAGE'",
		"'CREATE'",
		"'TEMPORARY'",
		"UNNEST($3::TEXT[], $4::BOOLEAN[], $5::BOOLEAN[], $9::BOOLEAN[])",
		"UNNEST($6::TEXT[], $7::TEXT[], $8::TEXT[])",
	} {
		if !strings.Contains(upperQuery, required) {
			t.Fatalf("role posture query omits %q", required)
		}
	}
}

// The table manifest moved out of rolePostureQuery's text and into
// domainPosture's Go-side data once the query became role-agnostic (Phase 2
// posture parameterization); this is the successor to the old
// "each table name appears in the query exactly once" scan, now checking the
// data the query is actually parameterized with rather than literal SQL
// text that no longer contains it.
func TestDomainPostureInventoriesEachOriginalTableExactlyOnce(t *testing.T) {
	t.Parallel()

	counts := map[string]int{}
	for _, table := range domainPosture().RequiredTables {
		counts[table.TableName]++
	}
	for _, table := range []string{
		"integrations",
		"integration_sources",
		"integration_datasets",
		"integration_credentials",
		"sync_runs",
		"sync_dispatch_transport_routes",
		"sync_run_units",
		"sync_watermarks",
		"sync_dispatch_outbox",
		"worker_job_outbox",
	} {
		if counts[table] != 1 {
			t.Fatalf("domain posture must inventory %q exactly once, got %d", table, counts[table])
		}
	}
}

func TestSyncDispatchOutboxPosturesKeepBothRolesLeastPrivilege(t *testing.T) {
	t.Parallel()

	find := func(t *testing.T, posture RolePosture) TablePrivilege {
		t.Helper()
		var matches []TablePrivilege
		for _, table := range posture.RequiredTables {
			if table.TableName == "sync_dispatch_outbox" {
				matches = append(matches, table)
			}
		}
		if len(matches) != 1 {
			t.Fatalf("sync_dispatch_outbox posture rows = %d, want exactly 1", len(matches))
		}
		return matches[0]
	}

	coordinator := find(t, coordinatorPosture())
	if !coordinator.AllowInsert || !coordinator.AllowUpdate || coordinator.AllowDelete {
		t.Errorf("coordinator sync_dispatch_outbox = %+v, want SELECT+INSERT+UPDATE without DELETE", coordinator)
	}

	// The coordinator fix must not widen or narrow the independently required
	// domain posture for provider-sync execution.
	domain := find(t, domainPosture())
	if !domain.AllowInsert || !domain.AllowUpdate || domain.AllowDelete {
		t.Errorf("domain sync_dispatch_outbox = %+v, want unchanged SELECT+INSERT+UPDATE without DELETE", domain)
	}
}

// unnest() does not error on mismatched array-parameter lengths; it silently
// NULL-pads the shorter ones instead (confirmed empirically against a live
// server — see CheckRolePosture's comment). This pins the Go-side guard that
// exists because the SQL layer cannot be relied on to catch this itself: a
// ragged set of lengths must be rejected loudly and distinctly from
// ErrUnavailable, since it signals a broken RolePosture construction, not an
// unready role.
func TestCheckRolePostureRejectsRaggedParallelArrays(t *testing.T) {
	t.Parallel()

	if err := validateParallelArrayLengths("test set", 3, 3, 3); err != nil {
		t.Fatalf("validateParallelArrayLengths(3, 3, 3) = %v, want nil", err)
	}
	if err := validateParallelArrayLengths("test set", 0); err != nil {
		t.Fatalf("validateParallelArrayLengths(0) = %v, want nil", err)
	}
	err := validateParallelArrayLengths("required table privileges", 3, 3, 2)
	if err == nil {
		t.Fatal("validateParallelArrayLengths(3, 3, 2) = nil, want a mismatch error")
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatalf(
			"validateParallelArrayLengths error must be distinct from ErrUnavailable so a construction bug "+
				"is never confused with an unready role, got %v", err,
		)
	}
	if !strings.Contains(err.Error(), "required table privileges") {
		t.Fatalf("validateParallelArrayLengths error = %q, want it to name the posture it was checking", err.Error())
	}
}
