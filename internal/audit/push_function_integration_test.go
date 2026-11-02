//go:build integration

package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	fnUUID1 = "00000000-0000-4000-8000-0000000000e1"
	fnUUID2 = "00000000-0000-4000-8000-0000000000e2"
)

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func callFunction(ctx context.Context, tx pgx.Tx, eventType, id, payload string, snapshot any) (int64, error) {
	var revision int64
	err := tx.QueryRow(ctx, `SELECT auth.append_external_push_lifecycle_event($1, $2, $3::jsonb, $4::jsonb)`, eventType, id, payload, snapshot).Scan(&revision)
	return revision, err
}

const goodPayload = `{"organization_id":"org_EXAMPLE0000000000000001","binding":"source","source_id":"src_EXAMPLE0000000000000001","change":{"status":"active","fingerprint":"0123456789abcdef","scopes":["ingest:write"]}}`

// TestPushFunctionIsTheCallersOnlyDoorAndRefusesEverythingElse runs as the
// go-api-like login, not as an owner.
func TestPushFunctionIsTheCallersOnlyDoorAndRefusesEverythingElse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPushFixture(t, ctx)

	t.Run("a direct write to the outbox or the revision table is refused", func(t *testing.T) {
		for _, sql := range []string{
			`INSERT INTO auth.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key) VALUES ('organization', 'org_x', 'organization.created', '{}', 'organization:org_x:1')`,
			`INSERT INTO auth.aggregate_revisions (aggregate_type, aggregate_id, revision) VALUES ('organization', 'org_x', 1)`,
			`UPDATE auth.aggregate_revisions SET revision = revision + 1`,
			`SELECT count(*) FROM auth.auth_outbox_events`,
			`DELETE FROM auth.auth_outbox_events`,
			`SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true), 1`,
		} {
			tx, err := env.caller.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, sql)
			_ = tx.Rollback(ctx)
			if strings.HasPrefix(sql, "SELECT set_config") {
				continue
			}
			if pgCode(err) != "42501" {
				t.Errorf("%s: err = %v, want permission denied (42501)", sql[:40], err)
			}
		}
	})

	t.Run("a setting the caller arms itself still lets it write nothing", func(t *testing.T) {
		tx, err := env.caller.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SELECT set_config('dev_health.audit_txid', pg_current_xact_id()::text, true)`); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO auth.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key) VALUES ('organization', 'org_x', 'organization.created', '{}', 'organization:org_x:1')`)
		if pgCode(err) != "42501" {
			t.Fatalf("armed caller insert: err = %v, want 42501", err)
		}
	})

	refusals := map[string]struct {
		eventType, id, payload string
		snapshot               any
	}{
		"a provisioning id shape":                    {"external_push_credential.created", "org_abc", goodPayload, nil},
		"an uppercase uuid":                          {"external_push_credential.created", strings.ToUpper(fnUUID1), goodPayload, nil},
		"an event of another aggregate":              {"organization.created", fnUUID1, goodPayload, nil},
		"an unknown verb":                            {"external_push_credential.deleted", fnUUID1, goodPayload, nil},
		"a payload with a revision":                  {"external_push_credential.created", fnUUID1, `{"revision":9,"organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"active"}}`, nil},
		"a payload with a key":                       {"external_push_credential.created", fnUUID1, `{"idempotency_key":"k","organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"active"}}`, nil},
		"a payload secret member":                    {"external_push_credential.created", fnUUID1, `{"secret_hash":"x","organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"active"}}`, nil},
		"a change secret member":                     {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"active","secret_hash":"x"}}`, nil},
		"a change token member":                      {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"active","token":"x"}}`, nil},
		"a payload that is a list":                   {"external_push_credential.created", fnUUID1, `[]`, nil},
		"no binding":                                 {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","source_id":"s","change":{"status":"active"}}`, nil},
		"an unknown binding":                         {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","binding":"tenant","source_id":"s","change":{"status":"active"}}`, nil},
		"a null binding":                             {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","binding":null,"source_id":"s","change":{"status":"active"}}`, nil},
		"a source binding without one":               {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","binding":"source","change":{"status":"active"}}`, nil},
		"an organization binding with a source":      {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","binding":"organization","source_id":"s","change":{"status":"active"}}`, nil},
		"an organization binding with a null source": {"external_push_credential.created", fnUUID1, `{"organization_id":"org_x","binding":"organization","source_id":null,"change":{"status":"active"}}`, nil},
		"an optional envelope member":                {"external_push_credential.created", fnUUID1, `{"actor_principal_id":"prn_x","organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"active"}}`, nil},
		"a revoked without a snapshot":               {"external_push_credential.revoked", fnUUID1, `{"organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"revoked"}}`, nil},
		"a snapshot secret member":                   {"external_push_credential.revoked", fnUUID1, `{"organization_id":"org_x","binding":"source","source_id":"s","change":{"status":"revoked"}}`, `{"status":"active","secret_hash":"x"}`},
	}
	for name, tc := range refusals {
		t.Run("refuses "+name, func(t *testing.T) {
			err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
				_, err := callFunction(ctx, tx, tc.eventType, tc.id, tc.payload, tc.snapshot)
				return err
			})
			if err == nil {
				t.Fatal("accepted")
			}
			var rows, revisions int
			if err := env.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'external_push_credential'),
				(SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_type = 'external_push_credential')`).Scan(&rows, &revisions); err != nil {
				t.Fatal(err)
			}
			if rows != 0 || revisions != 0 {
				t.Fatalf("a refused call left %d outbox rows and %d revision rows", rows, revisions)
			}
		})
	}

	t.Run("a second created for one id is refused and the caller cannot pick the revision", func(t *testing.T) {
		if err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
			revision, err := callFunction(ctx, tx, "external_push_credential.created", fnUUID1, goodPayload, nil)
			if err == nil && revision != 1 {
				err = fmt.Errorf("first revision = %d", revision)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
			_, err := callFunction(ctx, tx, "external_push_credential.created", fnUUID1, goodPayload, nil)
			return err
		})
		if pgCode(err) != "55000" {
			t.Fatalf("second created: err = %v, want 55000", err)
		}
		var key string
		var revision int64
		if err := env.pool.QueryRow(ctx, `SELECT idempotency_key, (payload->>'revision')::bigint FROM auth.auth_outbox_events WHERE aggregate_id = $1`, fnUUID1).Scan(&key, &revision); err != nil {
			t.Fatal(err)
		}
		if key != "external_push_credential:"+fnUUID1+":1" || revision != 1 {
			t.Fatalf("key %q revision %d", key, revision)
		}
	})

	t.Run("the commit guard is disarmed when the function returns", func(t *testing.T) {
		var setting *string
		if err := withPushTx(t, ctx, env, false, func(tx pgx.Tx) error {
			if _, err := callFunction(ctx, tx, "external_push_credential.created", fnUUID2, goodPayload, nil); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `SELECT current_setting('dev_health.audit_txid', true)`).Scan(&setting)
		}); err != nil {
			t.Fatal(err)
		}
		if setting != nil && *setting != "" {
			t.Fatalf("dev_health.audit_txid = %q after the function returned", *setting)
		}
	})
}

// TestPushWriterHoldsExactlyTheDeclaredSet reads the ACLs the migration left.
func TestPushWriterHoldsExactlyTheDeclaredSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPushFixture(t, ctx)
	q := func(sql string, args ...any) bool {
		var ok bool
		if err := env.pool.QueryRow(ctx, sql, args...).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	fn := `'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure`
	if !q(`SELECT (SELECT rolname FROM pg_roles WHERE oid = proowner) = $1 FROM pg_proc WHERE oid = `+fn, env.writerRole) {
		t.Error("the function is not owned by the writer role")
	}
	if q(`SELECT has_schema_privilege($1, 'auth', 'CREATE')`, env.writerRole) {
		t.Error("the writer role keeps CREATE on the auth schema after the migration")
	}
	if q(`SELECT nspacl::text LIKE '%' || $1 || '=C%' OR nspacl::text LIKE '%' || $1 || '=UC%' FROM pg_namespace WHERE nspname = 'auth'`, env.writerRole) {
		t.Error("the auth schema ACL still shows CREATE for the writer role")
	}
	if q(`SELECT EXISTS (SELECT 1 FROM pg_proc p, aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a WHERE p.oid = ` + fn + ` AND a.grantee = 0)`) {
		t.Error("PUBLIC can execute the function")
	}
	if !q(`SELECT has_function_privilege($1, `+fn+`, 'EXECUTE')`, env.callerRole) {
		t.Error("the caller cannot execute the function")
	}
	var runtime string
	if err := env.pool.QueryRow(ctx, `SELECT rolname FROM pg_roles WHERE rolname LIKE 'audit\_runtime%'`).Scan(&runtime); err != nil {
		t.Fatal(err)
	}
	if q(`SELECT has_function_privilege($1, `+fn+`, 'EXECUTE')`, runtime) {
		t.Error("the runtime role can execute the function")
	}
	for _, table := range []string{"principals", "security_audit_events", "credential_registry", "sessions"} {
		if q(`SELECT has_table_privilege($1, $2, 'SELECT')`, env.writerRole, "auth."+table) {
			t.Errorf("the writer role can read auth.%s", table)
		}
	}
	for _, table := range []string{"auth_outbox_events", "aggregate_revisions"} {
		if q(`SELECT has_table_privilege($1, $2, 'DELETE') OR has_table_privilege($1, $2, 'TRUNCATE')`, env.writerRole, "auth."+table) {
			t.Errorf("the writer role can delete from auth.%s", table)
		}
		if q(`SELECT has_table_privilege($1, $2, 'SELECT') AND $2 = 'auth.auth_outbox_events'`, env.writerRole, "auth."+table) {
			t.Errorf("the writer role can read auth.%s", table)
		}
	}
	for _, table := range []string{"auth_outbox_events", "aggregate_revisions"} {
		if q(`SELECT has_table_privilege($1, $2, 'INSERT')`, env.callerRole, "auth."+table) {
			t.Errorf("the caller role can insert into auth.%s", table)
		}
	}

	t.Run("a bare uuid is refused as the id of any other aggregate", func(t *testing.T) {
		tx, err := env.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := armTx(ctx, tx); err != nil {
			t.Fatal(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO auth.aggregate_revisions (aggregate_type, aggregate_id, revision) VALUES ('organization', $1, 1)`, fnUUID1)
		if pgCode(err) != "23514" {
			t.Fatalf("a bare uuid for an organization: err = %v, want a CHECK violation", err)
		}
	})

	t.Run("a changed owner is handed back to the writer by the next run", func(t *testing.T) {
		fn := `'auth.append_external_push_lifecycle_event(text,text,jsonb,jsonb)'::regprocedure`
		if _, err := env.pool.Exec(ctx, `ALTER FUNCTION auth.append_external_push_lifecycle_event(text, text, jsonb, jsonb) OWNER TO `+authschema.Quote(mustIdent(t, runtime))); err != nil {
			t.Fatal(err)
		}
		if q(`SELECT (SELECT rolname FROM pg_roles WHERE oid = proowner) = $1 FROM pg_proc WHERE oid = `+fn, env.writerRole) {
			t.Fatal("the mutation did not change the owner")
		}
		if _, err := authschema.Apply(ctx, env.pool, authschema.Options{
			Schema: "auth", RuntimeRole: runtime, SessionReaderRoles: []string{env.callerRole},
			PushLifecycleWriterRole: env.writerRole, PushLifecycleCallerRoles: []string{env.callerRole},
		}); err != nil {
			t.Fatalf("Apply after an owner change: %v", err)
		}
		if !q(`SELECT (SELECT rolname FROM pg_roles WHERE oid = proowner) = $1 FROM pg_proc WHERE oid = `+fn, env.writerRole) {
			t.Fatal("the next run did not hand the function back to the writer role")
		}
	})

	t.Run("a grant outside the auth schema fails the posture check", func(t *testing.T) {
		if _, err := env.pool.Exec(ctx, `CREATE TABLE public.push_writer_drift (id int)`); err != nil {
			t.Fatal(err)
		}
		if _, err := env.pool.Exec(ctx, `GRANT SELECT ON public.push_writer_drift TO `+authschema.Quote(mustIdent(t, env.writerRole))); err != nil {
			t.Fatal(err)
		}
		_, err := authschema.Apply(ctx, env.pool, authschema.Options{
			Schema: "auth", RuntimeRole: runtime, SessionReaderRoles: []string{env.callerRole},
			PushLifecycleWriterRole: env.writerRole, PushLifecycleCallerRoles: []string{env.callerRole},
		})
		if err == nil || !strings.Contains(err.Error(), "push_writer_drift") {
			t.Fatalf("a drifted grant to the writer: err = %v, want a posture violation naming the table", err)
		}
	})

	t.Run("a role membership of the writer fails the posture check", func(t *testing.T) {
		if _, err := env.pool.Exec(ctx, `GRANT `+authschema.Quote(mustIdent(t, env.callerRole))+` TO `+authschema.Quote(mustIdent(t, env.writerRole))); err != nil {
			t.Fatal(err)
		}
		_, err := authschema.Apply(ctx, env.pool, authschema.Options{
			Schema: "auth", RuntimeRole: runtime, SessionReaderRoles: []string{env.callerRole},
			PushLifecycleWriterRole: env.writerRole, PushLifecycleCallerRoles: []string{env.callerRole},
		})
		if err == nil || !strings.Contains(err.Error(), "member of") {
			t.Fatalf("a membership of the writer: err = %v, want a posture violation", err)
		}
	})

	t.Run("a grant to the writer inside the auth schema is revoked by the next run", func(t *testing.T) {
		if _, err := env.pool.Exec(ctx, `GRANT SELECT ON auth.principals TO `+authschema.Quote(mustIdent(t, env.writerRole))); err != nil {
			t.Fatal(err)
		}
		_, _ = authschema.Apply(ctx, env.pool, authschema.Options{
			Schema: "auth", RuntimeRole: runtime, SessionReaderRoles: []string{env.callerRole},
			PushLifecycleWriterRole: env.writerRole, PushLifecycleCallerRoles: []string{env.callerRole},
		})
		if q(`SELECT has_table_privilege($1, 'auth.principals', 'SELECT')`, env.writerRole) {
			t.Error("the writer still reads auth.principals after a grants run")
		}
	})
}

func mustIdent(t *testing.T, name string) authschema.ValidatedIdentifier {
	t.Helper()
	id, err := authschema.NewValidatedIdentifier(name)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestPushFunctionRowsEqualTheGoFormOfTheSameInputs: the function and the Go
// helpers audit.Commit writes with must give the same outbox and revision
// rows for the same inputs, apart from the row id, the clock and the aggregate
// id. It fails when either side changes a column, a default or the key format.
func TestPushFunctionRowsEqualTheGoFormOfTheSameInputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPushFixture(t, ctx)
	q := authschema.Quote(env.schema)

	goTx, err := env.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := armTx(ctx, goTx); err != nil {
		t.Fatal(err)
	}
	revision, err := bumpRevision(ctx, goTx, q, "external_push_credential", fnUUID1)
	if err != nil {
		t.Fatal(err)
	}
	payload := `{"revision":` + fmt.Sprint(revision) + `,"organization_id":"org_EXAMPLE0000000000000001","binding":"source","source_id":"src_EXAMPLE0000000000000001","change":{"status":"active","fingerprint":"0123456789abcdef","scopes":["ingest:write"]}}`
	if err := insertOutboxRow(ctx, goTx, q, "external_push_credential", fnUUID1, "external_push_credential.created", revision, []byte(payload)); err != nil {
		t.Fatal(err)
	}
	if err := goTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
		_, err := callFunction(ctx, tx, "external_push_credential.created", fnUUID2, goodPayload, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	columns, err := env.pool.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = 'auth' AND table_name = 'auth_outbox_events' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for columns.Next() {
		var name string
		if err := columns.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	columns.Close()
	normalised := map[string]bool{"id": true, "aggregate_id": true, "idempotency_key": true, "created_at": true, "available_at": true, "payload": true}
	for _, name := range names {
		if normalised[name] {
			continue
		}
		var a, b *string
		if err := env.pool.QueryRow(ctx, fmt.Sprintf(`SELECT (SELECT %[1]s::text FROM auth.auth_outbox_events WHERE aggregate_id = $1),
			(SELECT %[1]s::text FROM auth.auth_outbox_events WHERE aggregate_id = $2)`, name), fnUUID1, fnUUID2).Scan(&a, &b); err != nil {
			t.Fatal(err)
		}
		if (a == nil) != (b == nil) || (a != nil && *a != *b) {
			t.Errorf("column %s differs: Go form %v, function %v", name, deref(a), deref(b))
		}
	}
	var same bool
	if err := env.pool.QueryRow(ctx, `SELECT
		(SELECT payload FROM auth.auth_outbox_events WHERE aggregate_id = $1) = (SELECT payload FROM auth.auth_outbox_events WHERE aggregate_id = $2)
		AND (SELECT replace(idempotency_key, $1, 'ID') FROM auth.auth_outbox_events WHERE aggregate_id = $1) = (SELECT replace(idempotency_key, $2, 'ID') FROM auth.auth_outbox_events WHERE aggregate_id = $2)
		AND (SELECT available_at = created_at FROM auth.auth_outbox_events WHERE aggregate_id = $1) = (SELECT available_at = created_at FROM auth.auth_outbox_events WHERE aggregate_id = $2)
		AND (SELECT revision FROM auth.aggregate_revisions WHERE aggregate_id = $1) = (SELECT revision FROM auth.aggregate_revisions WHERE aggregate_id = $2)`,
		fnUUID1, fnUUID2).Scan(&same); err != nil {
		t.Fatal(err)
	}
	if !same {
		t.Error("payload, key shape, available_at default or revision differ between the Go form and the function")
	}
}

func deref(s *string) string {
	if s == nil {
		return "NULL"
	}
	return *s
}

const orgWidePayload = `{"organization_id":"org_EXAMPLE0000000000000001","binding":"organization","change":{"status":"active","fingerprint":"0123456789abcdef","scopes":["ingest:status"]}}`

type storedPush struct {
	eventType string
	payload   string
}

func storedPushEvents(t *testing.T, ctx context.Context, env *pushFixture, id string) []storedPush {
	t.Helper()
	rows, err := env.pool.Query(ctx, `SELECT event_type, payload::text FROM auth.auth_outbox_events
		WHERE aggregate_type = 'external_push_credential' AND aggregate_id = $1 ORDER BY id`, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []storedPush
	for rows.Next() {
		var row storedPush
		if err := rows.Scan(&row.eventType, &row.payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	return out
}

// TestPushFunctionStoresTheBindingAndNoPlaceholderSource: an organization-wide
// credential is stored with binding organization and no source_id member, and a
// source-bound one with both.
func TestPushFunctionStoresTheBindingAndNoPlaceholderSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPushFixture(t, ctx)

	call := func(eventType, id, payload string, snapshot any) {
		t.Helper()
		if err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
			_, err := callFunction(ctx, tx, eventType, id, payload, snapshot)
			return err
		}); err != nil {
			t.Fatalf("%s: %v", eventType, err)
		}
	}
	call("external_push_credential.created", fnUUID1, orgWidePayload, nil)
	call("external_push_credential.created", fnUUID2, goodPayload, nil)
	for id, want := range map[string]struct{ binding, source string }{fnUUID1: {"organization", ""}, fnUUID2: {"source", "src_EXAMPLE0000000000000001"}} {
		rows := storedPushEvents(t, ctx, env, id)
		if len(rows) != 1 {
			t.Fatalf("%s: %d rows, want 1", id, len(rows))
		}
		var got struct {
			Binding  string  `json:"binding"`
			SourceID *string `json:"source_id"`
		}
		if err := json.Unmarshal([]byte(rows[0].payload), &got); err != nil {
			t.Fatal(err)
		}
		if got.Binding != want.binding || (want.source == "") != (got.SourceID == nil) || (got.SourceID != nil && *got.SourceID != want.source) {
			t.Errorf("%s: stored payload %s, want binding %q source %q", id, rows[0].payload, want.binding, want.source)
		}
		if strings.Contains(rows[0].payload, `"source_id": null`) || strings.Contains(rows[0].payload, `"source_id": ""`) {
			t.Errorf("%s: a placeholder source in %s", id, rows[0].payload)
		}
	}
}

// TestPushFunctionSynthesizedCreatedCarriesTheSameBinding: the first touch of
// an organization-wide credential that predates the stream writes created and
// then the later event, both binding organization, neither with a source_id.
func TestPushFunctionSynthesizedCreatedCarriesTheSameBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newPushFixture(t, ctx)
	snapshot := `{"status":"active","fingerprint":"0123456789abcdef","scopes":["ingest:status"]}`
	revoked := `{"organization_id":"org_EXAMPLE0000000000000001","binding":"organization","change":{"status":"revoked"}}`
	if err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
		revision, err := callFunction(ctx, tx, "external_push_credential.revoked", fnUUID1, revoked, snapshot)
		if err == nil && revision != 2 {
			err = fmt.Errorf("revision = %d, want 2", revision)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	rows := storedPushEvents(t, ctx, env, fnUUID1)
	if len(rows) != 2 || rows[0].eventType != "external_push_credential.created" || rows[1].eventType != "external_push_credential.revoked" {
		t.Fatalf("rows = %+v, want created then revoked", rows)
	}
	for _, row := range rows {
		var members map[string]json.RawMessage
		if err := json.Unmarshal([]byte(row.payload), &members); err != nil {
			t.Fatal(err)
		}
		if string(members["binding"]) != `"organization"` {
			t.Errorf("%s: binding = %s, want \"organization\"", row.eventType, members["binding"])
		}
		if _, has := members["source_id"]; has {
			t.Errorf("%s: carries a source_id member: %s", row.eventType, row.payload)
		}
	}
}
