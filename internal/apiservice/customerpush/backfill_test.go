package customerpush

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
)

func TestStoredLifecycleEventsAreTheCreatedEventOfTheStoredToken(t *testing.T) {
	org := "9f1c2b3a-0000-4000-8000-000000000001"
	source := uuid.New()
	expiry := time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := map[string]tokenRow{
		"source bound":        {ID: uuid.New(), OrgID: org, SourceID: &source, Scopes: []byte(`["ingest:write","ingest:status"]`)},
		"organization wide":   {ID: uuid.New(), OrgID: org, Scopes: []byte(`["ingest:status"]`)},
		"expired not revoked": {ID: uuid.New(), OrgID: org, SourceID: &source, Scopes: []byte(`["ingest:write"]`), ExpiresAt: &expiry},
	}
	for name, row := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := storedLifecycleEvents(row)
			if err != nil || len(got) != 1 {
				t.Fatalf("events = %+v, %v", got, err)
			}
			names, _ := pushevent.ScopeNames(row.Scopes)
			var at *pytime.DateTime
			if row.ExpiresAt != nil {
				at = &pytime.DateTime{Time: *row.ExpiresAt}
			}
			want, err := createdEvents(newToken{ID: row.ID, OrgID: row.OrgID, SourceID: row.SourceID, Scopes: names, ExpiresAt: at})
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("events differ from the route's created event:\n got %+v\nwant %+v", got, want)
			}
			if got[0].EventType != externalpush.Created {
				t.Fatalf("event type %q", got[0].EventType)
			}
		})
	}
	t.Run("organization wide carries no source id", func(t *testing.T) {
		got, _ := storedLifecycleEvents(cases["organization wide"])
		if len(got) != 1 || got[0].Binding != externalpush.BindingOrganization || got[0].SourceID != "" {
			t.Fatalf("events = %+v", got)
		}
	})
}

func TestStoredLifecycleEventsOfARevokedTokenAreCreatedThenTheRouteRevokedEvent(t *testing.T) {
	source := uuid.New()
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	row := tokenRow{ID: uuid.New(), OrgID: "9f1c2b3a-0000-4000-8000-000000000001", SourceID: &source, Scopes: []byte(`["ingest:write"]`), RevokedAt: &at}
	got, err := storedLifecycleEvents(row)
	if err != nil {
		t.Fatal(err)
	}
	want, err := revokedEvent(row)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].EventType != externalpush.Created || !reflect.DeepEqual(got[1], want) {
		t.Fatalf("events = %+v, want created then the revoked event with its snapshot %+v", got, want)
	}
	if got[1].Snapshot == nil || got[1].Snapshot.Status != "active" {
		t.Fatalf("snapshot = %+v", got[1].Snapshot)
	}
}

// fakeBackfillDB is a scripted pool for the backfill: it holds token rows and
// the aggregates that exist, answers the statements the verb issues, and keeps
// every statement text. A writer error aborts the transaction until a rollback
// to savepoint, as Postgres does.
type fakeBackfillDB struct {
	tokens     []tokenRow
	aggregates map[string]bool
	writerErr  map[string]error
	stmts      []string
	events     map[string][]string
}

func newFakeBackfillDB(tokens ...tokenRow) *fakeBackfillDB {
	slices.SortFunc(tokens, func(a, b tokenRow) int { return strings.Compare(a.ID.String(), b.ID.String()) })
	return &fakeBackfillDB{tokens: tokens, aggregates: map[string]bool{}, writerErr: map[string]error{}, events: map[string][]string{}}
}

func fakeToken(revoked bool) tokenRow {
	source := uuid.New()
	row := tokenRow{ID: uuid.New(), OrgID: "9f1c2b3a-0000-4000-8000-000000000001", SourceID: &source, Scopes: []byte(`["ingest:write"]`)}
	if revoked {
		at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
		row.RevokedAt = &at
	}
	return row
}

func (db *fakeBackfillDB) Begin(context.Context) (pgx.Tx, error) {
	return &fakeBackfillTx{db: db, pending: map[string][]string{}}, nil
}

func (db *fakeBackfillDB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	db.stmts = append(db.stmts, sql)
	after, _ := args[0].(uuid.UUID)
	batch, _ := args[len(args)-1].(int)
	filtered := strings.Contains(sql, "aggregate_revisions")
	var out [][]any
	for _, t := range db.tokens {
		if strings.Compare(t.ID.String(), after.String()) <= 0 || (filtered && db.aggregates[t.ID.String()]) {
			continue
		}
		if len(out) < batch {
			out = append(out, []any{t.ID, t.SourceID == nil, t.RevokedAt != nil})
		}
	}
	return &fakeBackfillRows{rows: out, at: -1}, nil
}

type fakeBackfillRows struct {
	pgx.Rows
	rows [][]any
	at   int
}

func (r *fakeBackfillRows) Next() bool             { r.at++; return r.at < len(r.rows) }
func (r *fakeBackfillRows) Close()                 {}
func (r *fakeBackfillRows) Err() error             { return nil }
func (r *fakeBackfillRows) Scan(dest ...any) error { return assignFake(dest, r.rows[r.at]) }

func assignFake(dest, vals []any) error {
	for i, d := range dest {
		switch p := d.(type) {
		case *uuid.UUID:
			*p = vals[i].(uuid.UUID)
		case *bool:
			*p = vals[i].(bool)
		case *string:
			*p = vals[i].(string)
		case **uuid.UUID:
			*p, _ = vals[i].(*uuid.UUID)
		case *[]byte:
			*p = vals[i].([]byte)
		case **time.Time:
			*p, _ = vals[i].(*time.Time)
		case *time.Time:
			*p = vals[i].(time.Time)
		case *int64:
			*p = vals[i].(int64)
		default:
			return fmt.Errorf("fake scan: unsupported %T", d)
		}
	}
	return nil
}

type fakeBackfillRow struct {
	vals []any
	err  error
}

func (r fakeBackfillRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return assignFake(dest, r.vals)
}

type fakeBackfillTx struct {
	pgx.Tx
	db      *fakeBackfillDB
	pending map[string][]string
	marks   []map[string][]string
	aborted bool
}

func (tx *fakeBackfillTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	tx.db.stmts = append(tx.db.stmts, sql)
	switch {
	case strings.HasPrefix(sql, "SAVEPOINT"):
		tx.marks = append(tx.marks, cloneEvents(tx.pending))
	case strings.HasPrefix(sql, "ROLLBACK TO SAVEPOINT") && len(tx.marks) > 0:
		tx.pending, tx.aborted = cloneEvents(tx.marks[len(tx.marks)-1]), false
	case strings.HasPrefix(sql, "RELEASE SAVEPOINT") && len(tx.marks) > 0:
		tx.marks = tx.marks[:len(tx.marks)-1]
	}
	return pgconn.CommandTag{}, nil
}

func cloneEvents(m map[string][]string) map[string][]string {
	out := map[string][]string{}
	for k, v := range m {
		out[k] = slices.Clone(v)
	}
	return out
}

func (tx *fakeBackfillTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	tx.db.stmts = append(tx.db.stmts, sql)
	if tx.aborted {
		return fakeBackfillRow{err: &pgconn.PgError{Code: "25P02"}}
	}
	switch {
	case strings.Contains(sql, "append_external_push_lifecycle_event"):
		kind, id := args[0].(string), args[1].(string)
		if err := tx.db.writerErr[id]; err != nil {
			tx.aborted = true
			return fakeBackfillRow{err: err}
		}
		if kind == "external_push_credential.created" && (tx.db.aggregates[id] || len(tx.pending[id]) > 0) {
			tx.aborted = true
			return fakeBackfillRow{err: &pgconn.PgError{Code: "55000", Message: "needs a credential with no earlier event"}}
		}
		tx.pending[id] = append(tx.pending[id], kind)
		return fakeBackfillRow{vals: []any{int64(len(tx.pending[id]))}}
	case strings.Contains(sql, "aggregate_revisions"):
		return fakeBackfillRow{vals: []any{tx.db.aggregates[args[1].(string)]}}
	case strings.Contains(sql, "external_ingest_tokens"):
		id := args[0].(uuid.UUID)
		for _, t := range tx.db.tokens {
			if t.ID == id {
				return fakeBackfillRow{vals: []any{t.ID, t.OrgID, t.SourceID, t.Name, t.Prefix, t.Scopes, t.ExpiresAt, t.RevokedAt, t.LastUsedAt, t.CreatedAt}}
			}
		}
		return fakeBackfillRow{err: pgx.ErrNoRows}
	}
	return fakeBackfillRow{err: errors.New("fake: unexpected statement " + sql)}
}

func (tx *fakeBackfillTx) Commit(context.Context) error {
	if tx.aborted {
		return errors.New("fake: commit of an aborted transaction")
	}
	for id, kinds := range tx.pending {
		tx.db.aggregates[id] = true
		tx.db.events[id] = append(tx.db.events[id], kinds...)
	}
	return nil
}

func (tx *fakeBackfillTx) Rollback(context.Context) error { return nil }

func TestBackfillSecondRunWritesNothingAndSkipsEveryToken(t *testing.T) {
	db := newFakeBackfillDB(fakeToken(false), fakeToken(false), fakeToken(true))
	first, err := backfillLifecycle(context.Background(), db, "auth", true, 2)
	if err != nil || first.Written != 3 || first.Failed != 0 {
		t.Fatalf("first run = %+v, %v", first, err)
	}
	before := fmt.Sprint(db.events)
	second, err := backfillLifecycle(context.Background(), db, "auth", true, 2)
	if err != nil {
		t.Fatal(err)
	}
	if second.Candidates != 3 || second.Skipped != 3 || second.Written != 0 || second.Failed != 0 {
		t.Fatalf("second run = %+v, want every token skipped", second)
	}
	if after := fmt.Sprint(db.events); after != before {
		t.Fatalf("the second run wrote: %s -> %s", before, after)
	}
	for _, tok := range db.tokens {
		want := []string{"external_push_credential.created"}
		if tok.RevokedAt != nil {
			want = append(want, "external_push_credential.revoked")
		}
		if got := db.events[tok.ID.String()]; !slices.Equal(got, want) {
			t.Fatalf("token events = %v, want %v", got, want)
		}
	}
}

func TestBackfillTokenWithAnAggregateIsSkippedNotFailed(t *testing.T) {
	have, fresh := fakeToken(false), fakeToken(false)
	db := newFakeBackfillDB(have, fresh)
	db.aggregates[have.ID.String()] = true
	rep, err := backfillLifecycle(context.Background(), db, "auth", true, 5)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Skipped != 1 || rep.Failed != 0 || rep.Written != 1 {
		t.Fatalf("report = %+v, want 1 skipped, 0 failed, 1 written", rep)
	}
	if len(db.events[have.ID.String()]) != 0 {
		t.Fatalf("a token with an aggregate was written: %v", db.events[have.ID.String()])
	}
}

func TestBackfillRealWriterErrorIsStillFailed(t *testing.T) {
	a, b, c := fakeToken(false), fakeToken(false), fakeToken(false)
	db := newFakeBackfillDB(a, b, c)
	victim := db.tokens[1].ID.String()
	db.writerErr[victim] = &pgconn.PgError{Code: "22023", Message: "refused"}
	rep, err := backfillLifecycle(context.Background(), db, "auth", true, 5)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Failed != 1 || rep.Written != 2 || rep.Skipped != 0 {
		t.Fatalf("report = %+v, want 1 failed, 2 written, 0 skipped", rep)
	}
	if len(db.events[victim]) != 0 {
		t.Fatalf("the failed token was written")
	}
}

func TestBackfillOnlyStateCode55000MeansAnExistingAggregate(t *testing.T) {
	wrap := func(err error) error { return fmt.Errorf("%w: appending: %w", audit.ErrPushLifecycle, err) }
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"55000 wrapped", wrap(&pgconn.PgError{Code: "55000"}), true},
		{"55000 bare", &pgconn.PgError{Code: "55000"}, true},
		{"22023", wrap(&pgconn.PgError{Code: "22023"}), false},
		{"deadlock", wrap(&pgconn.PgError{Code: "40P01"}), false},
		{"plain", errors.New("55000"), false},
	}
	for _, c := range cases {
		if got := isExistingAggregate(c.err); got != c.want {
			t.Errorf("%s: isExistingAggregate = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestBackfillNamesTheAuthSchemaOnlyInTheWriterCall(t *testing.T) {
	db := newFakeBackfillDB(fakeToken(false), fakeToken(true), fakeToken(false))
	db.aggregates[db.tokens[0].ID.String()] = true
	for _, apply := range []bool{false, true} {
		if _, err := backfillLifecycle(context.Background(), db, "auth", apply, 2); err != nil {
			t.Fatal(err)
		}
	}
	if len(db.stmts) == 0 {
		t.Fatal("no statements seen")
	}
	writers := 0
	for _, sql := range db.stmts {
		if !strings.Contains(strings.ToLower(sql), "auth") {
			continue
		}
		if !strings.Contains(sql, ".append_external_push_lifecycle_event(") || strings.Contains(sql, "aggregate_revisions") {
			t.Errorf("a statement names the auth schema and is not the writer call: %s", sql)
		}
		writers++
	}
	if writers == 0 {
		t.Fatal("no writer call seen")
	}
}

func TestBackfillDryRunWritesNothing(t *testing.T) {
	db := newFakeBackfillDB(fakeToken(false), fakeToken(true))
	rep, err := backfillLifecycle(context.Background(), db, "auth", false, 5)
	if err != nil || rep.Candidates != 2 || rep.Written != 0 || rep.Skipped != 0 || rep.Failed != 0 {
		t.Fatalf("report = %+v, %v", rep, err)
	}
	if len(db.events) != 0 || len(db.aggregates) != 0 {
		t.Fatalf("a dry run wrote: %v", db.events)
	}
	for _, sql := range db.stmts {
		if strings.Contains(sql, "append_external_push_lifecycle_event") || strings.Contains(sql, "SAVEPOINT") {
			t.Fatalf("a dry run issued %s", sql)
		}
	}
}

func TestBackfillWritesEachTokenInsideASavepointAndRollsBackAFailedOne(t *testing.T) {
	ok, bad := fakeToken(false), fakeToken(false)
	db := newFakeBackfillDB(ok, bad)
	victim := db.tokens[1].ID.String()
	db.writerErr[victim] = &pgconn.PgError{Code: "22023"}
	if _, err := backfillLifecycle(context.Background(), db, "auth", true, 5); err != nil {
		t.Fatal(err)
	}
	var trail []string
	for _, sql := range db.stmts {
		switch {
		case strings.HasPrefix(sql, "SAVEPOINT"), strings.HasPrefix(sql, "ROLLBACK TO SAVEPOINT"), strings.HasPrefix(sql, "RELEASE SAVEPOINT"):
			trail = append(trail, strings.Fields(sql)[0]+" "+strings.Fields(sql)[1])
		case strings.Contains(sql, "append_external_push_lifecycle_event"):
			trail = append(trail, "WRITE")
		}
	}
	want := []string{"SAVEPOINT backfill_token", "WRITE", "RELEASE SAVEPOINT", "SAVEPOINT backfill_token", "WRITE", "ROLLBACK TO"}
	if !slices.Equal(trail, want) {
		t.Fatalf("statement trail = %v, want %v", trail, want)
	}
}
