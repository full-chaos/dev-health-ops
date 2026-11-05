package reconcilejob

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

func f(class pushreconcile.Class, id string) pushreconcile.Finding {
	return pushreconcile.Finding{Class: class, ID: id}
}

func fixedPass(passes ...Pass) (PassFunc, *int) {
	calls := 0
	return func(context.Context) (Pass, error) {
		p := passes[min(calls, len(passes)-1)]
		calls++
		return p, nil
	}, &calls
}

func noSleep(context.Context, time.Duration) error { return nil }

// C47: two passes; only the findings both report with the same class are drift.
func TestC47RunKeepsOnlyTheFindingsBothPassesReport(t *testing.T) {
	pass, calls := fixedPass(
		Pass{Tokens: 3, Projected: 3, Findings: []pushreconcile.Finding{f("status", "a"), f("missing", "b"), f("scopes", "c")}},
		Pass{Tokens: 4, Projected: 3, Findings: []pushreconcile.Finding{f("status", "a"), f("expiry", "c")}},
	)
	var slept time.Duration
	rep, err := Run(context.Background(), pass, time.Minute, func(_ context.Context, d time.Duration) error { slept = d; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if *calls != 2 || slept != time.Minute || rep.Passes != 2 {
		t.Fatalf("calls %d slept %v passes %d, want 2 passes one minute apart", *calls, slept, rep.Passes)
	}
	if !slices.Equal(rep.Drift, []pushreconcile.Finding{f("status", "a")}) {
		t.Fatalf("drift = %v, want only a/status", rep.Drift)
	}
	if rep.Transient != 2 || rep.Tokens != 4 || rep.Projected != 3 {
		t.Fatalf("transient %d tokens %d projected %d, want 2, 4, 3", rep.Transient, rep.Tokens, rep.Projected)
	}
}

func TestC47bDelayZeroOrLessIsRefusedWithNoReport(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		pass, calls := fixedPass(Pass{Findings: []pushreconcile.Finding{f("status", "a")}})
		rep, err := Run(context.Background(), pass, d, func(context.Context, time.Duration) error {
			t.Fatal("slept with a nonpositive delay")
			return nil
		})
		if err == nil || *calls != 0 || rep.Passes != 0 || len(rep.Drift) != 0 {
			t.Fatalf("delay %v: calls %d report %+v err %v", d, *calls, rep, err)
		}
	}
}

func TestC47cAReadErrorInEitherPassIsAnErrorNotZeroDrift(t *testing.T) {
	boom := errors.New("read failed")
	for at := 1; at <= 2; at++ {
		n := 0
		pass := func(context.Context) (Pass, error) {
			n++
			if n == at {
				return Pass{}, boom
			}
			return Pass{}, nil
		}
		if _, err := Run(context.Background(), pass, time.Second, noSleep); !errors.Is(err, boom) {
			t.Errorf("failure in pass %d: err = %v", at, err)
		}
	}
	pass, _ := fixedPass(Pass{})
	if _, err := Run(context.Background(), pass, time.Second, func(context.Context, time.Duration) error { return boom }); !errors.Is(err, boom) {
		t.Errorf("failed wait: err = %v", err)
	}
}

func TestC47dCountsHaveEveryClassAndCountEachFinding(t *testing.T) {
	got := CountByClass([]pushreconcile.Finding{f("status", "a"), f("status", "b"), f("extra", "c")})
	if len(got) != len(pushreconcile.Classes) {
		t.Fatalf("%d classes in the counts, want %d", len(got), len(pushreconcile.Classes))
	}
	if got["status"] != 2 || got["extra"] != 1 || got["scopes"] != 0 {
		t.Fatalf("counts = %v", got)
	}
	if !reflect.DeepEqual(Report{Drift: []pushreconcile.Finding{f("status", "a")}}.Counts(), CountByClass([]pushreconcile.Finding{f("status", "a")})) {
		t.Fatal("Report.Counts differs from CountByClass")
	}
}

// ---- the walk, over a fake transaction ----

type fakeRows struct {
	pgx.Rows
	rows [][]any
	at   int
}

func (r *fakeRows) Next() bool { r.at++; return r.at <= len(r.rows) }
func (r *fakeRows) Close()     {}
func (r *fakeRows) Err() error { return nil }
func (r *fakeRows) Scan(dest ...any) error {
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.rows[r.at-1][i]))
	}
	return nil
}

type fakeTx struct {
	pgx.Tx
	db *fakeDB
}

func (t *fakeTx) Query(ctx context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if _, ok := ctx.Deadline(); !ok {
		t.db.noDeadline = true
	}
	if t.db.queryErr != nil {
		return nil, t.db.queryErr
	}
	if strings.Contains(sql, "external_ingest_tokens") {
		return &fakeRows{rows: t.db.tokenRows}, nil
	}
	return &fakeRows{rows: t.db.projRows}, nil
}
func (t *fakeTx) Commit(context.Context) error   { return nil }
func (t *fakeTx) Rollback(context.Context) error { return nil }

type fakeDB struct {
	tokenRows, projRows [][]any
	queryErr            error
	noDeadline          bool
	begins              int
}

func (d *fakeDB) BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error) {
	d.begins++
	return &fakeTx{db: d}, nil
}

var (
	walkNow = time.Date(2026, 11, 3, 12, 0, 0, 0, time.UTC)
	tokenA  = uuid.MustParse("cccccccc-0000-4000-8000-00000000000c")
	orgA    = uuid.MustParse("aaaaaaaa-0000-4000-8000-00000000000a")
)

func tokenRow(id uuid.UUID) []any {
	return []any{id, orgA.String(), (*uuid.UUID)(nil), []byte(`["ingest:write"]`), (*time.Time)(nil), (*time.Time)(nil)}
}

func projRow(id string, scopes ...string) []any {
	return []any{id, orgA, (*string)(nil), "active", scopes, (*string)(nil)}
}

func walk(t *testing.T, db *fakeDB) (Pass, error) {
	t.Helper()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return NewPass(db, schema, 10, func() time.Time { return walkNow })(context.Background())
}

// C46: the walk reports every class from the domain sweep and extra from the projection sweep, once.
func TestC46WalkCleanStoreHasNoFindingAndCountsBothSides(t *testing.T) {
	db := &fakeDB{tokenRows: [][]any{tokenRow(tokenA)}, projRows: [][]any{projRow(tokenA.String(), "ingest:write")}}
	pass, err := walk(t, db)
	if err != nil || len(pass.Findings) != 0 || pass.Tokens != 1 || pass.Projected != 1 {
		t.Fatalf("pass %+v err %v", pass, err)
	}
}

func TestC46bWalkReportsADifferenceOnceNotOncePerSweep(t *testing.T) {
	db := &fakeDB{tokenRows: [][]any{tokenRow(tokenA)}, projRows: [][]any{projRow(tokenA.String(), "ingest:write", "schema:read")}}
	pass, err := walk(t, db)
	if err != nil || !slices.Equal(pass.Findings, []pushreconcile.Finding{f("scopes", tokenA.String())}) {
		t.Fatalf("pass %+v err %v, want one scopes finding", pass, err)
	}
}

func TestC46cWalkReportsAProjectionRowWithNoTokenAsExtra(t *testing.T) {
	other := uuid.MustParse("dddddddd-0000-4000-8000-00000000000d").String()
	db := &fakeDB{projRows: [][]any{projRow(other, "ingest:write")}}
	pass, err := walk(t, db)
	if err != nil || !slices.Equal(pass.Findings, []pushreconcile.Finding{f("extra", other)}) || pass.Projected != 1 {
		t.Fatalf("pass %+v err %v, want one extra finding", pass, err)
	}
}

func TestC46dWalkReportsATokenWithNoProjectionAsMissing(t *testing.T) {
	db := &fakeDB{tokenRows: [][]any{tokenRow(tokenA)}}
	pass, err := walk(t, db)
	if err != nil || !slices.Equal(pass.Findings, []pushreconcile.Finding{f("missing", tokenA.String())}) {
		t.Fatalf("pass %+v err %v, want one missing finding", pass, err)
	}
}

func TestC46eEveryPageReadHasADeadlineAndAReadErrorIsAnError(t *testing.T) {
	db := &fakeDB{tokenRows: [][]any{tokenRow(tokenA)}}
	if _, err := walk(t, db); err != nil || db.noDeadline {
		t.Fatalf("err %v, a page read ran without a deadline: %v", err, db.noDeadline)
	}
	boom := errors.New("read failed")
	if _, err := walk(t, &fakeDB{queryErr: boom}); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the read error", err)
	}
}
