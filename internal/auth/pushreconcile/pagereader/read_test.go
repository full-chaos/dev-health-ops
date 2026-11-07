package pagereader

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// C22: the domain read names the six columns the reconcile needs and no secret, name or use column.
func TestC22DomainReadHoldsNoSecretColumns(t *testing.T) {
	const want = "id, org_id, source_id, scopes, expires_at, revoked_at"
	if reconcileTokenColumns != want {
		t.Fatalf("columns = %q, want %q", reconcileTokenColumns, want)
	}
	for name, sql := range map[string]string{"page": reconcileTokenPageSQL, "ids": reconcileTokenIDsSQL} {
		if !strings.HasPrefix(strings.TrimSpace(sql), "SELECT "+want+" FROM external_ingest_tokens") {
			t.Errorf("%s: statement = %q", name, sql)
		}
		for _, bad := range []string{"token_hash", "token_prefix", "name", "last_used_at", "created_at", "SELECT *", "FOR UPDATE", "INSERT", "UPDATE", "DELETE", ";"} {
			if strings.Contains(sql, bad) {
				t.Errorf("%s: statement names %q: %q", name, bad, sql)
			}
		}
	}
	for _, want := range []string{"id > $1", "ORDER BY id", "LIMIT $2"} {
		if !strings.Contains(reconcileTokenPageSQL, want) {
			t.Errorf("page statement lacks %q", want)
		}
	}
	if !strings.Contains(reconcileTokenIDsSQL, "id = ANY($1)") {
		t.Errorf("ids statement lacks id = ANY($1)")
	}
}

type snapRows struct {
	pgx.Rows
	rows [][]any
	at   int
}

func (f *snapRows) Next() bool { f.at++; return f.at <= len(f.rows) }
func (f *snapRows) Close()     {}
func (f *snapRows) Err() error { return nil }
func (f *snapRows) Scan(dest ...any) error {
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(f.rows[f.at-1][i]))
	}
	return nil
}

type snapTx struct {
	pgx.Tx
	log       *[]string
	tokenRows [][]any
	projRows  [][]any
}

func (t *snapTx) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "external_ingest_tokens") {
		*t.log = append(*t.log, "tokens")
		return &snapRows{rows: t.tokenRows}, nil
	}
	*t.log = append(*t.log, "projection")
	return &snapRows{rows: t.projRows}, nil
}
func (t *snapTx) Commit(context.Context) error   { *t.log = append(*t.log, "commit"); return nil }
func (t *snapTx) Rollback(context.Context) error { *t.log = append(*t.log, "rollback"); return nil }

type snapDB struct {
	log       []string
	opts      []pgx.TxOptions
	tx        *snapTx
	tokenRows [][]any
	projRows  [][]any
}

func (d *snapDB) BeginTx(_ context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	d.opts = append(d.opts, opts)
	d.log = append(d.log, "begin")
	d.tx = &snapTx{log: &d.log, tokenRows: d.tokenRows, projRows: d.projRows}
	return d.tx, nil
}

func schemaAuth(t *testing.T) authschema.ValidatedIdentifier {
	t.Helper()
	id, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seededSnap() *snapDB {
	id := uuid.MustParse("cccccccc-0000-4000-8000-00000000000c")
	org := uuid.MustParse("aaaaaaaa-0000-4000-8000-00000000000a")
	return &snapDB{
		tokenRows: [][]any{{id, org.String(), (*uuid.UUID)(nil), []byte(`["ingest:write"]`), (*time.Time)(nil), (*time.Time)(nil)}},
		projRows:  [][]any{{id.String(), org, (*string)(nil), "active", []string{"ingest:write"}, (*string)(nil)}},
	}
}

// C23: one page pair is one REPEATABLE READ, READ ONLY transaction: both reads go through
// the same transaction and it ends once.
func TestC23PagePairRunsInOneRepeatableReadReadOnlyTransaction(t *testing.T) {
	for name, read := range map[string]func(*snapDB) (PagePair, error){
		"domain page": func(d *snapDB) (PagePair, error) {
			return ReadDomainPage(context.Background(), d, schemaAuth(t), uuid.Nil, 10)
		},
		"projection page": func(d *snapDB) (PagePair, error) {
			return ReadProjectionPage(context.Background(), d, schemaAuth(t), "", 10)
		},
	} {
		d := seededSnap()
		pair, err := read(d)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(d.opts) != 1 || d.opts[0].IsoLevel != pgx.RepeatableRead || d.opts[0].AccessMode != pgx.ReadOnly {
			t.Fatalf("%s: tx options = %+v", name, d.opts)
		}
		if len(pair.Tokens) != 1 || len(pair.Projected) != 1 {
			t.Fatalf("%s: pair = %+v", name, pair)
		}
		var reads []string
		for _, e := range d.log {
			if e == "tokens" || e == "projection" {
				reads = append(reads, e)
			}
		}
		if len(reads) != 2 || d.log[0] != "begin" {
			t.Fatalf("%s: log = %v", name, d.log)
		}
		ends := 0
		for _, e := range d.log {
			if e == "commit" || e == "rollback" {
				ends++
			}
		}
		if ends < 1 {
			t.Fatalf("%s: the transaction is never ended: %v", name, d.log)
		}
	}
}

func TestC23bAnEmptyDomainPageReadsNoProjectionAndIsLast(t *testing.T) {
	d := &snapDB{}
	pair, err := ReadDomainPage(context.Background(), d, schemaAuth(t), uuid.Nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if !pair.Last || len(pair.Tokens) != 0 || len(pair.Projected) != 0 {
		t.Fatalf("pair = %+v", pair)
	}
	for _, e := range d.log {
		if e == "projection" {
			t.Fatalf("projection read for no ids: %v", d.log)
		}
	}
}

func TestC23cBadBatchRefusedBeforeBeginning(t *testing.T) {
	d := &snapDB{}
	for _, bad := range []int{0, -1, 1001} {
		if _, err := ReadDomainPage(context.Background(), d, schemaAuth(t), uuid.Nil, bad); err == nil {
			t.Errorf("domain batch %d accepted", bad)
		}
		if _, err := ReadProjectionPage(context.Background(), d, schemaAuth(t), "", bad); err == nil {
			t.Errorf("projection batch %d accepted", bad)
		}
	}
	if len(d.log) != 0 {
		t.Fatalf("a transaction began: %v", d.log)
	}
}
