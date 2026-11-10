package identity

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// legacyOrganizationCreatedLog is insertOrganizationCreatedLog as it stood before CHAOS-13507 (ops 490d43a1), a
// direct INSERT; the parity cells compare the routed row to it.
func legacyOrganizationCreatedLog(ctx context.Context, tx interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}, b organizationBirth, req OnboardOrganizationRequest) error {
	changes := pyjson.NewObject()
	changes.Set("organization_name", b.Name)
	encoded, err := pyjson.Marshal(changes)
	if err != nil {
		return err
	}
	metadata := []byte("{}")
	if len(req.RequestMetadata) > 0 {
		metadata = req.RequestMetadata
	}
	description := "Organization created during onboarding"
	if req.Description != nil {
		description = *req.Description
	}
	_, err = tx.Exec(ctx, `INSERT INTO public.audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description, changes, request_metadata, status, created_at)
VALUES ($1, $2, $3, 'create', 'organization', $4, $5, $6::json, $7::json, 'success', now())`,
		uuid.New(), b.ID, req.Principal, b.ID.String(), description, string(encoded), string(metadata))
	return err
}

type recordedStatement struct {
	sql  string
	args []any
}

type orgCreatedLogTx struct {
	now     time.Time
	execs   []recordedStatement
	queries []string
}

func (r *orgCreatedLogTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.execs = append(r.execs, recordedStatement{sql, args})
	return pgconn.CommandTag{}, nil
}

func (r *orgCreatedLogTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.queries = append(r.queries, sql)
	return orgCreatedLogNow{r.now}
}

type orgCreatedLogNow struct{ at time.Time }

func (t orgCreatedLogNow) Scan(dest ...any) error {
	if len(dest) != 1 {
		return errors.New("orgCreatedLogNow scans one column")
	}
	at, ok := dest[0].(*time.Time)
	if !ok {
		return errors.New("orgCreatedLogNow scans into *time.Time")
	}
	*at = t.at
	return nil
}

// storedColumn is what one column of an INSERT stores: a bound argument (dereferenced) or an SQL literal, with its cast.
type storedColumn struct {
	value any
	cast  string
	sql   string
}

var auditInsertShape = regexp.MustCompile(`(?s)^INSERT INTO public\.audit_logs\s*\(([^)]*)\)\s*VALUES\s*\((.*)\)\s*$`)

func storedColumns(t *testing.T, st recordedStatement) map[string]storedColumn {
	t.Helper()
	m := auditInsertShape.FindStringSubmatch(strings.TrimSpace(st.sql))
	if m == nil {
		t.Fatalf("not a single-row audit_logs INSERT: %q", st.sql)
	}
	columns, values := strings.Split(m[1], ","), strings.Split(m[2], ",")
	if len(columns) != len(values) {
		t.Fatalf("%d columns, %d values: %q", len(columns), len(values), st.sql)
	}
	out := map[string]storedColumn{}
	for i, column := range columns {
		value := strings.TrimSpace(values[i])
		expr, cast, _ := strings.Cut(value, "::")
		c := storedColumn{cast: cast}
		if strings.HasPrefix(expr, "$") {
			n, err := strconv.Atoi(expr[1:])
			if err != nil || n < 1 || n > len(st.args) {
				t.Fatalf("placeholder %q of %d args", expr, len(st.args))
			}
			c.value = deref(st.args[n-1])
		} else if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") {
			c.value = strings.Trim(expr, "'")
		} else {
			c.sql = expr
		}
		out[strings.TrimSpace(column)] = c
	}
	return out
}

func deref(v any) any {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		return rv.Elem().Interface()
	}
	return v
}

// The routed row stores what the direct INSERT stored, column by column, on every input shape: id aside (a new uuid
// each), created_at by form (now() before, the transaction's now() read in the same transaction now), and
// error_message NULL (the direct INSERT left it to the column, which has no default). The JSON columns keep their text
// argument and ::json cast, so json stores the same text.
func TestOrganizationCreatedLogRowIsTheDirectInsertsRow(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 10, 9, 30, 0, 123456000, time.FixedZone("x", 3600))
	description := "Organization created by the platform"
	for name, c := range map[string]struct {
		org string
		req OnboardOrganizationRequest
	}{
		"defaults":    {"Acme Ops", OnboardOrganizationRequest{Principal: uuid.New(), Name: "Acme Ops"}},
		"empty json":  {"Acme Ops", OnboardOrganizationRequest{Principal: uuid.New(), Name: "Acme Ops", RequestMetadata: []byte{}}},
		"request set": {"Olive's Ünicode Org!!", OnboardOrganizationRequest{Principal: uuid.New(), Name: "Olive's Ünicode Org!!", Description: &description, RequestMetadata: []byte(`{"ip_address":"192.0.2.1"}`)}},
	} {
		b := newOrganizationBirth("acme", c.org, &c.req.Principal)
		legacy := &orgCreatedLogTx{now: now}
		if err := legacyOrganizationCreatedLog(ctx, legacy, b, c.req); err != nil {
			t.Fatal(err)
		}
		routed := &orgCreatedLogTx{now: now}
		if err := insertOrganizationCreatedLog(ctx, routed, b, c.req); err != nil {
			t.Fatal(err)
		}
		if len(legacy.execs) != 1 || len(routed.execs) != 1 {
			t.Fatalf("%s: %d and %d statements, want one each", name, len(legacy.execs), len(routed.execs))
		}
		if len(routed.queries) != 1 || routed.queries[0] != `SELECT now()` {
			t.Fatalf("%s: routed reads %q, want the transaction's now() once", name, routed.queries)
		}
		old, got := storedColumns(t, legacy.execs[0]), storedColumns(t, routed.execs[0])
		if _, ok := old["error_message"]; ok {
			t.Fatalf("%s: the direct INSERT names error_message", name)
		}
		old["error_message"] = storedColumn{}
		if got["error_message"].value != nil || got["error_message"].sql != "" {
			t.Errorf("%s: error_message = %#v, want NULL", name, got["error_message"])
		}
		if len(got) != len(old) {
			t.Fatalf("%s: routed columns %d, direct %d", name, len(got), len(old))
		}
		for column, want := range old {
			have, ok := got[column]
			if !ok {
				t.Errorf("%s: routed row has no %s", name, column)
				continue
			}
			switch column {
			case "id":
				if id, ok := have.value.(uuid.UUID); !ok || id == uuid.Nil {
					t.Errorf("%s: id %#v, want a new uuid", name, have.value)
				}
			case "created_at":
				if want.sql != "now()" {
					t.Fatalf("%s: the direct INSERT's created_at is %#v", name, want)
				}
				if at, ok := have.value.(time.Time); !ok || !at.Equal(now) || at.Location() != time.UTC {
					t.Errorf("%s: created_at %#v, want the transaction's now() %v in UTC", name, have.value, now)
				}
			default:
				if !reflect.DeepEqual(have, want) {
					t.Errorf("%s: %s = %#v, direct INSERT %#v", name, column, have, want)
				}
			}
		}
		if v := got["changes"].value; v != `{"organization_name":"`+c.org+`"}` {
			t.Errorf("%s: changes %#v", name, v)
		}
	}
}
