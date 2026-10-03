package logging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// plantedSecretShapes are secret-shaped strings in the forms a database, provider or transport error carries them.
var plantedSecretShapes = []string{
	"the planted detail of ticket 7933",
	"a planted bearer header of ticket 7933",
	"https://svc:planted-userinfo-7933@internal-host.example.test/db?sslmode=require",
	"https://api.example.test/repos?access_token=planted-query-7933",
	"the planted assignment of ticket 7933",
	`{"token":"planted-json-7933"}`,
	"Authorization: token planted-header-7933",
}

type fakePgError struct{ state, text string }

func (e *fakePgError) Error() string    { return e.text }
func (e *fakePgError) SQLState() string { return e.state }

func attrsText(err error) string {
	var b strings.Builder
	for _, attr := range ErrorAttrs(err) {
		b.WriteString(attr.Key + "=" + attr.Value.String() + "\n")
	}
	return b.String()
}

func TestErrorAttrsCarryNoSecretShapedTextFromTheErrorMessage(t *testing.T) {
	for _, shape := range plantedSecretShapes {
		for name, err := range map[string]error{
			"plain":    errors.New("failed: " + shape),
			"wrapped":  fmt.Errorf("op x: %w", errors.New("inner "+shape)),
			"postgres": &fakePgError{state: "23505", text: `duplicate key value violates unique constraint "k": Key (token)=(` + shape + `) already exists`},
		} {
			t.Run(name+"/"+shape[:12], func(t *testing.T) {
				text := attrsText(err)
				for _, secret := range []string{"planted"} {
					if strings.Contains(text, secret) {
						t.Fatalf("an attribute carries the planted secret:\n%s", text)
					}
				}
			})
		}
	}
}

func TestErrorClassIsTheErrorsIdentityNotItsText(t *testing.T) {
	for name, row := range map[string]struct {
		err  error
		want string
	}{
		"nil":                {nil, "none"},
		"canceled":           {fmt.Errorf("x: %w", context.Canceled), ErrorClassCanceled},
		"deadline":           {fmt.Errorf("x: %w", context.DeadlineExceeded), ErrorClassDeadline},
		"postgres":           {&fakePgError{state: "40001", text: "serialization failure"}, ErrorClassPostgres},
		"json syntax":        {&json.SyntaxError{}, ErrorClassDecode},
		"json type":          {&json.UnmarshalTypeError{Value: "string"}, ErrorClassDecode},
		"refused":            {fmt.Errorf("dial: %w", syscall.ECONNREFUSED), "refused"},
		"reset":              {fmt.Errorf("read: %w", syscall.ECONNRESET), "reset"},
		"eof":                {io.ErrUnexpectedEOF, "eof"},
		"dns":                {&net.DNSError{Err: "no such host", Name: "planted-host-7933.example.test"}, "dns"},
		"unclassified":       {errors.New("deadline exceeded planted"), ErrorClassOther},
		"text only mentions": {errors.New("context canceled"), ErrorClassOther},
	} {
		t.Run(name, func(t *testing.T) {
			if got := ErrorClass(row.err); got != row.want {
				t.Fatalf("ErrorClass = %q, want %q", got, row.want)
			}
		})
	}
}

func TestErrorTypeIsTheInnermostGoTypeName(t *testing.T) {
	if got := ErrorType(fmt.Errorf("a: %w", fmt.Errorf("b: %w", &fakePgError{}))); got != "*logging.fakePgError" {
		t.Fatalf("ErrorType = %q", got)
	}
	if got := ErrorType(errors.Join(errors.New("planted"), errors.New("x"))); strings.Contains(got, "planted") || got == "" {
		t.Fatalf("ErrorType of a joined error = %q", got)
	}
	if got := ErrorType(nil); got != "none" {
		t.Fatalf("ErrorType(nil) = %q", got)
	}
}

func TestErrorAttrsCodeAndSchemaNames(t *testing.T) {
	text := attrsText(&fakePgError{state: "23505", text: "duplicate key value violates unique constraint"})
	for _, want := range []string{"error_class=postgres", "error_type=*logging.fakePgError", "error_code=23505"} {
		if !strings.Contains(text, want) {
			t.Fatalf("attributes lack %q:\n%s", want, text)
		}
	}
	real := &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint", Detail: "Key (token)=(planted-secret-7933) already exists.", ConstraintName: "sync_runs_pkey", TableName: "sync_runs", ColumnName: "id"}
	text = attrsText(fmt.Errorf("insert: %w", real))
	for _, want := range []string{"error_class=postgres", "error_type=*pgconn.PgError", "error_code=23505"} {
		if !strings.Contains(text, want) {
			t.Fatalf("attributes of a real PgError lack %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "planted") || strings.Contains(text, "Key (") {
		t.Fatalf("the detail (key values) of a PgError reached an attribute:\n%s", text)
	}
	for _, banned := range []string{"sync_runs_pkey", "sync_runs", "error_constraint", "error_table", "error_column"} {
		if strings.Contains(text, banned) {
			t.Fatalf("a schema name of a PgError reached an attribute (%q):\n%s", banned, text)
		}
	}
}

type panickingMessageError struct{}

func (panickingMessageError) Error() string { panic("boom") }

func TestErrorAttrsSurviveAnErrorMethodThatPanics(t *testing.T) {
	text := attrsText(panickingMessageError{})
	if !strings.Contains(text, "error_class=other") || !strings.Contains(text, "error_type=logging.panickingMessageError") {
		t.Fatalf("attributes of a panicking error:\n%s", text)
	}
}

func TestErrorAttrIsOneGroupNamedError(t *testing.T) {
	var out strings.Builder
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	logger.Info("failed", ErrorAttr(&pgconn.PgError{Code: "23505", Message: "m", Detail: "Key (a)=(planted-secret-7933)", ConstraintName: "sync_runs_pkey"}))
	line := out.String()
	for _, want := range []string{`"error":{`, `"class":"postgres"`, `"type":"*pgconn.PgError"`, `"code":"23505"`} {
		if !strings.Contains(line, want) {
			t.Fatalf("the log line lacks %s: %s", want, line)
		}
	}
	if strings.Contains(line, "planted") {
		t.Fatalf("the log line carries the key value: %s", line)
	}
}

// Each anchor of the SQLSTATE check: a value that holds a valid code PLUS more is refused whole.
func TestSQLStateIsAnchored(t *testing.T) {
	for _, state := range []string{"23505 planted detail", "planted 23505", "x23505y", "2350", "235055", "23a05", "23505\n", " 23505", ""} {
		if text := attrsText(&fakePgError{state: state, text: "x"}); strings.Contains(text, "error_code") {
			t.Fatalf("SQLSTATE %q was accepted:\n%s", state, text)
		}
	}
	if text := attrsText(&fakePgError{state: "23505", text: "x"}); !strings.Contains(text, "error_code=23505") {
		t.Fatalf("a valid SQLSTATE was refused:\n%s", text)
	}
}

// Detail, Message and Hint of a PostgreSQL error never become attributes, even when the value is identifier-shaped.
func TestAnIdentifierShapedPgDetailIsNeverAnAttribute(t *testing.T) {
	text := attrsText(&pgconn.PgError{Code: "23505", Message: "secret_message_7933", Detail: "secret_value_7933", Hint: "secret_hint_7933", ConstraintName: "users_pkey"})
	if strings.Contains(text, "secret_") {
		t.Fatalf("an attribute carries Message, Detail or Hint:\n%s", text)
	}
	for _, name := range []string{"users_pkey", "error_constraint", "error_table", "error_column", "error_detail"} {
		if strings.Contains(text, name) {
			t.Fatalf("an attribute carries %q:\n%s", name, text)
		}
	}
	if !strings.Contains(text, "error_code=23505") {
		t.Fatalf("the SQLSTATE is missing:\n%s", text)
	}
}
