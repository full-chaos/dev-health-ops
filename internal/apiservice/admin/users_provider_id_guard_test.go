package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// touchedDB records that the handler reached the database, and fails the call.
type touchedDB struct{ touched *bool }

func (d touchedDB) Begin(context.Context) (pgx.Tx, error) {
	*d.touched = true
	return nil, errors.New("stop")
}

func (d touchedDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	*d.touched = true
	return pgconn.CommandTag{}, errors.New("stop")
}

func (d touchedDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	*d.touched = true
	return nil, errors.New("stop")
}

func (d touchedDB) QueryRow(context.Context, string, ...any) pgx.Row {
	*d.touched = true
	return failingRow{}
}

func createUserWithBody(t *testing.T, body string) (code int, response, logs string, touched bool) {
	t.Helper()
	var logBuffer bytes.Buffer
	h := &handlers{store: pgStore{Pool: touchedDB{&touched}}, logger: slog.New(slog.NewTextHandler(&logBuffer, nil))}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/users", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	decoded, outcome, _, err := pybody.Read(request)
	if err != nil || outcome != pybody.Ready {
		t.Fatalf("body: %v %v", err, outcome)
	}
	ctx := policy.WithBody(request.Context(), decoded)
	ctx = policy.WithUser(ctx, &policy.User{UserID: uuid.NewString(), ID: uuid.New(), IsSuperuser: true})
	recorder := httptest.NewRecorder()
	h.createUser(recorder, request.WithContext(ctx))
	return recorder.Code, recorder.Body.String(), logBuffer.String(), touched
}

// CHAOS-11619: an admin create of a non-local provider user with an empty,
// blank, missing or ValidSubject-invalid provider id is refused with 422
// before any database work; the answer names the field, never the value.
func TestCreateUserRefusesAnInvalidProviderIDBeforeTheDatabase(t *testing.T) {
	for name, body := range map[string]string{
		"github empty":           `{"email":"a@example.test","auth_provider":"github","auth_provider_id":""}`,
		"github blank":           `{"email":"a@example.test","auth_provider":"github","auth_provider_id":"  "}`,
		"github missing":         `{"email":"a@example.test","auth_provider":"github"}`,
		"github null":            `{"email":"a@example.test","auth_provider":"github","auth_provider_id":null}`,
		"google edge whitespace": `{"email":"a@example.test","auth_provider":"google","auth_provider_id":" MARKER77"}`,
		"oidc empty":             `{"email":"a@example.test","auth_provider":"oidc","auth_provider_id":""}`,
	} {
		t.Run(name, func(t *testing.T) {
			code, response, logs, touched := createUserWithBody(t, body)
			if touched {
				t.Fatal("the handler reached the database with an invalid provider id")
			}
			if code != http.StatusUnprocessableEntity || !strings.Contains(response, "auth_provider_id") {
				t.Fatalf("status %d body %q, want 422 naming auth_provider_id", code, response)
			}
			if strings.Contains(response, "MARKER77") || strings.Contains(logs, "MARKER77") {
				t.Fatalf("the provider id reached the response or the log: %q %q", response, logs)
			}
		})
	}
}

// Valid ids, local users and service accounts are not refused (control): the
// handler goes on to the database.
func TestCreateUserGoesOnWhenTheProviderIDIsNotInvalid(t *testing.T) {
	for name, body := range map[string]string{
		"github valid id":  `{"email":"a@example.test","auth_provider":"github","auth_provider_id":"12345"}`,
		"local no id":      `{"email":"a@example.test"}`,
		"local explicit":   `{"email":"a@example.test","auth_provider":"local","auth_provider_id":""}`,
		"empty provider":   `{"email":"a@example.test","auth_provider":""}`,
		"service no id":    `{"email":"svc@example.test","auth_provider":"service"}`,
		"service empty id": `{"email":"svc@example.test","auth_provider":"service","auth_provider_id":""}`,
	} {
		t.Run(name, func(t *testing.T) {
			code, response, _, touched := createUserWithBody(t, body)
			if !touched {
				t.Fatalf("refused before the database: %d %q", code, response)
			}
		})
	}
}
