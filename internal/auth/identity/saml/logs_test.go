package saml

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

const logProbe = "user@example.com"

type failingProviders struct{}

func (f failingProviders) Provider(context.Context, uuid.UUID) (Provider, error) {
	return Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: "saml"}, nil
}

type failingEntitlements struct{ err error }

func (f failingEntitlements) OrgHasSSO(context.Context, uuid.UUID) (bool, error) { return false, f.err }

func TestSAMLLogsCarryNoErrorText(t *testing.T) {
	for name, err := range map[string]error{
		"database": &pgconn.PgError{Code: "23505", Message: "duplicate", Detail: "Key (email)=(" + logProbe + ") already exists."},
		"client":   fmt.Errorf("fetch: %w", errors.New("dial https://idp.example.com/?token="+logProbe)),
	} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))

			h := Handlers{Logger: logger}
			req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/saml/x/acs?token="+logProbe, nil)
			rec := httptest.NewRecorder()
			h.writeError(rec, req, err)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("status %d", rec.Code)
			}

			svc := &Service{Providers: failingProviders{}, Entitlements: failingEntitlements{err: err}, Logger: logger, BaseURL: func() string { return "" }}
			if _, err := svc.Metadata(context.Background(), uuid.New()); !errors.Is(err, ErrNotEntitled) {
				t.Fatalf("entitlement error: %v, want ErrNotEntitled", err)
			}

			out := buf.String()
			if strings.Contains(out, logProbe) || strings.Contains(out, "duplicate") || strings.Contains(out, "dial") {
				t.Fatalf("log carries error or request text: %s", out)
			}
			if strings.Count(out, `"error":{"class":`) != 2 {
				t.Fatalf("log lines lack the error class group: %s", out)
			}
			if name == "database" && !strings.Contains(out, `"code":"23505"`) {
				t.Fatalf("database error without its SQLSTATE: %s", out)
			}
		})
	}
}
