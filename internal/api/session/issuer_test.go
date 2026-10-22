package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

type fakeIssuer struct {
	requests []sessionissue.Request
	err      error
}

func (f *fakeIssuer) Issue(_ context.Context, _ *http.Request, req sessionissue.Request) (sessionissue.Pair, error) {
	f.requests = append(f.requests, req)
	if f.err != nil {
		return sessionissue.Pair{}, f.err
	}
	return sessionissue.Pair{Access: "issued.access.token", Refresh: "dhr1_issued"}, nil
}

// With auth-service owning sessions the pair comes from the issue route: the
// membership's organization, amr pwd, and no go-api mint or refresh_tokens
// write (the nil transaction would fail any database write).
func TestIssueMembershipTokensTakesThePairFromAuthService(t *testing.T) {
	issuer := &fakeIssuer{}
	h := handlers{Deps: Deps{Issuer: issuer}}
	user := &userRow{ID: uuid.New(), Email: "alice@example.com"}
	org := uuid.New()
	for name, membership := range map[string]*membershipRow{"member": {OrgID: org}, "no membership": nil} {
		issuer.requests = nil
		pair, err := h.issueMembershipTokens(context.Background(), nil, httptest.NewRequest(http.MethodPost, "/", nil), user, membership)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if pair.access != "issued.access.token" || pair.refresh != "dhr1_issued" {
			t.Fatalf("%s: pair %+v, want the issued one", name, pair)
		}
		if len(issuer.requests) != 1 {
			t.Fatalf("%s: %d issue calls", name, len(issuer.requests))
		}
		got := issuer.requests[0]
		if got.PrincipalID != user.ID || strings.Join(got.AMR, ",") != "pwd" {
			t.Fatalf("%s: request %+v", name, got)
		}
		if (membership == nil) != (got.OrganizationID == nil) || (membership != nil && *got.OrganizationID != org) {
			t.Fatalf("%s: organization %v", name, got.OrganizationID)
		}
	}
}

func TestAnIssueFailureAnswers503AndMintsNothing(t *testing.T) {
	issuer := &fakeIssuer{err: fmt.Errorf("%w: status 503", sessionissue.ErrUnavailable)}
	h := handlers{Deps: Deps{Issuer: issuer, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}}
	_, err := h.issueMembershipTokens(context.Background(), nil, httptest.NewRequest(http.MethodPost, "/", nil), &userRow{ID: uuid.New()}, nil)
	if !errors.Is(err, sessionissue.ErrUnavailable) {
		t.Fatalf("err %v, want ErrUnavailable", err)
	}
	w := httptest.NewRecorder()
	h.fail(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil), "issue tokens", err)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), sessionissue.UnavailableText) {
		t.Fatalf("answer %d %s, want 503 %q", w.Code, w.Body, sessionissue.UnavailableText)
	}
	w = httptest.NewRecorder()
	h.fail(w, httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil), "other", errors.New("boom"))
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("an unrelated failure answers %d, want 500", w.Code)
	}
}
