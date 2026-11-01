package local

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

const registerAcceptedBody = `{"message":"Check your email to continue"}`

// registerHandler is the test-only HTTP face of Register: a policy refusal is
// a 422, every other answer is the one 202 body.
func registerHandler(f *Flows, violations func(http.ResponseWriter, []string)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Email, Password string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		out, err := f.Register(r.Context(), RegisterRequest{Email: in.Email, Password: in.Password})
		switch {
		case err != nil:
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(500)
			_, _ = w.Write([]byte(`{"detail":"Internal Server Error"}`))
		case out.Kind == FlowPolicy:
			violations(w, out.Violations)
		default:
			w.Header().Set("content-type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(registerAcceptedBody))
		}
	})
}

func registerOver(t *testing.T, h http.Handler, email string) (int, string, http.Header) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/register", strings.NewReader(`{"email":"`+email+`","password":"`+flowPassword+`"}`)))
	body, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(body), rec.Result().Header
}

func TestRegisterHTTPAnswerBytesAreEqualAndTheWorkMatches(t *testing.T) {
	var hashes atomic.Int32
	run := func(email string, existing bool) (int, string, http.Header, *flowRig) {
		rig := newFlowRig(t, true)
		rig.f.hash = func(p string) (string, error) { hashes.Add(1); return "hash-of:" + p, nil }
		if existing {
			rig.user(email, true)
		}
		status, body, header := registerOver(t, registerHandler(rig.f, nil), email)
		return status, body, header, rig
	}
	hashes.Store(0)
	newStatus, newBody, newHeader, newRig := run("fresh@example.com", false)
	newHashes := hashes.Swap(0)
	oldStatus, oldBody, oldHeader, oldRig := run("taken@example.com", true)
	oldHashes := hashes.Load()

	if newStatus != http.StatusAccepted || newStatus != oldStatus || newBody != oldBody || newBody != registerAcceptedBody ||
		newHeader.Get("content-type") != oldHeader.Get("content-type") {
		t.Fatalf("new %d %q, taken %d %q: want the same bytes", newStatus, newBody, oldStatus, oldBody)
	}
	if newHashes != 1 || oldHashes != 1 {
		t.Errorf("password hashed %d times for a new address and %d for a taken one, want 1 and 1", newHashes, oldHashes)
	}
	if len(newRig.mail.sent) != 1 || len(oldRig.mail.sent) != 1 {
		t.Fatalf("mails: new %d, taken %d, want 1 and 1", len(newRig.mail.sent), len(oldRig.mail.sent))
	}
	if newRig.mail.sent[0].Purpose != PurposeEmailVerification || oldRig.mail.sent[0].Purpose != PurposeAccountExists {
		t.Errorf("purposes %q and %q", newRig.mail.sent[0].Purpose, oldRig.mail.sent[0].Purpose)
	}
}

// A slug collision that survives the free-slug read rolls the sign-up back. The
// answer is the accepted one, for any address: no mail, nothing to tell apart.
func TestRegisterSlugCollisionAnswersTheUniformAnswer(t *testing.T) {
	rig := newFlowRig(t, true)
	rig.f.accounts = collidingAccounts{Accounts: rig.f.accounts}
	status, body, _ := registerOver(t, registerHandler(rig.f, nil), "fresh@example.com")
	if status != http.StatusAccepted || body != registerAcceptedBody {
		t.Fatalf("answer %d %q, want the accepted bytes", status, body)
	}
	if len(rig.mail.sent) != 0 {
		t.Fatalf("mails %d, want none: no account exists", len(rig.mail.sent))
	}
}

type collidingAccounts struct{ Accounts }

func (collidingAccounts) ProvisionLocal(context.Context, ProvisionLocalRequest) (uuid.UUID, error) {
	return uuid.Nil, identity.ErrOrganizationSlugCollision
}
