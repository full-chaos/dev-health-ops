package session

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

const byteLimitViolation = "Password must be no more than 72 bytes long"

// The three passwords of the byte-limit cells: 72 ASCII bytes (accepted),
// 73 ASCII bytes, and 38 characters that are 73 bytes (35 two-byte letters
// and "Aa1"): under 72 characters, over 72 bytes.
var (
	passwordAt72Bytes = "Aa1" + strings.Repeat("x", 69)
	passwordAt73Bytes = "Aa1" + strings.Repeat("x", 70)
	passwordMultiByte = "Aa1" + strings.Repeat("é", 35)
	quietPasswordLogs = slog.New(slog.NewTextHandler(io.Discard, nil))
)

// reachesPool runs a handler that has no pool: a panic means it went past
// the password check to the transaction.
func reachesPool(run func()) (reached bool) {
	defer func() {
		if recover() != nil {
			reached = true
		}
	}()
	run()
	return false
}

func TestPasswordByteLimitCellsHoldTheirSizes(t *testing.T) {
	for password, want := range map[string]int{passwordAt72Bytes: 72, passwordAt73Bytes: 73, passwordMultiByte: 73} {
		if len(password) != want {
			t.Errorf("a cell is %d bytes, want %d", len(password), want)
		}
	}
	if n := len([]rune(passwordMultiByte)); n >= 72 {
		t.Errorf("the multi-byte cell is %d characters, want under 72", n)
	}
}

func wantByteRefusal(t *testing.T, rec *httptest.ResponseRecorder, password string) {
	t.Helper()
	if rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status %d, want 422; body %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), byteLimitViolation) {
		t.Errorf("body %s lacks the byte refusal", rec.Body)
	}
	if strings.Contains(rec.Body.String(), password) {
		t.Errorf("body %s echoes the password", rec.Body)
	}
}

func registerWith(password string) (rec *httptest.ResponseRecorder, reached bool) {
	rec = httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/register", nil)
	r = r.WithContext(context.WithValue(r.Context(), registerInputKey{}, registerInput{email: "a@example.com", password: password}))
	reached = reachesPool(func() { handlers{}.register(rec, r) })
	return rec, reached
}

func TestRegisterRefusesAPasswordOverTheByteLimitBeforeHashing(t *testing.T) {
	for name, password := range map[string]string{"73 bytes": passwordAt73Bytes, "multi-byte under 72 characters": passwordMultiByte} {
		rec, reached := registerWith(password)
		if reached {
			t.Fatalf("%s: register went on to the transaction", name)
		}
		wantByteRefusal(t, rec, password)
		const want = `{"detail":{"message":"Password validation failed","errors":["Password must be no more than 72 bytes long"]}}`
		if strings.TrimSpace(rec.Body.String()) != want {
			t.Errorf("%s: body %s, want the parity table's shape %s", name, rec.Body, want)
		}
	}
}

func TestRegisterAcceptsA72BytePasswordPastThePolicy(t *testing.T) {
	if _, reached := registerWith(passwordAt72Bytes); !reached {
		t.Fatal("a 72-byte password was refused")
	}
}

func changeWith(password string) (rec *httptest.ResponseRecorder, reached bool) {
	rec = httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/change-password", nil)
	ctx := policy.WithUser(r.Context(), &policy.User{UserID: "10000000-0000-4000-8000-000000000001"})
	ctx = context.WithValue(ctx, changePasswordInputKey{}, changePasswordInput{current: "whatever-current-1", next: password})
	h := handlers{Deps: Deps{Logger: quietPasswordLogs}}
	reached = reachesPool(func() { h.changePassword(rec, r.WithContext(ctx)) })
	return rec, reached
}

func TestChangePasswordRefusesAPasswordOverTheByteLimitBeforeHashing(t *testing.T) {
	for name, password := range map[string]string{"73 bytes": passwordAt73Bytes, "multi-byte under 72 characters": passwordMultiByte} {
		rec, reached := changeWith(password)
		if reached {
			t.Fatalf("%s: change-password went on to the transaction", name)
		}
		wantByteRefusal(t, rec, password)
	}
}

func TestChangePasswordAcceptsA72BytePasswordPastThePolicy(t *testing.T) {
	if _, reached := changeWith(passwordAt72Bytes); !reached {
		t.Fatal("a 72-byte password was refused")
	}
}
