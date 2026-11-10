//go:build integration

package admin_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pinrig"
)

// A session signed in before an admin password set, through the route or the
// operator command, is refused by go-api's session check afterwards; one
// signed in after the write stays live.
func TestAdminPasswordWritesEndEarlierAuthSessions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pool, instance := migratedPostgres(t, ctx)
	rig := pinrig.NewPinRig(t, ctx, pool, instance)

	const rootPassword = "pin root password 91"
	rootHash, err := bcrypt.GenerateFromPassword([]byte(rootPassword), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	users := map[string]uuid.UUID{"root": uuid.New(), "route": uuid.New(), "operator": uuid.New()}
	for name, id := range users {
		if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, $3, true, true, $4, 0, now(), now())`, id, "pin-"+name+"@example.com", string(rootHash), name == "root"); err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
	}
	check := func(name string, user, sid uuid.UUID, wantRefused bool) {
		t.Helper()
		err := rig.Check(ctx, user, sid)
		if got := pinrig.Refused(err); got != wantRefused || (!got && err != nil) {
			t.Fatalf("%s: err = %v, refused = %v, want refused = %v", name, err, got, wantRefused)
		}
	}

	tokens := edtokens.New(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth := tokens.Authenticator(t, policy.PGStore{Pool: pool}, logger)
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/users") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	bearer := tokens.MustMint(t, edtokens.Claims{UserID: users["root"]})

	oldRoute := rig.SignIn(t, ctx, users["route"])
	check("route: before", users["route"], oldRoute, false)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/api/v1/admin/users/"+users["route"].String()+"/password",
		strings.NewReader(fmt.Sprintf(`{"admin_password":%q,"password":"Another Strong Pass 42!"}`, rootPassword)))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+bearer)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("admin password set: %d", response.StatusCode)
	}
	check("route: older session after", users["route"], oldRoute, true)
	check("route: newer session after", users["route"], rig.SignIn(t, ctx, users["route"]), false)

	oldOperator := rig.SignIn(t, ctx, users["operator"])
	check("operator: before", users["operator"], oldOperator, false)
	password := "Operator Strong Pass 42!"
	id := users["operator"].String()
	if _, err := (admin.Operator{Pool: pool, Now: time.Now}).UpdateUser(ctx, admin.UpdateUserInput{ID: &id, Password: &password}); err != nil {
		t.Fatalf("operator password update: %v", err)
	}
	check("operator: older session after", users["operator"], oldOperator, true)
	check("operator: newer session after", users["operator"], rig.SignIn(t, ctx, users["operator"]), false)
}
