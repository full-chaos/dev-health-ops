//go:build integration

package adminops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// fakeActor is the --actor the harness gives every verb that needs one. The
// fake auth-service takes any actor: the actor rule is auth-service's own and
// has its own cells.
const fakeActor = "0f000000-0000-4000-8000-00000000000f"

const fakeToken = "svc_authsess_" + "0123456789abcdef0123456789abcdef0123456789a"

// needsProvisioning reports whether args is a verb that goes through
// auth-service's provisioning routes: a membership change or a real delete.
func needsProvisioning(args []string) bool {
	if len(args) < 2 {
		return false
	}
	switch args[0] + " " + args[1] {
	case "users update":
		for _, arg := range args {
			if arg == "--org" || arg == "--remove-from-org" {
				return true
			}
		}
	case "orgs delete":
		for _, arg := range args {
			if arg == "--dry-run" {
				return false
			}
		}
		return true
	}
	return false
}

// startFakeAuthService serves the provisioning routes the CLI calls over the
// database at uri, doing what the in-process membership writes did before the
// CLI went through auth-service, so every verb's stdout and rows stay the
// ones the frozen Python session recorded.
func startFakeAuthService(t *testing.T, uri string) string {
	t.Helper()
	mux := http.NewServeMux()
	answer := func(w http.ResponseWriter, status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(body)
	}
	refuse := func(w http.ResponseWriter, status int, reason string) {
		answer(w, status, map[string]string{"reason_code": reason})
	}
	handle := func(path string, run func(ctx context.Context, conn *pgx.Conn, in map[string]string) (int, any)) {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+fakeToken {
				refuse(w, http.StatusUnauthorized, "invalid_credentials")
				return
			}
			var in map[string]string
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				refuse(w, http.StatusBadRequest, "malformed")
				return
			}
			conn, err := pgx.Connect(r.Context(), uri)
			if err != nil {
				refuse(w, http.StatusServiceUnavailable, "store_unavailable")
				return
			}
			defer conn.Close(context.Background())
			status, body := run(r.Context(), conn, in)
			if reason, ok := body.(string); ok {
				refuse(w, status, reason)
				return
			}
			answer(w, status, body)
		})
	}
	membership := func(in map[string]string, role *string, status string) map[string]any {
		return map[string]any{"membership_id": uuid.NewString(), "organization_id": in["organization_id"],
			"principal_id": in["principal_id"], "role": role, "status": status}
	}
	currentRole := func(ctx context.Context, conn *pgx.Conn, in map[string]string) (string, bool, error) {
		var role string
		err := conn.QueryRow(ctx, `SELECT role FROM memberships WHERE org_id = $1 AND user_id = $2`, in["organization_id"], in["principal_id"]).Scan(&role)
		if err == pgx.ErrNoRows {
			return "", false, nil
		}
		return role, err == nil, err
	}
	owners := func(ctx context.Context, conn *pgx.Conn, in map[string]string) (int, error) {
		var n int
		err := conn.QueryRow(ctx, `SELECT count(*) FROM memberships WHERE org_id = $1 AND role = 'owner'`, in["organization_id"]).Scan(&n)
		return n, err
	}
	validRole := func(role string) bool {
		return role == "owner" || role == "admin" || role == "member" || role == "viewer"
	}

	handle("/internal/v1/memberships/add", func(ctx context.Context, conn *pgx.Conn, in map[string]string) (int, any) {
		role := in["role"]
		if !validRole(role) {
			return http.StatusBadRequest, "role_invalid"
		}
		if _, found, err := currentRole(ctx, conn, in); err != nil {
			return http.StatusServiceUnavailable, "store_unavailable"
		} else if found {
			return http.StatusConflict, "already_member"
		}
		now := time.Now().UTC()
		if _, err := conn.Exec(ctx, `INSERT INTO memberships (id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, NULL, $5, $5, $5)`, uuid.New(), in["organization_id"], in["principal_id"], role, now); err != nil {
			return http.StatusServiceUnavailable, "store_unavailable"
		}
		return http.StatusOK, membership(in, &role, "active")
	})
	handle("/internal/v1/memberships/role", func(ctx context.Context, conn *pgx.Conn, in map[string]string) (int, any) {
		role := in["role"]
		current, found, err := currentRole(ctx, conn, in)
		switch {
		case err != nil:
			return http.StatusServiceUnavailable, "store_unavailable"
		case !found:
			return http.StatusNotFound, "membership_not_found"
		case !validRole(role):
			return http.StatusBadRequest, "role_invalid"
		case current == role:
			return http.StatusOK, membership(in, &role, "active")
		}
		if current == "owner" {
			if n, err := owners(ctx, conn, in); err != nil {
				return http.StatusServiceUnavailable, "store_unavailable"
			} else if n <= 1 {
				return http.StatusConflict, "last_owner"
			}
		}
		if _, err := conn.Exec(ctx, `UPDATE memberships SET role = $3, updated_at = $4 WHERE org_id = $1 AND user_id = $2`,
			in["organization_id"], in["principal_id"], role, time.Now().UTC()); err != nil {
			return http.StatusServiceUnavailable, "store_unavailable"
		}
		return http.StatusOK, membership(in, &role, "active")
	})
	handle("/internal/v1/memberships/remove", func(ctx context.Context, conn *pgx.Conn, in map[string]string) (int, any) {
		current, found, err := currentRole(ctx, conn, in)
		switch {
		case err != nil:
			return http.StatusServiceUnavailable, "store_unavailable"
		case !found:
			return http.StatusNotFound, "membership_not_found"
		}
		if current == "owner" {
			if n, err := owners(ctx, conn, in); err != nil {
				return http.StatusServiceUnavailable, "store_unavailable"
			} else if n <= 1 {
				return http.StatusConflict, "last_owner"
			}
		}
		now := time.Now().UTC()
		if _, err := conn.Exec(ctx, `DELETE FROM memberships WHERE org_id = $1 AND user_id = $2`, in["organization_id"], in["principal_id"]); err != nil {
			return http.StatusServiceUnavailable, "store_unavailable"
		}
		if _, err := conn.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = $3 WHERE user_id = $1 AND org_id = $2 AND revoked_at IS NULL`,
			in["principal_id"], in["organization_id"], now); err != nil {
			return http.StatusServiceUnavailable, "store_unavailable"
		}
		return http.StatusOK, membership(in, nil, "removed")
	})
	handle("/internal/v1/organizations/deprovision", func(_ context.Context, _ *pgx.Conn, in map[string]string) (int, any) {
		return http.StatusOK, map[string]string{"organization_id": in["organization_id"]}
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return strings.TrimSuffix(server.URL, "/")
}
