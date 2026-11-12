//go:build integration

package orgs

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// The licensing entitlements answer carries the org_licenses row's is_valid
// as stored: false for a lapsed license, true with no license row, and true
// for a row still marked valid whose expires_at has passed (legacy parity: the
// column is the answer, not the date). web refuses an ACR credential on
// is_valid false (CHAOS-11206 carry).
func TestEntitlementsAnswerTheStoredIsValid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, true)
	past := time.Now().UTC().Add(-24 * time.Hour)
	for _, c := range []struct {
		name      string
		license   bool
		isValid   bool
		expiresAt *time.Time
		want      bool
	}{
		{"no license row", false, false, nil, true},
		{"license is_valid false", true, false, nil, false},
		{"license is_valid true, expires_at past", true, true, &past, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			org := uuid.New()
			pgseed.Org(ctx, t, env.Admin, org.String(), "free")
			if c.license {
				pgseed.OrgLicense(ctx, t, env.Admin, org.String(), "team", "{}")
				if _, err := env.Admin.Exec(ctx, `UPDATE org_licenses SET is_valid = $2, expires_at = $3 WHERE org_id = $1`, org, c.isValid, c.expiresAt); err != nil {
					t.Fatal(err)
				}
			}
			r := httptest.NewRequest(http.MethodGet, "/api/v1/licensing/entitlements/"+org.String(), nil)
			r.SetPathValue("org_id", org.String())
			r = r.WithContext(policy.WithUser(r.Context(), &policy.User{OrgID: org.String()}))
			w := httptest.NewRecorder()

			handlers{pool: env.Admin, logger: slog.New(slog.DiscardHandler), now: time.Now}.entitlements(w, r)

			if w.Code != http.StatusOK {
				t.Fatalf("status %d body %s; want 200", w.Code, w.Body)
			}
			var body struct {
				IsValid *bool `json:"is_valid"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.IsValid == nil || *body.IsValid != c.want {
				t.Fatalf("body %s; want is_valid %v", w.Body, c.want)
			}
		})
	}
}
