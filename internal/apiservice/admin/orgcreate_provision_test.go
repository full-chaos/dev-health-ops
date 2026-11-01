package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

const (
	orgCreateSlugRead     = "SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE lower(slug) = $1"
	orgCreateOwnerRead    = "SELECT id, email, is_active, is_superuser FROM users WHERE id = $1"
	orgCreateOrgInsert    = "INSERT INTO organizations (id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
	orgCreateMemberRead   = "SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2"
	orgCreateMemberInsert = "INSERT INTO memberships (id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)"

	orgCreateOwnerEmail = "owner@example.com"
)

var orgCreateWithOwner = `{"name": "Acme", "owner_user_id": "` + provOwner.String() + `"}`

// orgCreateDB is a provisionDB on its own type, so the create cells share one seed.
type orgCreateDB struct{ *provisionDB }

func newOrgCreateDB() *orgCreateDB {
	email := orgCreateOwnerEmail
	return &orgCreateDB{provisionDB: &provisionDB{rows: map[string][]any{
		orgCreateOwnerRead: {provOwner, email, true, false},
	}}}
}

// orgCreateProvisioner is the fake the create cells count calls on.
type orgCreateProvisioner struct{ fakeProvisioner }

// callCreateOrg runs POST /api/v1/admin/orgs as the superuser provActor; auth
// nil is the switch off. It returns the answer and the log text.
func callCreateOrg(db *orgCreateDB, auth authprovision.Provisioner, body string) (int, string, string) {
	h := newProvisionHandlers(db.provisionDB, auth)
	h.store.Pool = db
	var logs bytes.Buffer
	h.logger = slog.New(slog.NewTextHandler(&logs, nil))
	r := answerWith(h.createOrganization, http.MethodPost, func(req *http.Request) *http.Request {
		req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true, UserID: validUUID, ID: provActor, Email: "root@example.com"}))
		return withBody(body)(req)
	})
	return r.Code, strings.TrimSpace(r.Body.String()), logs.String()
}

func internalBody() string {
	r := answerWith(func(w http.ResponseWriter, _ *http.Request) { policy.WriteInternal(w) }, http.MethodPost, nil)
	return strings.TrimSpace(r.Body.String())
}

func unavailableBody() string {
	r := answerWith(func(w http.ResponseWriter, _ *http.Request) {
		policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(sessionissue.UnavailableText), nil)
	}, http.MethodPost, nil)
	return strings.TrimSpace(r.Body.String())
}

func assertNoAudit(t *testing.T, log []string) {
	t.Helper()
	for _, step := range log {
		if strings.Contains(step, "audit_logs") {
			t.Fatalf("an audit row was written: %v", log)
		}
	}
}

// The switch-off create with an owner: today's one transaction, pinned.
var orgCreateSwitchOffSteps = []string{orgCreateSlugRead, "BEGIN", "tx:" + orgCreateOrgInsert, "tx:" + orgCreateMemberRead, "tx:" + orgCreateMemberInsert, "COMMIT", "ROLLBACK"}

// U7: the switch off makes no auth call and runs today's statements in
// today's order; the answer is 201 with the organization.
func TestOrgCreateSwitchOffRunsTodaysSQL(t *testing.T) {
	db := newOrgCreateDB()
	code, body, _ := callCreateOrg(db, nil, orgCreateWithOwner)
	if code != http.StatusCreated {
		t.Fatalf("answer %d %s", code, body)
	}
	if !reflect.DeepEqual(db.log, orgCreateSwitchOffSteps) {
		t.Fatalf("switch-off steps:\n got %q\nwant %q", db.log, orgCreateSwitchOffSteps)
	}
}

// An owner go-api does not hold answers today's 500 (the foreign key failure),
// before any write or auth call.
func TestOrgCreateSwitchOnUnknownOwnerIsTodays500(t *testing.T) {
	db := newOrgCreateDB()
	delete(db.rows, orgCreateOwnerRead)
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}}
	code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner)
	if code != http.StatusInternalServerError || body != internalBody() {
		t.Fatalf("answer %d %s, want today's 500", code, body)
	}
	if want := []string{orgCreateSlugRead, orgCreateOwnerRead}; !reflect.DeepEqual(db.log, want) {
		t.Fatalf("steps %q, want %q", db.log, want)
	}
}

var operatorCreateOrgSQL = []string{"BEGIN",
	"tx:SELECT id, email, username, password_hash, full_name, avatar_url, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, last_login_at, created_at, updated_at FROM users WHERE lower(email) = $1",
	"tx:" + orgCreateSlugRead, "tx:" + orgCreateOrgInsert, "tx:" + orgCreateMemberRead, "tx:" + orgCreateMemberInsert, "COMMIT", "ROLLBACK"}

func TestOperatorCreateOrgSwitchOffRunsTodaysSQL(t *testing.T) {
	db := &provisionDB{rows: map[string][]any{"FROM users WHERE lower(email) = $1": {zeroFill{}}}}
	if _, err := (Operator{db: db}).CreateOrg(context.Background(), CreateOrgInput{Name: "Acme", Tier: "community", OwnerEmail: strp("a@example.test")}); err != nil {
		t.Fatalf("switch off: %v", err)
	}
	if !reflect.DeepEqual(db.log, operatorCreateOrgSQL) {
		t.Fatalf("switch-off SQL:\n got %q\nwant %q", db.log, operatorCreateOrgSQL)
	}
}

const orgCreateIDRead = "FROM organizations WHERE id = $1"

func orgBornRow() []any {
	description := "made here"
	return []any{provOrg, "acme", "Acme", &description, []byte(`{"a":1}`), "team", "manual", true, provStamp, provStamp}
}

// The switch on: every create goes to auth-service, with or without an owner;
// go-api writes neither row, reads the created row back and answers it.
func TestAdminCreateGoesToAuthWithTheSwitchOn(t *testing.T) {
	for name, body := range map[string]string{
		"owner":    `{"name": "Acme", "slug": "acme", "description": "made here", "tier": "team", "settings": {"a": 1}, "owner_user_id": "` + provOwner.String() + `"}`,
		"no owner": `{"name": "Acme", "slug": "acme", "description": "made here", "tier": "team", "settings": {"a": 1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			db := newOrgCreateDB()
			db.rows[orgCreateIDRead] = orgBornRow()
			auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB}}
			code, answer, _ := callCreateOrg(db, auth, body)
			if code != http.StatusCreated || !strings.Contains(answer, `"slug":"acme"`) && !strings.Contains(answer, `"slug": "acme"`) {
				t.Fatalf("answer %d %s", code, answer)
			}
			if len(auth.calls) != 1 || auth.calls[0] != "create" {
				t.Fatalf("auth calls %v, want one create", auth.calls)
			}
			in := auth.args[0][0].(authprovision.CreateOrganizationRequest)
			if in.Actor != provActor || in.Slug != "acme" || in.Name != "Acme" || in.Tier != "team" || in.ManagedBy != "manual" ||
				in.Description == nil || *in.Description != "made here" || string(in.Settings) != `{"a":1}` {
				t.Fatalf("request %+v", in)
			}
			if (name == "owner") != (in.Owner != nil && *in.Owner == provOwner) {
				t.Fatalf("owner %v", in.Owner)
			}
			for _, step := range db.log {
				if strings.Contains(step, "INSERT") || step == "BEGIN" {
					t.Fatalf("go-api wrote: %q", db.log)
				}
			}
		})
	}
}

func TestAdminCreateRefusalAnswers(t *testing.T) {
	for reason, want := range map[string]int{"superuser_required": http.StatusForbidden, "admin_required": http.StatusForbidden, "principal_deactivated": http.StatusServiceUnavailable} {
		db := newOrgCreateDB()
		auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB, err: &authprovision.Refused{Status: want, Reason: reason}}}
		if code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner); code != want {
			t.Fatalf("%s: %d %s", reason, code, body)
		}
	}
	db := newOrgCreateDB()
	auth := &orgCreateProvisioner{fakeProvisioner: fakeProvisioner{db: db.provisionDB, err: authprovision.ErrUnavailable}}
	if code, body, _ := callCreateOrg(db, auth, orgCreateWithOwner); code != http.StatusServiceUnavailable || body != unavailableBody() {
		t.Fatalf("auth down: %d %s", code, body)
	}
	assertNoAudit(t, db.log)
}

func TestAdminCreateUnchangedSwitchOff(t *testing.T) {
	db := newOrgCreateDB()
	if code, body, _ := callCreateOrg(db, nil, `{"name": "Acme"}`); code != http.StatusCreated {
		t.Fatalf("answer %d %s", code, body)
	}
	want := []string{orgCreateSlugRead, "BEGIN", "tx:" + orgCreateOrgInsert, "COMMIT", "ROLLBACK"}
	if !reflect.DeepEqual(db.log, want) {
		t.Fatalf("steps:\n got %q\nwant %q", db.log, want)
	}
}

func TestOperatorCreateOrgRefusedWithTheSwitchOn(t *testing.T) {
	db := &provisionDB{}
	_, err := Operator{db: db, OrgProvisioning: true}.CreateOrg(context.Background(), CreateOrgInput{Name: "Acme", Tier: "community"})
	if !errors.Is(err, ErrOrgProvisioningOn) || len(db.log) != 0 {
		t.Fatalf("err %v, database calls %v; want ErrOrgProvisioningOn and none", err, db.log)
	}
}
