package authprovision_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/orgreactivate"
)

// Core.Post is PostExpecting(200): every caller of Post meets the same outcome it met before PostExpecting existed. One
// cell per caller (every call site of Core.Post in authprovision and orgreactivate): a 2xx that is not 200 and a 3xx are
// unavailable, a 5xx is unavailable, a 4xx is a refusal that is not unavailable, whatever the body.
func TestEveryPostCallerKeepsItsNonSuccessOutcomes(t *testing.T) {
	ctx := context.Background()
	org, actor, other, id := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	callers := map[string]func(t *testing.T, s *usersServer) error{}
	provisioner := func(t *testing.T, s *usersServer) *authprovision.Client {
		base, workload := newWorkloadFor(t, s)
		client, err := authprovision.NewWorkload(base, workload, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	callers["AddMember"] = func(t *testing.T, s *usersServer) error {
		_, err := provisioner(t, s).AddMember(ctx, org, other, actor, "member", nil)
		return err
	}
	callers["ChangeRole"] = func(t *testing.T, s *usersServer) error {
		_, err := provisioner(t, s).ChangeRole(ctx, org, other, actor, "admin")
		return err
	}
	callers["RemoveMember"] = func(t *testing.T, s *usersServer) error {
		return provisioner(t, s).RemoveMember(ctx, org, other, actor)
	}
	callers["TransferOwnership"] = func(t *testing.T, s *usersServer) error {
		return provisioner(t, s).TransferOwnership(ctx, org, actor, other, actor)
	}
	callers["CreateInvite"] = func(t *testing.T, s *usersServer) error {
		_, err := provisioner(t, s).CreateInvite(ctx, authprovision.InviteRequest{OrgID: org, Email: "a@example.com", Role: "member", TTL: time.Hour, Actor: actor})
		return err
	}
	callers["AcceptInvite"] = func(t *testing.T, s *usersServer) error {
		_, err := provisioner(t, s).AcceptInvite(ctx, authprovision.AcceptRequest{Token: "t", PrincipalID: other})
		return err
	}
	callers["Deprovision"] = func(t *testing.T, s *usersServer) error { return provisioner(t, s).Deprovision(ctx, org, actor) }
	callers["OnboardOrganization"] = func(t *testing.T, s *usersServer) error {
		_, err := provisioner(t, s).OnboardOrganization(ctx, authprovision.OnboardRequest{PrincipalID: other, Name: "Acme"})
		return err
	}
	callers["CreateOrganization"] = func(t *testing.T, s *usersServer) error {
		_, err := provisioner(t, s).CreateOrganization(ctx, authprovision.CreateOrganizationRequest{Actor: actor, Slug: "acme", Name: "Acme"})
		return err
	}
	callers["CreateAccount"] = func(t *testing.T, s *usersServer) error {
		_, err := authprovision.NewAccountClient(newCoreFor(t, s)).CreateAccount(ctx, org, other, actor, "svc")
		return err
	}
	callers["DisableAccount"] = func(t *testing.T, s *usersServer) error {
		_, err := authprovision.NewAccountClient(newCoreFor(t, s)).DisableAccount(ctx, id, actor)
		return err
	}
	callers["IssueKey"] = func(t *testing.T, s *usersServer) error {
		_, err := authprovision.NewKeyClient(newCoreFor(t, s)).IssueKey(ctx, id, actor, nil)
		return err
	}
	callers["CreateUser"] = func(t *testing.T, s *usersServer) error {
		_, err := authprovision.NewUsersClient(newCoreFor(t, s)).CreateUser(ctx, authprovision.UserCreateRequest{Actor: actor, Email: "a@example.com"})
		return err
	}
	callers["UpdateUser"] = func(t *testing.T, s *usersServer) error {
		return authprovision.NewUsersClient(newCoreFor(t, s)).UpdateUser(ctx, authprovision.UserUpdateRequest{Actor: actor, Principal: other})
	}
	callers["DeleteUser"] = func(t *testing.T, s *usersServer) error {
		_, err := authprovision.NewUsersClient(newCoreFor(t, s)).DeleteUser(ctx, authprovision.UserDeleteRequest{Actor: actor, Principal: other, Mode: "soft"})
		return err
	}
	reactivate := func(t *testing.T, s *usersServer) *orgreactivate.Client {
		base, workload := newWorkloadFor(t, s)
		client, err := orgreactivate.NewWorkload(base, workload, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return client
	}
	callers["Reactivate"] = func(t *testing.T, s *usersServer) error { return reactivate(t, s).Reactivate(ctx, org, actor, nil) }
	callers["Suspend"] = func(t *testing.T, s *usersServer) error { return reactivate(t, s).Suspend(ctx, org, actor, nil) }
	callers["Rename"] = func(t *testing.T, s *usersServer) error { return reactivate(t, s).Rename(ctx, org, actor, "Acme") }
	callers["State"] = func(t *testing.T, s *usersServer) error {
		_, err := reactivate(t, s).State(ctx, org)
		return err
	}
	if len(callers) < 19 {
		t.Fatalf("%d callers in the table, want at least 19 (every one of the 16 Core.Post call sites, the shared ones through one method each)", len(callers))
	}
	unavailable := func(err error) bool {
		return errors.Is(err, authprovision.ErrUnavailable) || errors.Is(err, orgreactivate.ErrUnavailable)
	}
	for name, call := range callers {
		for _, status := range []int{http.StatusCreated, http.StatusAccepted, http.StatusNoContent, http.StatusFound, http.StatusInternalServerError, http.StatusServiceUnavailable} {
			server := &usersServer{answer: func(string) (int, string) { return status, `{"principal_id":"` + other.String() + `"}` }}
			if err := call(t, server); err == nil || !unavailable(err) {
				t.Errorf("%s answered %d: err = %v, want unavailable", name, status, err)
			}
		}
		for _, status := range []int{http.StatusBadRequest, http.StatusForbidden, http.StatusNotFound, http.StatusConflict} {
			server := &usersServer{answer: func(string) (int, string) { return status, `{"reason_code":"x_reason"}` }}
			if err := call(t, server); err == nil || unavailable(err) {
				t.Errorf("%s answered %d: err = %v, want a refusal that is not unavailable", name, status, err)
			}
		}
	}
}
