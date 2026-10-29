//go:build integration

package authruntime

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
)

// proxyClient is go-api's provisioning client against the live routes, with
// the go-api credential of newProvisioningEnv.
func proxyClient(t *testing.T, e *routesEnv, header map[string]string) *authprovision.Client {
	t.Helper()
	base, err := url.Parse(e.front.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := authprovision.New(base, strings.TrimPrefix(header["Authorization"], "Bearer "), 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func wantRefused(t *testing.T, step string, err error, status int, reason string) {
	t.Helper()
	var refused *authprovision.Refused
	if !errors.As(err, &refused) || refused.Status != status || refused.Reason != reason {
		t.Fatalf("%s: %v, want %d %s", step, err, status, reason)
	}
}

// Each member verb through go-api's client: the auth commit writes both
// planes, and a repeat of a verb that committed answers the defined refusal
// go-api maps (PARITY-10988), so a retry after a go-api failure is safe.
func TestProxyClientMemberVerbsAndTheirRepeats(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	client := proxyClient(t, e, header)
	owner, org := e.seed(t, ctx)
	member := e.account(t, ctx, acctMail(), false, true)

	added, err := client.AddMember(ctx, org, member, owner, "member", nil)
	if err != nil || added.Status != "active" {
		t.Fatalf("add: %+v %v", added, err)
	}
	if pub, auth := e.memberRows(t, ctx, org, member); pub != "member" || auth != "active" {
		t.Fatalf("after add: public %q auth %q", pub, auth)
	}
	_, err = client.AddMember(ctx, org, member, owner, "member", nil)
	wantRefused(t, "add again", err, 409, "already_member")

	if _, err := client.ChangeRole(ctx, org, member, owner, "admin"); err != nil {
		t.Fatalf("role: %v", err)
	}
	if pub, _ := e.memberRows(t, ctx, org, member); pub != "admin" {
		t.Fatalf("after role: public %q", pub)
	}

	// CHAOS-11779: a removal through go-api leaves no active auth row.
	if err := client.RemoveMember(ctx, org, member, owner); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if pub, auth := e.memberRows(t, ctx, org, member); pub != "" || auth != "removed" {
		t.Fatalf("after remove: public %q auth %q, want no public row and auth removed", pub, auth)
	}
	wantRefused(t, "remove again", client.RemoveMember(ctx, org, member, owner), 404, "membership_not_found")
	wantRefused(t, "remove the last owner", client.RemoveMember(ctx, org, owner, owner), 409, "last_owner")

	next := e.account(t, ctx, acctMail(), false, true)
	if _, err := client.AddMember(ctx, org, next, owner, "member", nil); err != nil {
		t.Fatal(err)
	}
	if err := client.TransferOwnership(ctx, org, owner, next, owner); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if pub, _ := e.memberRows(t, ctx, org, next); pub != "owner" {
		t.Fatalf("after transfer: %q", pub)
	}
	wantRefused(t, "transfer from a former owner", client.TransferOwnership(ctx, org, owner, next, next), 409, "not_owner")
}

// X1 invite create: auth committed the invite and go-api's audit row, then
// go-api failed (the mail, or the re-read). A repeat is the 409 go-api maps to
// today's pending-invite answer; nothing is written twice.
func TestProxyClientInviteCreateRepeatIsThePendingRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	client := proxyClient(t, e, header)
	owner, org := e.seed(t, ctx)
	address := acctMail()
	request := authprovision.InviteRequest{OrgID: org, Email: address, Role: "member", TTL: 72 * time.Hour, Actor: owner,
		Description: "Organization invite created", RequestMetadata: []byte(`{"ip_address": "198.51.100.7"}`)}
	created, err := client.CreateInvite(ctx, request)
	if err != nil || created.Token == "" {
		t.Fatalf("create: %+v %v", created, err)
	}
	_, err = client.CreateInvite(ctx, request)
	wantRefused(t, "create again", err, 409, "invite_pending")
	var invites, audits int
	if err := e.admin.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM public.org_invites WHERE org_id = $1),
  (SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_invited')`, org).Scan(&invites, &audits); err != nil {
		t.Fatal(err)
	}
	if invites != 1 || audits != 1 {
		t.Fatalf("%d invites, %d member_invited rows; want 1 and 1", invites, audits)
	}
}

// X1 accept: auth committed the membership, then go-api failed to issue the
// tokens. A repeat is the 400 invite_invalid go-api maps to today's "Invalid
// or expired invite"; the membership stands, the caller signs in again.
func TestProxyClientAcceptRepeatIsTheInvalidInviteRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	client := proxyClient(t, e, header)
	owner, org := e.seed(t, ctx)
	address := acctMail()
	created, err := client.CreateInvite(ctx, authprovision.InviteRequest{OrgID: org, Email: address, Role: "member", TTL: time.Hour, Actor: owner})
	if err != nil {
		t.Fatal(err)
	}
	invitee := e.account(t, ctx, address, false, true)
	accept := authprovision.AcceptRequest{Token: created.Token, PrincipalID: invitee, Description: "Invite accepted"}
	accepted, err := client.AcceptInvite(ctx, accept)
	if err != nil || accepted.OrgID != org || accepted.Role != "member" {
		t.Fatalf("accept: %+v %v", accepted, err)
	}
	_, err = client.AcceptInvite(ctx, accept)
	wantRefused(t, "accept again", err, 400, "invite_invalid")
	if pub, auth := e.memberRows(t, ctx, org, invitee); pub != "member" || auth != "active" {
		t.Fatalf("after the repeat: public %q auth %q", pub, auth)
	}
	var joined int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM public.audit_logs WHERE org_id = $1 AND action = 'member_joined'`, org).Scan(&joined); err != nil || joined != 1 {
		t.Fatalf("%d member_joined rows (%v), want 1", joined, err)
	}
}

// X1 deprovision: auth committed, then a go-api purge step failed. The repeat
// of the delete calls deprovision again: a 200 that changes nothing, so the
// purge runs again.
func TestProxyClientDeprovisionRepeatIsANoOp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e, header := newProvisioningEnv(t, ctx)
	client := proxyClient(t, e, header)
	_, org := e.seed(t, ctx)
	super := e.account(t, ctx, acctMail(), true, true)
	if err := client.Deprovision(ctx, org, super); err != nil {
		t.Fatalf("deprovision: %v", err)
	}
	if err := client.Deprovision(ctx, org, super); err != nil {
		t.Fatalf("repeat deprovision: %v, want the no-op 200", err)
	}
}
