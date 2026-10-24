//go:build integration

package ratelimitvalkey_test

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The ACL line the auth-service runbook documents is enough for the store and
// reaches nothing outside the auth_svc_ prefix: a go-api limit id is refused.
func TestTheAuthServiceLoginCountsUnderItsPrefixAndNowhereElse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	admin, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	acl := admin.B().Arbitrary("ACL", "SETUSER", "auth-service-ratelimit").
		Args("on", ">acl-test-password", "resetkeys", "~dho:rl:auth_svc_*", "resetchannels", "-@all",
			"+hello", "+select", "+ping", "+get", "+eval", "+incr", "+pexpire", "+client|tracking").Build()
	if err := admin.Do(ctx, acl).Error(); err != nil {
		t.Fatal(err)
	}
	login, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	login.User = url.UserPassword("auth-service-ratelimit", "acl-test-password")
	client, err := valkeystore.Open(ctx, valkeystore.DefaultConfig(login.String()))
	if err != nil {
		t.Fatalf("the ACL login cannot connect: %v", err)
	}
	t.Cleanup(client.Close)
	store := newStore(t, client)
	own := httpapi.Limit{ID: "auth_svc_login_ip", Count: 2, Window: time.Hour}
	if !hit(t, store, own, "k", "/p") || !hit(t, store, own, "k", "/p") {
		t.Fatal("the first two hits under the auth_svc_ prefix were refused")
	}
	if hit(t, store, own, "k", "/p") {
		t.Fatal("the third hit was allowed")
	}
	if _, err := store.Peek(ctx, httpapi.Hit{Limit: own, Key: "k", Path: "/p"}); err != nil {
		t.Fatalf("peek under the prefix: %v", err)
	}
	foreign := httpapi.Limit{ID: "auth_login_ip", Count: 20, Window: time.Hour}
	if _, err := httpapi.NewKeyedLimiter(store, foreign).Allow(ctx, "k", "/p"); err == nil {
		t.Fatal("the auth-service login counted under go-api's prefix")
	}
	if _, err := store.Peek(ctx, httpapi.Hit{Limit: foreign, Key: "k", Path: "/p"}); err == nil {
		t.Fatal("the auth-service login read go-api's counter")
	}
}
