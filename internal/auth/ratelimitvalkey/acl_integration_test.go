//go:build integration

package ratelimitvalkey_test

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	valkeygo "github.com/valkey-io/valkey-go"
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
	rule := append([]string{"on", ">acl-test-password"}, strings.Fields(ratelimitvalkey.ACLRules)...)
	acl := admin.B().Arbitrary("ACL", "SETUSER", ratelimitvalkey.ACLUser).Args(rule...).Build()
	if err := admin.Do(ctx, acl).Error(); err != nil {
		t.Fatal(err)
	}
	login, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	login.User = url.UserPassword(ratelimitvalkey.ACLUser, "acl-test-password")
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

// Valkey started the way the chart starts it with valkey.acl.enabled (--aclfile on
// the Secret's users.acl: the default user line and the rate-limit user with a
// HASHED password) keeps the default user open for the probes and the other
// planes, lets the rate-limit user count under its prefix with its password, and
// refuses the wrong password. The point of the file is that this survives a
// restart (an ACL SETUSER on a running Valkey does not), so the container is
// stopped and started on the same file and every assertion is made again.
func TestTheAclFileTheChartMountsDefinesTheDefaultAndTheRateLimitUser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	const password = "acl-file-test-password"
	instance, err := containers.StartValkeyWithACLFile(ctx, ratelimitvalkey.ACLFile(password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	open := func(uri string) (valkeygo.Client, error) {
		client, openErr := valkeystore.Open(ctx, valkeystore.DefaultConfig(uri))
		if openErr == nil {
			t.Cleanup(client.Close)
		}
		return client, openErr
	}
	assertFile := func(phase, uri string, window time.Duration) {
		t.Helper()
		// The default user: no password, the probes' PING and any key.
		def, err := open(uri)
		if err != nil {
			t.Fatalf("%s: the default user cannot connect: %v", phase, err)
		}
		if err := def.Do(ctx, def.B().Set().Key("anything").Value("1").Build()).Error(); err != nil {
			t.Fatalf("%s: the default user lost its access: %v", phase, err)
		}
		login, err := url.Parse(uri)
		if err != nil {
			t.Fatal(err)
		}
		login.User = url.UserPassword(ratelimitvalkey.ACLUser, password)
		client, err := open(login.String())
		if err != nil {
			t.Fatalf("%s: the rate-limit user cannot connect: %v", phase, err)
		}
		store := newStore(t, client)
		own := httpapi.Limit{ID: "auth_svc_login_ip_" + phase, Count: 1, Window: window}
		if !hit(t, store, own, "k", "/p") || hit(t, store, own, "k", "/p") {
			t.Fatalf("%s: the rate-limit user did not count under its prefix", phase)
		}
		foreign := httpapi.Limit{ID: "auth_login_ip", Count: 20, Window: window}
		if _, err := httpapi.NewKeyedLimiter(store, foreign).Allow(ctx, "k", "/p"); err == nil {
			t.Fatalf("%s: the rate-limit user counted under go-api's prefix", phase)
		}
		login.User = url.UserPassword(ratelimitvalkey.ACLUser, "not-the-password")
		if _, err := open(login.String()); err == nil {
			t.Fatalf("%s: the wrong password was accepted", phase)
		}
	}
	assertFile("first", instance.URI, time.Hour)

	// Restart on the same file: stop, start, read the new mapped port.
	stopTimeout := 20 * time.Second
	if err := instance.Container.Stop(ctx, &stopTimeout); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := instance.Container.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}
	host, err := instance.Container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := instance.Container.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	restarted := url.URL{Scheme: "redis", Host: host + ":" + port.Port(), Path: "1"}
	var lastErr error
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		probe, err := valkeygo.NewClient(valkeygo.MustParseURL(restarted.String()))
		if err == nil {
			lastErr = probe.Do(ctx, probe.B().Ping().Build()).Error()
			probe.Close()
			if lastErr == nil {
				break
			}
		} else {
			lastErr = err
		}
	}
	if lastErr != nil {
		t.Fatalf("Valkey did not answer after the restart: %v", lastErr)
	}
	assertFile("restarted", restarted.String(), time.Hour)
}

func redactedURI(string) string { return "(uri withheld)" }
