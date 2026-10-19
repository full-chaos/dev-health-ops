//go:build integration

package sessionscenario

import (
	"context"
	"testing"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func TestLimitResetKeepsUnrelatedValkeyKeys(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	vk, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = vk.Close(context.Background()) })
	options, err := valkeygo.ParseURL(vk.URI)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, key := range []string{"dho:rl:login:a:b", "dho:rl:refresh:c:d", "unrelated:key"} {
		if err := client.Do(ctx, client.B().Set().Key(key).Value("1").Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	Target{ValkeyURI: vk.URI}.resetLimits(t, ctx)
	for key, want := range map[string]bool{"dho:rl:login:a:b": false, "dho:rl:refresh:c:d": false, "unrelated:key": true} {
		got, err := client.Do(ctx, client.B().Exists().Key(key).Build()).AsInt64()
		if err != nil {
			t.Fatal(err)
		}
		if (got == 1) != want {
			t.Errorf("key %q present=%v after the limit reset, want %v", key, got == 1, want)
		}
	}
}
