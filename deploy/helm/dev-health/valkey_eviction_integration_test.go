//go:build integration

package devhealth_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// Valkey started with the args RENDERED by helm is filled past maxmemory with
// keys that carry a TTL, the way the rate-limit counters do. With the policy
// planned for prod (volatile-ttl) the keys without a TTL -- an ingest stream and a plain
// marker -- are never evicted; keys with a TTL are. The image is the test
// harness's mirrored Valkey image: the chart's pinned digest is not in the test
// image mirror.
func TestRenderedValkeyEvictionKeepsKeysWithoutATTL(t *testing.T) {
	requireHelmUnderCI(t)
	deployment, _ := valkeyDeployment(t, "--set", "valkey.maxmemoryPolicy=volatile-ttl", "--set", "valkey.maxmemory=4mb")
	if got := valkeyArg(t, deployment, "--maxmemory-policy"); got != "volatile-ttl" {
		t.Fatalf("the render carries --maxmemory-policy %q, want volatile-ttl", got)
	}
	container := at(deployment, "spec", "template", "spec", "containers", 0)
	image, _ := at(container, "image").(string)
	args := valkeyArgs(deployment)
	if !strings.HasPrefix(image, "valkey/valkey:") || len(args) == 0 {
		t.Fatalf("the rendered valkey container has image %q and args %v", image, args)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        containers.ValkeyImage,
			Cmd:          args,
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start the rendered valkey: %v", err)
	}
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer stopCancel()
		_ = c.Terminate(stopCtx)
	})
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "6379/tcp")
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(valkeygo.ClientOption{InitAddress: []string{host + ":" + port.Port()}, SelectDB: 1, DisableCache: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	deadline := time.Now().Add(60 * time.Second)
	for client.Do(ctx, client.B().Ping().Build()).Error() != nil {
		if time.Now().After(deadline) {
			t.Fatal("the rendered valkey never answered PING")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got, err := client.Do(ctx, client.B().ConfigGet().Parameter("maxmemory-policy").Build()).AsStrMap(); err != nil || got["maxmemory-policy"] != "volatile-ttl" {
		t.Fatalf("the running valkey reports maxmemory-policy %v (%v), want volatile-ttl", got, err)
	}

	const stream, marker, entries = "ingest:it:pull_requests", "ingest:it:marker", 200
	for i := 0; i < entries; i++ {
		cmd := client.B().Xadd().Key(stream).Id("*").FieldValue().FieldValue("payload", strings.Repeat("x", 100)).Build()
		if err := client.Do(ctx, cmd).Error(); err != nil {
			t.Fatalf("XADD %d: %v", i, err)
		}
	}
	if err := client.Do(ctx, client.B().Set().Key(marker).Value("1").Build()).Error(); err != nil {
		t.Fatal(err)
	}

	value := strings.Repeat("c", 1024)
	for i := 0; i < 12000; i++ {
		ttl := int64(600000 + (i%2)*3000000)
		cmd := client.B().Set().Key(fmt.Sprintf("dho:rl:fill_%d:k:p", i)).Value(value).PxMilliseconds(ttl).Build()
		if err := client.Do(ctx, cmd).Error(); err != nil {
			t.Fatalf("SET %d of a key with a TTL failed (volatile-ttl evicts keys with a TTL first): %v", i, err)
		}
	}

	evicted := infoField(t, ctx, client, "stats", "evicted_keys")
	if n, _ := strconv.Atoi(evicted); n == 0 {
		t.Fatalf("evicted_keys = %q after writing ~12 MiB into a 4 MiB Valkey: no memory pressure was reached", evicted)
	}
	if n, err := client.Do(ctx, client.B().Xlen().Key(stream).Build()).AsInt64(); err != nil || n != entries {
		t.Errorf("the ingest stream holds %d entries (%v) after eviction, want all %d", n, err, entries)
	}
	if n, err := client.Do(ctx, client.B().Exists().Key(marker).Build()).AsInt64(); err != nil || n != 1 {
		t.Errorf("the marker without a TTL is gone after eviction (exists=%d, %v)", n, err)
	}
	t.Logf("evicted_keys=%s", evicted)
}

func infoField(t *testing.T, ctx context.Context, client valkeygo.Client, section, field string) string {
	t.Helper()
	text, err := client.Do(ctx, client.B().Info().Section(section).Build()).ToString()
	if err != nil {
		t.Fatalf("INFO %s: %v", section, err)
	}
	for _, line := range strings.Split(text, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), field+":"); ok {
			return v
		}
	}
	t.Fatalf("INFO %s has no %s", section, field)
	return ""
}
