//go:build integration

package admin

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// On a real Valkey: the record lives between 120 s and its 150 s life, N concurrent consumes admit exactly one, another
// admin's consume leaves the record, and a deleted record is refused.
func TestHandoffRecordOnARealValkey(t *testing.T) {
	ctx := context.Background()
	instance, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	options, err := valkeygo.ParseURL(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	store := valkeyHandoffs{client: client}
	admin := uuid.NewString()

	id, err := newHandoffID()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.record(ctx, id, admin); err != nil {
		t.Fatal(err)
	}
	ttl, err := client.Do(ctx, client.B().Pttl().Key(handoffKeyPrefix+id).Build()).AsInt64()
	if err != nil || time.Duration(ttl)*time.Millisecond <= 120*time.Second || time.Duration(ttl)*time.Millisecond > handoffTTL {
		t.Fatalf("record PTTL %d ms (%v), want in (120 s, 150 s]", ttl, err)
	}
	if err := store.record(ctx, id, uuid.NewString()); err == nil {
		t.Fatalf("a second record write with the same id succeeded")
	}
	if ok, err := store.consume(ctx, id, uuid.NewString()); ok || err != nil {
		t.Fatalf("another admin's consume: %v %v, want refused", ok, err)
	}

	const n = 32
	results := make([]bool, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { results[i], errs[i] = store.consume(ctx, id, admin) })
	}
	wg.Wait()
	admitted := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatalf("consume %d: %v", i, errs[i])
		}
		if results[i] {
			admitted++
		}
	}
	if admitted != 1 {
		t.Fatalf("%d of %d concurrent consumes admitted, want 1", admitted, n)
	}

	gone := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	if err := store.record(ctx, gone, admin); err != nil {
		t.Fatal(err)
	}
	if err := client.Do(ctx, client.B().Del().Key(handoffKeyPrefix+gone).Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.consume(ctx, gone, admin); ok || err != nil {
		t.Fatalf("consume of a deleted record: %v %v, want refused", ok, err)
	}
}
