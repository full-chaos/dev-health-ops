//go:build integration

package heatmap

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/crossorg"
)

func TestAPersonWhoseNewestRowsAreRetractionsDoesNotResolve(t *testing.T) {
	ctx := context.Background()
	admin, client := crossorg.Start(ctx, t)
	f := crossorg.Default()
	crossorg.SeedRepos(ctx, t, admin, f)
	crossorg.SeedRetractedPeople(ctx, t, admin, f, time.Date(2026, 9, 15, 0, 0, 0, 0, time.UTC))
	for name, want := range map[string]bool{"alice": true, "bob": false, "carol": true} {
		sum := md5.Sum([]byte(name + "@example.com"))
		identity, err := resolvePersonIdentity(ctx, client, hex.EncodeToString(sum[:]), f.OrgA)
		if err != nil {
			t.Fatal(err)
		}
		if (identity != "") != want {
			t.Errorf("resolvePersonIdentity(%s) = %q, want found=%v", name, identity, want)
		}
	}
}
