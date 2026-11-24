package providersync

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// The rate-limit deferral commits and its reply is lost (a COMMIT fault of
// class 08 on the scripted wire server applies the write, then drops the
// connection). The deferral stamped the unit dispatching with a not-before
// fence, so the right answer is success; the read-back is what finds it.
func TestRateLimitDeferralWithALostReplyAfterTheCommitIsReadBack(t *testing.T) {
	t.Parallel()
	w := &storeWire{faults: []*wireFault{{step: "commit", code: "08006", times: 1}}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	claim := nativeTestClaim("github", "commits")
	w.sourceID = claim.SourceID
	repository, err := NewPostgresRepository(w.pool(t, ctx), providerfoundation.NewMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	repository.storeRetrySleep = func(context.Context, int) error { return nil }
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	defer slog.SetDefault(previous)
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	err = repository.DeferForRateLimit(ctx, claim, now.Add(time.Minute), now, RateLimitObservation{}, RateLimitEpisodeStep{})
	if err != nil || w.updates != 1 || w.commits != 1 || w.reads != 1 {
		t.Fatalf("err=%v updates=%d commits=%d reads=%d, want nil, 1, 1, 1 (committed, reply lost, read back once)",
			err, w.updates, w.commits, w.reads)
	}
}
