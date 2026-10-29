package revisionfeed

import (
	"context"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
)

// ErrNotUsableAsFloorSource: the store fails the status rule (ReadStatus), so the fence ignores its floors. The error text
// carries the closed Status.Reason and nothing else.
var ErrNotUsableAsFloorSource = errors.New("revisionfeed: not usable as a floor source")

// ErrNoFloors: the hash holds no floor. The fence counts a consumer input that reads fine as a usable source, which stops
// the blind margin from applying; an empty hash (a fresh, flushed or lost Valkey) is no evidence of "no revision above 0", so
// the input reports it as an error and the fence skips it ("read_failed"). Never an empty map, never a zero floor.
var ErrNoFloors = errors.New("revisionfeed: no published floors")

type floorInput struct{ client Client }

// NewFloorInput is the revision feed as a restore-fence floor input: an ADVISORY lower bound (the fence takes the maximum
// of every input and the restored value), usable only while ReadStatus says the store persists and does not evict the hash.
func NewFloorInput(client Client) restorefence.FloorInput { return floorInput{client: client} }

// RegisterFloorInput registers the feed as a floor input of this process. The registry is per process: the auth-service
// registers its own input (the census requires it), and the restore-fence CLI registers one with its own Valkey login.
func RegisterFloorInput(client Client) { restorefence.RegisterFloorInput(NewFloorInput(client)) }

func (floorInput) Name() string { return "valkey-revision-feed" }

func (f floorInput) Status(ctx context.Context) error {
	status, err := ReadStatus(ctx, f.client)
	if err != nil {
		return err
	}
	if !status.Usable {
		return fmt.Errorf("%w: %s", ErrNotUsableAsFloorSource, status.Reason)
	}
	return nil
}

func (f floorInput) Floors(ctx context.Context) (map[string]int64, error) {
	return PublishedFloors(ctx, f.client)
}
