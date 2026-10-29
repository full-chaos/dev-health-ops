package revisionfeed

import (
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
)

// Stub: the next commit implements it.
var ErrNotUsableAsFloorSource = errors.New("revisionfeed: not usable as a floor source")

func NewFloorInput(Client) restorefence.FloorInput { return nil }
