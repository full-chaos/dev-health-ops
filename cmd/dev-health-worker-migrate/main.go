// dev-health-worker-migrate is a thin main over internal/rivermigrate, the
// same code `dho migrate river` runs. It is kept only because the Python
// image's `dev-hops migrate postgres` execs it by this name (and acr's
// lane.sh names it); it is deleted with spec S10, when that verb moves into
// the binary. It holds no logic of its own.
package main

import "github.com/full-chaos/dev-health-ops/internal/rivermigrate"

func main() { rivermigrate.Main() }
