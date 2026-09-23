package goapiproof

import (
	"context"
	"fmt"

	mintedgetoken "github.com/full-chaos/dev-health-ops/internal/mintcli/edgetoken"
	mintenvelope "github.com/full-chaos/dev-health-ops/internal/mintcli/envelope"
)

// MintViaAllowlistedHelper mints one of a fixed set of credentials
// in process and returns it, trimmed.
//
// Before the S1 fold this exec'd a fixed, compiled-in subprocess path
// (/usr/local/bin/mint-envelope or /usr/local/bin/mint-edge-token) -- the
// dev-health-go-api-tools image's own baked-in locations, built from the
// standalone cmd/mint-envelope and cmd/mint-edge-token binaries. Now that
// the dho binary folds every Go-API tool into one process (spec S1), the
// caller and the minting logic live in the same binary, so minting calls
// the mintcli packages' own Mint functions directly: no subprocess, no
// exec.Command call for go.lang.security.audit.dangerous-exec-command to
// flag, and no fixed filesystem path to keep in sync with the image build.
//
// helperName SELECTS a helper, exactly as it did when it selected an
// executable path: "mint-envelope" or "mint-edge-token", never anything
// that reaches a shell or a filesystem path.
//
// args are the helper's own argv[1:] -- operator-supplied flag data (e.g.
// "-org", "<org>"), parsed by the mintcli package's own flag set exactly
// as the standalone binary parsed them.
//
// A failure's error is the mintcli package's own -- neither package ever
// includes key material or the minted credential in an error, matching the
// contract the exec'd binaries held.
func MintViaAllowlistedHelper(ctx context.Context, helperName string, args []string) (string, error) {
	switch helperName {
	case "mint-envelope":
		return mintenvelope.Mint(args)
	case "mint-edge-token":
		return mintedgetoken.Mint(ctx, args)
	default:
		return "", fmt.Errorf("goapiproof: %q is not an allowlisted minting helper -- expected \"mint-envelope\" or \"mint-edge-token\"", helperName)
	}
}
