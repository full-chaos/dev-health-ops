package adminops

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// `admin licenses keygen|create` port the Python verbs: an Ed25519 key pair for
// license signing, and a signed license key. Neither touches the database.

func licensesGroup() cli.Command {
	return cli.Command{
		Name: "licenses", Summary: "license key management", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "keygen", Summary: "generate an Ed25519 key pair for license signing", Kind: cli.Verb, Run: runLicensesKeygen},
			{Name: "create", Summary: "create a signed license key", Kind: cli.Verb, Run: runLicensesCreate},
		},
	}
}

func runLicensesKeygen(_ context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin licenses keygen")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not generate a key pair")
		return cli.ExitFailure
	}
	// The private key is the 32-byte seed, as PyNaCl's SigningKey.encode() is
	// (not Go's 64-byte seed+public key).
	fmt.Fprintf(env.Stdout, "PUBLIC_KEY=%s\n", base64.StdEncoding.EncodeToString(public))
	fmt.Fprintf(env.Stdout, "LICENSE_PRIVATE_KEY=%s\n", base64.StdEncoding.EncodeToString(private.Seed()))
	return cli.ExitOK
}

func runLicensesCreate(_ context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin licenses create")
	var orgID, tier, orgName, contactEmail optString
	flags.Var(&orgID, "org-id", "organization id (required)")
	flags.Var(&tier, "tier", "license tier: community, team or enterprise (required)")
	durationText := flags.String("duration-days", "365", "days until expiry")
	flags.Var(&orgName, "org-name", "organization name")
	flags.Var(&contactEmail, "contact-email", "billing contact email")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !orgID.set || !tier.set {
		fmt.Fprintln(env.Stderr, "argument error: --org-id and --tier are required")
		return cli.ExitUsage
	}
	switch tier.value {
	case "community", "team", "enterprise":
	default:
		fmt.Fprintln(env.Stderr, "argument error: --tier must be one of community, team, enterprise")
		return cli.ExitUsage
	}
	days, err := parsePyInt(*durationText)
	if err != nil {
		fmt.Fprintln(env.Stderr, "argument error: --duration-days must be an integer")
		return cli.ExitUsage
	}
	key, _, err := secrets.Resolve("LICENSE_PRIVATE_KEY", env.Lookup)
	if err != nil {
		fmt.Fprintf(env.Stderr, "configuration error: %v\n", err)
		return cli.ExitFailure
	}
	if !key.Configured() {
		fmt.Fprintln(env.Stdout, "Error: LICENSE_PRIVATE_KEY environment variable is required")
		return cli.ExitFailure
	}
	license, err := licensing.SignLicense(key.Reveal(), licensing.LicenseRequest{
		OrgID: orgID.value, Tier: tier.value, IssuedAt: nowUnix(), LicenseID: uuid.NewString(),
		DurationDays: days, OrgName: orgName.ptr(), ContactEmail: contactEmail.ptr(),
	})
	if err != nil {
		// Python prints the ValueError text; the key itself is never in it.
		fmt.Fprintf(env.Stdout, "Error: %s\n", secrets.RedactValues(err.Error(), key.Reveal()))
		return cli.ExitFailure
	}
	fmt.Fprintln(env.Stdout, license)
	return cli.ExitOK
}

func nowUnix() int64 { return time.Now().Unix() }
