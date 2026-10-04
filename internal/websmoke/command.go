package websmoke

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/server"
)

// Command is the `smoke` group of the dho binary. It holds the one verb, `web-path`: the bigboy
// web-path smoke (CHAOS-8647). It lives in the dho image because that Go image is already on the
// bigboy stack (query-api, migrate and go-api run it by digest), so the smoke needs no second
// runtime and its GraphQL documents come from the same build that serves them.
func Command() cli.Command {
	return cli.Command{
		Name:    "smoke",
		Kind:    cli.Group,
		Summary: "stack smoke checks that need a logged-in web session",
		Children: []cli.Command{{
			Name:    "web-path",
			Kind:    cli.Verb,
			Summary: "bigboy web-path smoke: log in through web, drive the Cockpit and Diagnose calls (exit 0 pass, 1 fail, 2 refused target, 3 known-missing only)",
			Run: func(_ context.Context, env cli.Env) int {
				err := runVerb(env)
				var code *exitError
				if errors.As(err, &code) {
					if code.msg != "" {
						fmt.Fprintln(env.Stderr, code.msg)
					}
					return code.code
				}
				if err != nil && !errors.Is(err, flag.ErrHelp) {
					fmt.Fprintf(env.Stderr, "smoke web-path: %v\n", err)
				}
				return cli.ExitForVerbError(err)
			},
		}},
	}
}

type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

// runVerb reads its configuration from environment NAMES only (the compose service sets them):
// the email, the password FILE path, the target and the paths. No flag or argument carries a
// credential, so none can reach argv.
func runVerb(env cli.Env) error {
	flags := flag.NewFlagSet("web-path", flag.ContinueOnError)
	flags.SetOutput(env.Stderr)
	if err := flags.Parse(env.Args); err != nil {
		return cli.WrapFlagParseError(err)
	}
	if flags.NArg() != 0 {
		return &cli.FlagUsageError{Err: errors.New("web-path accepts no arguments: it reads DHO_SMOKE_* from the environment")}
	}
	get := func(name, def string) string {
		if v, ok := env.Lookup(name); ok && v != "" {
			return v
		}
		return def
	}
	target, err := ParseBaseURL(get("DHO_SMOKE_BASE_URL", "http://traefik:3000"))
	if err != nil {
		return &exitError{ExitRefused, "FAIL: " + err.Error()}
	}
	publicHost := get("DHO_SMOKE_PUBLIC_HOST", "www.commanderkeen.dev")
	if !PublicHostAllowed(publicHost) {
		return &exitError{ExitRefused, "FAIL: public_host_not_allowed=" + publicHost}
	}
	var missing []string
	email, passwordFile := get("DHO_SMOKE_ADMIN_EMAIL", ""), get("DHO_SMOKE_ADMIN_PASSWORD_FILE", "")
	if email == "" {
		missing = append(missing, "DHO_SMOKE_ADMIN_EMAIL")
	}
	if passwordFile == "" {
		missing = append(missing, "DHO_SMOKE_ADMIN_PASSWORD_FILE")
	}
	if len(missing) > 0 {
		return &exitError{ExitFail, "FAIL: unset: " + strings.Join(missing, ", ")}
	}
	cfg := Config{
		Target:       target,
		PublicHost:   publicHost,
		Email:        email,
		PasswordFile: passwordFile,
		WebSrc:       get("DHO_SMOKE_WEB_SRC", "/web-src"),
		Catalog:      get("DHO_SMOKE_CATALOG", "/catalog/catalog.json"),
		RoutingOps:   get("DHO_SMOKE_ROUTING_OPS", "/smoke/routing-ops.txt"),
		ReceiptPath:  get("DHO_SMOKE_RECEIPT_PATH", "/receipts/web-path-smoke-receipt.json"),
		Documents:    server.WebPathSmokeDocument,
	}
	return report(cfg, env.Stdout, env.Stderr)
}

// report runs the smoke, writes the receipt and prints the verdict. It returns an exitError for
// every non-zero outcome, so the verb's exit code is the smoke's.
func report(cfg Config, stdout, stderr io.Writer) error {
	out := Run(cfg)
	if err := WriteReceipt(cfg.ReceiptPath, out.Receipt); err != nil {
		fmt.Fprintf(stderr, "FAIL: receipt_unwritable=%T\n", err)
		return &exitError{ExitFail, "receipt not written"}
	}
	for _, f := range out.Receipt.Failures {
		fmt.Fprintf(stderr, "FAIL: %s\n", f)
	}
	for _, k := range out.Receipt.KnownMissing {
		fmt.Fprintf(stdout, "KNOWN-MISSING: %s\n", k)
	}
	fmt.Fprintf(stdout, "WEB_PATH_SMOKE verdict=%s checks=%d passed=%d failed=%d known_missing=%d\n",
		out.Verdict(), len(out.Receipt.Checks), out.Passed, len(out.Receipt.Failures), len(out.Receipt.KnownMissing))
	if code := out.ExitCode(); code != ExitPass {
		return &exitError{code, ""}
	}
	return nil
}
