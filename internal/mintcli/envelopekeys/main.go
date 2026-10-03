// Package envelopekeys is the `mint envelope-keys` verb of the dho binary: it
// writes a new envelope signing key (private, 0400) and the matching JWKS
// (public, 0444) into a directory. The self-hosted compose stack runs it once,
// as a one-shot init service, into a named volume; go-api mounts the private
// half read-only, query-api mounts the JWKS only. It refuses to overwrite an
// existing key and prints no key material.
package envelopekeys

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

// Command is the `mint envelope-keys` verb of the dho binary.
func Command() cli.Command {
	return cli.Command{
		Name:    "envelope-keys",
		Kind:    cli.Verb,
		Summary: "write a new envelope private key and its JWKS into a directory; refuses to overwrite",
		Run: func(_ context.Context, env cli.Env) int {
			code, err := run(env.Args, env.Stdout, env.Stderr, env.Lookup)
			if err != nil && !errors.Is(err, flag.ErrHelp) {
				fmt.Fprintln(env.Stderr, "mint envelope-keys:", err)
			}
			return code
		},
	}
}

func run(args []string, stdout, flagErrOutput io.Writer, lookup func(string) (string, bool)) (int, error) {
	defaultKeyID := envelopemint.DefaultKeyID
	if lookup != nil {
		if v, ok := lookup(envelopemint.KeyIDEnvVar); ok && strings.TrimSpace(v) != "" {
			defaultKeyID = v
		}
	}
	fs := flag.NewFlagSet("mint-envelope-keys", flag.ContinueOnError)
	fs.SetOutput(flagErrOutput)
	dir := fs.String("dir", "", "target directory; the private key goes under private/, the JWKS under public/")
	keyID := fs.String("key-id", defaultKeyID, "key id (kid) of the JWKS entry; defaults to "+envelopemint.KeyIDEnvVar+" when set")
	skip := fs.Bool("skip-existing", false, "exit 0 and change nothing when a complete, matching key set already exists (for a step that runs on every start); a partial or mismatching set still fails")
	if err := fs.Parse(args); err != nil {
		err = cli.WrapFlagParseError(err)
		return cli.ExitForVerbError(err), err
	}
	if strings.TrimSpace(*dir) == "" {
		err := cli.WrapFlagParseError(errors.New("-dir is required"))
		return cli.ExitUsage, err
	}

	var err error
	created := true
	if *skip {
		created, err = envelopemint.EnsureKeyFiles(*dir, *keyID)
	} else {
		_, err = envelopemint.GenerateKeyFiles(*dir, *keyID)
	}
	if err != nil {
		if errors.Is(err, envelopemint.ErrKeyFilesExist) || errors.Is(err, envelopemint.ErrKeyFilesInconsistent) {
			return cli.ExitRefused, fmt.Errorf("%w (existing files are not changed)", err)
		}
		return cli.ExitFailure, err
	}
	paths := envelopemint.PathsIn(*dir)
	if created {
		fmt.Fprintf(stdout, "wrote envelope key id %q: private key %s (0400), JWKS %s (0444)\n", *keyID, paths.Private, paths.JWKS)
	} else {
		fmt.Fprintf(stdout, "envelope key id %q already present under %s; nothing changed\n", *keyID, *dir)
	}
	return cli.ExitOK, nil
}
