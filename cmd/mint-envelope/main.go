// Command mint-envelope is the tools image's `-proof-bearer-exec` helper
// for go-api-prove: it mints a fresh effective-principal envelope LOCALLY,
// using the same Ed25519 signing key material the api pod's environment
// holds (GO_API_ENVELOPE_PRIVATE_KEY) -- reached here the same way, via a
// Secret mounted into this process's own environment (secretKeyRef), never
// a copied key file or a network call to a running pod. See
// internal/envelopemint for the signing logic, kept byte-compatible with
// the Python edge's issuer and cmd/query-api/internal/principal's
// verifier.
//
// Its stdout is exactly what go-api-prove's `-proof-bearer-exec` contract
// expects: the fresh envelope and nothing else (see cmd/go-api-prove/main.go's
// -proof-bearer-exec flag help and its mintBearer, which accepts the token
// with or without a leading "Bearer "). Nothing else this program does --
// not a failure message, not its own diagnostics -- ever carries key
// material or the envelope itself outside of a successful stdout write.
package main

import (
	"crypto/ed25519"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "mint-envelope:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("mint-envelope", flag.ContinueOnError)
	org := fs.String("org", "", "org id to mint the envelope for")
	keyFile := fs.String("key-file", "", "path to a PEM file holding the Ed25519 private key (PKCS#8, \"PRIVATE KEY\" block); defaults to reading it from the "+envelopemint.PrivateKeyEnvVar+" environment variable, which is how the tools pod receives it via secretKeyRef")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*org) == "" {
		return errors.New("-org is required")
	}

	privateKey, err := loadKey(*keyFile)
	if err != nil {
		return err
	}

	envelope, err := envelopemint.MintProveEnvelope(privateKey, *org, envelopemint.Options{})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, envelope)
	return err
}

// loadKey reads the signing key from -key-file when given (a path, never
// a secret value, so it is safe on argv), otherwise from
// GO_API_ENVELOPE_PRIVATE_KEY by name -- the same env var name the api
// pod's own environment already uses, reached the same way (a Secret
// mounted via secretKeyRef into this pod's environment).
func loadKey(keyFile string) (ed25519.PrivateKey, error) {
	if strings.TrimSpace(keyFile) == "" {
		return envelopemint.LoadPrivateKeyFromEnv()
	}
	pemBytes, err := os.ReadFile(keyFile)
	if err != nil {
		return nil, fmt.Errorf("-key-file %s: %w", keyFile, err)
	}
	return envelopemint.LoadPrivateKey(pemBytes)
}
