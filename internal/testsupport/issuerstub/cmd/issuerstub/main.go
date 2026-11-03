// Command issuerstub runs the CI-only access-token issuer of the live e2e (see the issuerstub package). It is a
// test tool: no product image holds it, and its keys are generated at every start.
//
//	issuerstub -dir DIR [-key-id KID]
//
// It writes the key files into DIR, listens on https://localhost:<free port>, writes DIR/issuer-url, and serves
// until it is stopped.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/issuerstub"
)

func main() {
	fs := flag.NewFlagSet("issuerstub", flag.ExitOnError)
	dir := fs.String("dir", "", "directory for the key files (private/, public/, ca.pem, issuer-url); must not hold a key set")
	keyID := fs.String("key-id", issuerstub.DefaultKeyID, "kid of the signing key")
	_ = fs.Parse(os.Args[1:])
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "issuerstub: -dir is required")
		os.Exit(2)
	}
	files, err := issuerstub.Prepare(*dir, *keyID)
	if err == nil {
		err = issuerstub.Serve(files)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "issuerstub:", err)
		os.Exit(1)
	}
}
