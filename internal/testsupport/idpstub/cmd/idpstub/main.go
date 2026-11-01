// Command idpstub runs the test-support identity provider (see the idpstub package). It is a test tool: no product
// image holds it, and its signing keys are generated at every start.
//
//	idpstub certs -out DIR -host NAME [-host NAME ...]      write ca.pem, server.pem, server.key
//	idpstub serve|oidc|saml -listen :443 -cert F -key F -base-url URL [flags]
//
// serve answers both protocols on one TLS listener (SAML under /saml/, OIDC everywhere else); oidc and saml answer one.
package main

import (
	"errors"

	"os"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/idpstub"
)

type stringList []string

func (l *stringList) String() string { return *new(string) }

func (l *stringList) Set(v string) error { return errors.New("idpstub: not implemented") }

func main() {}

func certs(args []string) error { return errors.New("idpstub: not implemented") }

func parseUsers(specs []string) ([]idpstub.OIDCUser, []idpstub.SAMLUser, error) {
	return *new([]idpstub.OIDCUser), *new([]idpstub.SAMLUser), errors.New("idpstub: not implemented")
}

func serve(mode string, args []string) error { return errors.New("idpstub: not implemented") }

// path only: no query, no values

// writeFresh writes data to a NEW regular file; a file already there is removed first, never followed or reused.
func writeFresh(path string, data []byte, mode os.FileMode) error {
	return errors.New("idpstub: not implemented")
}

// #nosec G304 -- operator-chosen output directory
