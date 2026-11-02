// Package idpstub is a test-support OpenID Connect and SAML identity provider. The auth integration tests and the
// flip-back kit both use it, so the two cannot drift apart. It is never part of a product image: signing keys are
// generated at every start and no key or certificate is checked in.
package idpstub
