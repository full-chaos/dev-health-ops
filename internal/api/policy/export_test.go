package policy

import "github.com/golang-jwt/jwt/v5"

// OracleTestKey is the signing key of the package's token fixtures, for the
// oracle test in the external test package (the oracle helper imports this
// package, so that test cannot be in it).
func OracleTestKey() string { return testKey }

// OracleTokenVersion is tokenVersion, for the same test.
func OracleTokenVersion(claims jwt.MapClaims) (int64, bool) { return tokenVersion(claims) }
