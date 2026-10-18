package authconfig

import (
	"testing"
)

// FuzzConfigLoad drives configuration resolution with arbitrary operator text
// in the settings that carry a parser (durations, addresses, key id, schema).
// Properties: no panic; an accepted configuration is inside the documented
// bounds, and the two listeners never share an address.
func FuzzConfigLoad(f *testing.F) {
	f.Add("30s", ":8095", ":8096", "kid-1", "auth")
	f.Add("0s", ":8095", ":8095", "k", "auth")
	f.Add("-5s", "host:port", "", "bad kid", "Auth;drop")
	f.Add("1h", "127.0.0.1:0", "[::1]:0", "a.b_c-d", "a")
	f.Fuzz(func(t *testing.T, shutdown, api, operator, keyID, schema string) {
		env := map[string]string{
			EnvDatabaseURI:     "postgres://u:p@127.0.0.1:1/db",
			EnvSigningKeyFile:  "/nonexistent",
			EnvSigningKeyID:    keyID,
			EnvShutdownTimeout: shutdown,
			EnvAPIAddress:      api,
			EnvOperatorAddress: operator,
			EnvDatabaseSchema:  schema,
		}
		cfg, err := Load(Spec{LookupEnv: func(name string) (string, bool) {
			value, ok := env[name]
			return value, ok
		}})
		if err != nil {
			return
		}
		if cfg.ShutdownTimeout < minShutdownTimeout || cfg.ShutdownTimeout > maxShutdownTimeout {
			t.Fatalf("accepted shutdown timeout %s outside [%s, %s]", cfg.ShutdownTimeout, minShutdownTimeout, maxShutdownTimeout)
		}
		if cfg.APIAddress == cfg.OperatorAddress {
			t.Fatalf("accepted one address %q for both listeners", cfg.APIAddress)
		}
		if validateKeyID(cfg.SigningKeyID) != nil || validateIdentifier(EnvDatabaseSchema, cfg.DatabaseSchema) != nil {
			t.Fatalf("accepted an unvalidated key id %q or schema %q", cfg.SigningKeyID, cfg.DatabaseSchema)
		}
	})
}
