package ratelimitvalkey

import (
	"crypto/sha256"
	"encoding/hex"
)

// ACLUser is the Valkey login of auth-service's rate-limit store: its own ACL
// user, never go-api's.
const ACLUser = "auth-service-ratelimit"

// ACLRules is what that user may do: only the auth_svc_ counters of this store
// (the key prefix keyPrefix plus the auth-service limiter ids), the one revision
// feed hash under the same prefix (internal/auth/revisionfeed HighWaterKey) and
// the channels under &auth_svc_* (its Channel), and only the commands the
// store, the feed script (+hget +hset inside EVAL, +publish) and the subscriber
// (+subscribe) and the client handshake use. The documented ACL line
// (docs/reference/auth/configuration.md), the chart's ACL file format
// (deploy/helm/dev-health/values.yaml valkey.acl) and the hosted ACL
// integration test carry exactly this text after the password.
const ACLRules = "resetkeys ~dho:rl:auth_svc_* resetchannels &auth_svc_* -@all +hello +select +ping +get +eval +incr +pexpire +hget +hset +publish +subscribe +client|tracking"

// ACLDefaultUserLine keeps the default user as it is without the file: no
// password, every key, every command. The in-chart Valkey's probes and the other
// planes that use it connect as this user, so an ACL file that omits it would
// lock them out. What bounds who can reach the auth_svc_ counters through it is
// the network policy around that Valkey, not the ACL.
const ACLDefaultUserLine = "user default on nopass ~* &* +@all"

// ACLFile is the content of the ACL file the chart mounts (key users.acl): the
// default user line and the rate-limit user with the SHA-256 of password in hex
// after "#", never the password itself.
func ACLFile(password string) string {
	sum := sha256.Sum256([]byte(password))
	return ACLDefaultUserLine + "\n" +
		"user " + ACLUser + " on #" + hex.EncodeToString(sum[:]) + " " + ACLRules + "\n"
}
