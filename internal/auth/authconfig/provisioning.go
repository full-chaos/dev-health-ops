package authconfig

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const (
	// EnvMembershipRoutes ("true" or "false", default false) mounts the
	// internal membership, invite and organization deprovision routes go-api
	// calls with its service credential.
	EnvMembershipRoutes = "AUTH_MEMBERSHIP_ROUTES"
	// EnvInviteTokenSecret signs new invite tokens: go-api's link signing key
	// (LINK_TOKEN_SECRET). Never the session JWT key (AUTH_JWT_SECRET_KEY).
	// Secret: environment or _FILE only. Required while the membership routes
	// mount.
	EnvInviteTokenSecret = "AUTH_INVITE_TOKEN_SECRET"
	// EnvInviteTokenSecretPrevious verifies invite tokens only, never signs:
	// go-api's LINK_TOKEN_SECRET_PREVIOUS. Never the session JWT key. Secret.
	// Required while the membership routes mount.
	EnvInviteTokenSecretPrevious = "AUTH_INVITE_TOKEN_SECRET_PREVIOUS"
)

// Provisioning is the internal membership and invite routes' setting.
type Provisioning struct {
	// Routes is AUTH_MEMBERSHIP_ROUTES.
	Routes bool
	// InviteSecret signs new invite tokens; InviteSecretPrevious only
	// verifies them. Both are set exactly when Routes is.
	InviteSecret         secrets.Value
	InviteSecretPrevious secrets.Value
}

// InviteAcceptSecrets are the keys a presented invite token is checked
// against: the signing key, then the previous key (a repeat left out). The
// session JWT key is not one: a token go-api signed with its JWT_SECRET_KEY
// chain is no invite token here. Nil when the routes are off.
func (c Config) InviteAcceptSecrets() []string {
	if !c.Provisioning.Routes {
		return nil
	}
	out := []string{c.Provisioning.InviteSecret.Reveal()}
	if previous := c.Provisioning.InviteSecretPrevious.Reveal(); previous != "" && previous != out[0] {
		out = append(out, previous)
	}
	return out
}

// provisioning reads the routes switch and, when it is on, both invite keys.
// A missing key, or a key equal to the session JWT key, is named by its
// variable only, never by a value. The routes mount with the session routes,
// so they need the session secrets.
func provisioning(lookup secrets.LookupEnv, session SessionTokens) (Provisioning, error) {
	routes, err := strictBool(lookup, EnvMembershipRoutes)
	if err != nil {
		return Provisioning{}, err
	}
	secret, hasSecret, err := secrets.ResolveSecret(EnvInviteTokenSecret, lookup)
	if err != nil {
		return Provisioning{}, err
	}
	previous, hasPrevious, err := secrets.ResolveSecret(EnvInviteTokenSecretPrevious, lookup)
	if err != nil {
		return Provisioning{}, err
	}
	if !routes {
		return Provisioning{}, nil
	}
	if !session.Enabled {
		return Provisioning{}, fmt.Errorf("%s needs the session secrets (%s and %s)",
			EnvMembershipRoutes, EnvAccessTokenSecret, EnvSessionSuccessorKey)
	}
	for _, need := range []struct {
		name string
		set  bool
	}{{EnvInviteTokenSecret, hasSecret && secret.Reveal() != ""}, {EnvInviteTokenSecretPrevious, hasPrevious && previous.Reveal() != ""}} {
		if !need.set {
			return Provisioning{}, fmt.Errorf("%s=true needs %s", EnvMembershipRoutes, need.name)
		}
	}
	for _, key := range []struct {
		name  string
		value secrets.Value
	}{{EnvInviteTokenSecret, secret}, {EnvInviteTokenSecretPrevious, previous}} {
		if key.value.Reveal() == session.AccessSecret.Reveal() {
			return Provisioning{}, fmt.Errorf("%s must not equal %s: an invite key is go-api's LINK_TOKEN_SECRET, never the session JWT key",
				key.name, EnvAccessTokenSecret)
		}
	}
	return Provisioning{Routes: true, InviteSecret: secret, InviteSecretPrevious: previous}, nil
}
