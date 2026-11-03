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
	// (LINK_TOKEN_SECRET, else its JWT_SECRET_KEY chain). Secret: environment
	// or _FILE only. Required while the membership routes mount.
	EnvInviteTokenSecret = "AUTH_INVITE_TOKEN_SECRET"
	// EnvInviteTokenSecretPrevious verifies invite tokens only, never signs:
	// go-api's LINK_TOKEN_SECRET_PREVIOUS. Secret. Required while the
	// membership routes mount.
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
// against, signing key first, as go-api's authmail.Config.AcceptSecrets: the
// signing key, the previous key, then the JWT key (go-api's TokenSecret
// chain, which signed links before a link key was set). Blanks and repeats are
// left out. Nil when the routes are off.
func (c Config) InviteAcceptSecrets() []string {
	if !c.Provisioning.Routes {
		return nil
	}
	out := []string{c.Provisioning.InviteSecret.Reveal()}
	for _, secret := range []string{c.Provisioning.InviteSecretPrevious.Reveal(), c.Session.AccessSecret.Reveal()} {
		seen := false
		for _, have := range out {
			seen = seen || have == secret
		}
		if secret != "" && !seen {
			out = append(out, secret)
		}
	}
	return out
}

// provisioning reads the routes switch and, when it is on, both invite keys.
// A missing key is named by its variable only, never by a value. The routes
// mount with the session routes, so they need the session secrets.
func provisioning(lookup secrets.LookupEnv, sessionEnabled bool) (Provisioning, error) {
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
	if !sessionEnabled {
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
	return Provisioning{Routes: true, InviteSecret: secret, InviteSecretPrevious: previous}, nil
}
