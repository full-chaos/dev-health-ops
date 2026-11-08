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
	// (LINK_TOKEN_SECRET). Secret: environment
	// or _FILE only. Required while the membership routes mount.
	EnvInviteTokenSecret = "AUTH_INVITE_TOKEN_SECRET"
	// EnvInviteTokenSecretPrevious verifies invite tokens only, never signs:
	// go-api's LINK_TOKEN_SECRET_PREVIOUS. Secret. Optional: unset or empty,
	// no previous key is accepted.
	EnvInviteTokenSecretPrevious = "AUTH_INVITE_TOKEN_SECRET_PREVIOUS"
)

// Provisioning is the internal membership and invite routes' setting.
type Provisioning struct {
	// Routes is AUTH_MEMBERSHIP_ROUTES.
	Routes bool
	// InviteSecret signs new invite tokens; InviteSecretPrevious only
	// verifies them. InviteSecret is set exactly when Routes is;
	// InviteSecretPrevious may be empty.
	InviteSecret         secrets.Value
	InviteSecretPrevious secrets.Value
}

// InviteAcceptSecrets are the keys a presented invite token is checked
// against, signing key first, as go-api's authmail.Config.AcceptSecrets: the
// signing key, then the previous key. No session or access key verifies an
// invite. Blanks and repeats are left out. Nil when the routes are off.
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
// A missing key is named by its variable only, never by a value. The routes
// mount with the session routes, so they need the session secrets.
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
		return Provisioning{}, fmt.Errorf("%s needs the session secrets (%s)", EnvMembershipRoutes, sessionSecretNames(session.AccessClass))
	}
	if !hasSecret || secret.Reveal() == "" {
		return Provisioning{}, fmt.Errorf("%s=true needs %s", EnvMembershipRoutes, EnvInviteTokenSecret)
	}
	out := Provisioning{Routes: true, InviteSecret: secret}
	if hasPrevious {
		out.InviteSecretPrevious = previous
	}
	return out, nil
}
