// Package workload is the auth-service workload registry: the write authority
// for auth.workloads and auth.workload_bindings, and the one read the token
// exchange uses to turn a federated identity into a registered workload. Every
// verb commits through audit.Commit. Resolve fails closed.
package workload

import (
	"regexp"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

var (
	namePattern     = regexp.MustCompile(`^[a-z][a-z0-9-]{1,62}$`)
	audiencePattern = regexp.MustCompile(`^[a-z][a-z0-9._:-]{0,63}$`)
	envPattern      = regexp.MustCompile(`^[a-z][a-z0-9-]{0,31}$`)
	uidPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

const (
	maxAudiences = 16
	maxActions   = 64
)

// Spec is one platform workload registration.
type Spec struct {
	Name           string
	Audiences      []string
	AllowedActions []string
}

// BindingSpec is where a workload runs. ServiceAccountUID is optional; when
// set the exchange requires it equal.
type BindingSpec struct {
	Environment        string
	TrustDomain        string
	Issuer             string
	Namespace          string
	ServiceAccountName string
	ServiceAccountUID  string
}

// Actor is who ran a verb; both are nil for an operator command.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
}

// Registration is what Resolve returns for an enabled binding.
type Registration struct {
	PrincipalID    uuid.UUID
	Name           string
	Audiences      []string
	AllowedActions []string
	BindingID      uuid.UUID
	// Revision is the workload's provisioning revision; audience and enable
	// changes bump it, allowed-action and binding changes do not (Resolve
	// reads the current rows on every call).
	Revision int64
}

// ValidateName refuses a name outside the workload name pattern.
func ValidateName(name string) error {
	if !namePattern.MatchString(name) {
		return ErrNameInvalid
	}
	return nil
}

// ValidateAudiences refuses a list that is too long, has a malformed member or
// repeats one. Membership in audiences.json is the registry contract's rule.
func ValidateAudiences(audiences []string) error {
	if len(audiences) > maxAudiences {
		return ErrAudienceInvalid
	}
	seen := map[string]bool{}
	for _, a := range audiences {
		if !audiencePattern.MatchString(a) || seen[a] {
			return ErrAudienceInvalid
		}
		seen[a] = true
	}
	return nil
}

// ValidateActions refuses a list with a member outside the catalog, a repeat,
// or more than 64 members. There is no wildcard: "*" is not a catalog action.
func ValidateActions(cat *policycatalog.Catalog, actions []string) error {
	if len(actions) > maxActions {
		return ErrActionInvalid
	}
	seen := map[string]bool{}
	for _, a := range actions {
		if _, err := cat.Action(a); err != nil || seen[a] {
			return ErrActionInvalid
		}
		seen[a] = true
	}
	return nil
}

// ValidateBinding refuses an empty, over-long or malformed binding field.
func ValidateBinding(b BindingSpec) error {
	for _, f := range []struct {
		v   string
		max int
	}{{b.TrustDomain, 253}, {b.Issuer, 512}, {b.Namespace, 253}, {b.ServiceAccountName, 253}} {
		if f.v == "" || len(f.v) > f.max || strings.TrimSpace(f.v) != f.v {
			return ErrBindingInvalid
		}
	}
	if !envPattern.MatchString(b.Environment) {
		return ErrBindingInvalid
	}
	if b.ServiceAccountUID != "" && !uidPattern.MatchString(b.ServiceAccountUID) {
		return ErrBindingInvalid
	}
	return nil
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	in := map[string]bool{}
	for _, x := range a {
		in[x] = true
	}
	for _, x := range b {
		if !in[x] {
			return false
		}
	}
	return true
}

// Narrow returns the requested scope when the registration may carry the
// audience and every requested action is allowed. It never widens: an empty
// allowed list allows nothing, and an empty request yields an empty scope.
func (r Registration) Narrow(audience string, scope []string) ([]string, error) {
	aud := false
	for _, a := range r.Audiences {
		if a == audience {
			aud = true
		}
	}
	if !aud {
		return nil, ErrRefused
	}
	allowed := map[string]bool{}
	for _, a := range r.AllowedActions {
		allowed[a] = true
	}
	out := make([]string, 0, len(scope))
	seen := map[string]bool{}
	for _, s := range scope {
		if !allowed[s] {
			return nil, ErrRefused
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out, nil
}
