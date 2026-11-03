// Package provisioning is the provisioning-event.v1 vocabulary as Go data,
// for producers in a binary that does not ship the schema files
// (contracts.Validate reads them from disk). TestVocabularyMatchesTheSchema
// holds this table to the schema's per-aggregate conditionals and member
// definitions in both directions.
package provisioning

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"regexp"
	"sort"
	"time"
)

// Surface is the wire contract this vocabulary mirrors
// (Surface; a test holds them equal). This package
// does not import contracts: internal/audit imports it, and the contracts
// tests import internal/audit.
const Surface = "provisioning-event.v1"

// Aggregate is one aggregate_type: its id prefix, its event
// verbs and the payload.change members it may carry.
type Aggregate struct {
	IDPrefix string
	// IDSuffix, when set, is the exact regexp body that follows the prefix (default: the generic 1-64 character wire id).
	IDSuffix string
	Events   []string
	// Change are the members an ordinary event of the aggregate may carry.
	Change   []string
	Required []string
	// Scoped lists, for an event that carries members of its own, exactly the members that event may carry: it replaces
	// Change for that event, so a scope event never carries an ordinary member and an ordinary event never carries a
	// scope member.
	Scoped map[string][]string
	// NoOrganization marks a platform-level aggregate: its events carry no organization_id.
	NoOrganization bool
	// EventRequired lists change members one event must carry besides Required.
	EventRequired map[string][]string
	// Fixed pins a change member to one value for every event of the aggregate.
	Fixed map[string]string
	// OneShot marks an aggregate that has exactly one event, at revision 1, and never another (a terminal fact such as
	// a session revoke). It keeps no aggregate_revisions row and no feed state: the producer writes revision 1 without
	// a counter, and the feed publishes without a high-water mark.
	OneShot bool
}

// MaxWireIDLength is the longest aggregate_id the idempotency_key can carry.
const MaxWireIDLength = 64

var provisioningAggregates = map[string]Aggregate{
	"principal": {IDPrefix: "prn_",
		Events:        []string{"principal.created", "principal.disabled", "principal.enabled", "principal.sessions_revoked"},
		Change:        []string{"status"},
		Scoped:        map[string][]string{"principal.sessions_revoked": {"revoked_before", "reason"}},
		EventRequired: map[string][]string{"principal.sessions_revoked": {"revoked_before", "reason"}}},
	"user": {IDPrefix: "prn_",
		Events: []string{"user.created", "user.email_verified", "user.email_unverified", "user.email_changed",
			"user.credentials_changed", "user.tokens_invalidated"}},
	"external_identity": {IDPrefix: "idn_",
		Events: []string{"external_identity.linked", "external_identity.unlinked"},
		Change: []string{"provider", "principal_id"}, Required: []string{"principal_id"}},
	"organization": {IDPrefix: "org_",
		Events: []string{"organization.created", "organization.updated", "organization.suspended", "organization.reactivated",
			"organization.sessions_revoked"},
		Change:        []string{"status", "slug"},
		Scoped:        map[string][]string{"organization.sessions_revoked": {"revoked_before", "reason"}},
		EventRequired: map[string][]string{"organization.sessions_revoked": {"revoked_before", "reason"}}},
	"membership": {IDPrefix: "mbr_",
		Events: []string{"membership.invited", "membership.activated", "membership.role_changed", "membership.suspended", "membership.removed"},
		Change: []string{"status", "role_id"}},
	"platform_role_assignment": {IDPrefix: "pra_",
		Events: []string{"platform_role_assignment.granted", "platform_role_assignment.revoked"},
		Change: []string{"status", "role_id"}},
	"service_account": {IDPrefix: "prn_",
		Events: []string{"service_account.created", "service_account.disabled", "service_account.enabled", "service_account.owner_changed",
			"service_account.key_changed"},
		Change: []string{"status", "owner_principal_id", "credential_id"}},
	"workload": {IDPrefix: "prn_",
		Events: []string{"workload.created", "workload.disabled", "workload.enabled", "workload.audiences_changed",
			"workload.bootstrap_changed"},
		Change: []string{"status", "audiences", "credential_id"}},
	"invite": {IDPrefix: "inv_",
		Events: []string{"invite.created", "invite.accepted", "invite.revoked"},
		Change: []string{"status"}},
	"session": {IDPrefix: "ses_",
		Events: []string{"session.revoked"},
		Change: []string{"status", "reason"}, Required: []string{"status", "reason"},
		Fixed: map[string]string{"status": "revoked"}, OneShot: true},
	"signing_key": {IDPrefix: "key_", IDSuffix: "[A-Za-z0-9_-]{43}", NoOrganization: true,
		Events: []string{"signing_key.compromised"},
		Change: []string{"kid"}, Required: []string{"kid"}},
	"resource_grant": {IDPrefix: "grt_",
		Events: []string{"resource_grant.created", "resource_grant.updated", "resource_grant.revoked", "resource_grant.expired"},
		Change: []string{"status", "resource_type", "resource_key", "action_set", "effect", "expires_at"}},
}

// changeMember is the value rule of one payload.change member.
type changeMember struct {
	enum     []string
	pattern  string
	array    bool
	minItems int
	maxItems int
}

var wireTimestampPattern = `^([0-9]{3}[1-9]|[0-9]{2}[1-9][0-9]|[0-9][1-9][0-9]{2}|[1-9][0-9]{3})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]{1,6})?(Z|[+-](0[0-9]|1[0-9]|2[0-3]):[0-5][0-9])$`

var provisioningChangeMembers = map[string]changeMember{
	"status":             {enum: []string{"active", "disabled", "suspended", "pending", "removed", "revoked", "expired", "accepted"}},
	"slug":               {pattern: `^[a-z0-9][a-z0-9-]{1,62}$`},
	"role_id":            {pattern: `^rol_[A-Za-z0-9_-]{1,64}$`},
	"owner_principal_id": {pattern: `^prn_[A-Za-z0-9_-]{1,64}$`},
	"principal_id":       {pattern: `^prn_[A-Za-z0-9_-]{1,64}$`},
	"credential_id":      {pattern: `^crd_[A-Za-z0-9_-]{1,64}$`},
	"provider":           {pattern: `^[a-z][a-z0-9_-]{1,31}$`},
	"audiences":          {array: true, minItems: 1, maxItems: 16, pattern: `^[a-z][a-z0-9._:-]{0,63}$`},
	"resource_type":      {enum: []string{"organization", "repository"}},
	"resource_key":       {pattern: `^(\*|[a-z0-9._-]{1,100}/[a-z0-9._-]{1,100})$`},
	"action_set":         {array: true, minItems: 1, maxItems: 64, pattern: `^[a-z][a-z0-9_]*([.][a-z][a-z0-9_]*)+$`},
	"effect":             {enum: []string{"allow", "deny"}},
	"expires_at":         {pattern: wireTimestampPattern},
	"revoked_before":     {pattern: wireTimestampPattern},
	"reason":             {enum: []string{"caller", "refresh_reuse", "password_changed", "org_ended", "delegation_ended", "compromised"}},
	"kid":                {pattern: `^[A-Za-z0-9._-]{1,64}$`},
}

var (
	wireIDSuffix  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	memberPattern = map[string]*regexp.Regexp{}
	idPatterns    = map[string]*regexp.Regexp{}
)

func init() {
	for name, a := range provisioningAggregates {
		if a.IDSuffix != "" {
			idPatterns[name] = regexp.MustCompile(a.IDPattern())
		}
	}
	for name, m := range provisioningChangeMembers {
		if m.pattern != "" {
			memberPattern[name] = regexp.MustCompile(m.pattern)
		}
	}
}

// AggregateTypes lists the aggregate types, sorted.
func AggregateTypes() []string {
	out := make([]string, 0, len(provisioningAggregates))
	for name := range provisioningAggregates {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// SigningKeyID is the aggregate id of the signing_key aggregate for kid: "key_" and the unpadded base64url SHA-256 of the kid
// (43 characters). A kid may hold a dot, which an aggregate id may not, so the id is derived and the kid travels as change.kid.
func SigningKeyID(kid string) string {
	sum := sha256.Sum256([]byte(kid))
	return "key_" + base64.RawURLEncoding.EncodeToString(sum[:])
}

// IDPattern is the schema pattern of the aggregate ids of aggregateType.
func (a Aggregate) IDPattern() string {
	suffix := a.IDSuffix
	if suffix == "" {
		suffix = "[A-Za-z0-9_-]{1,64}"
	}
	return "^" + a.IDPrefix + suffix + "$"
}

// ChangeMembers are the members the event may carry.
func (a Aggregate) ChangeMembers(event string) []string {
	if own, ok := a.Scoped[event]; ok {
		return own
	}
	return a.Change
}

// IsOneShot reports whether aggregateType is a one-shot aggregate (see Aggregate.OneShot).
func IsOneShot(aggregateType string) bool { return provisioningAggregates[aggregateType].OneShot }

// AggregateOf returns the vocabulary of one aggregate type.
func AggregateOf(aggregateType string) (Aggregate, bool) {
	a, ok := provisioningAggregates[aggregateType]
	return a, ok
}

// WireID is the provisioning-event.v1 id of an aggregate: its type's prefix
// and the server-minted id (a UUID's canonical text). It returns "" for an
// unknown aggregate type or an id the wire cannot carry, which
// CheckEvent then refuses.
func WireID(aggregateType, id string) string {
	a, ok := provisioningAggregates[aggregateType]
	if !ok || !wireIDSuffix.MatchString(id) || len(a.IDPrefix)+len(id) > MaxWireIDLength {
		return ""
	}
	return a.IDPrefix + id
}

// CheckEvent refuses an event the provisioning-event.v1 schema
// would refuse: an unknown aggregate type, an id without the type's prefix,
// an id longer than the idempotency_key carries, a verb outside the type's
// vocabulary, a missing required change member, a change member the type may
// not carry, or a member value outside its rule.
func CheckEvent(aggregateType, aggregateID, eventType string, change map[string]any) error {
	a, ok := provisioningAggregates[aggregateType]
	if !ok {
		return fmt.Errorf("%s: unknown aggregate_type %q", Surface, aggregateType)
	}
	if len(aggregateID) <= len(a.IDPrefix) || aggregateID[:len(a.IDPrefix)] != a.IDPrefix ||
		!wireIDSuffix.MatchString(aggregateID[len(a.IDPrefix):]) || (a.IDSuffix != "" && !idPatterns[aggregateType].MatchString(aggregateID)) {
		return fmt.Errorf("%s: aggregate_id %q is not a %s id", Surface, aggregateID, a.IDPrefix)
	}
	if len(aggregateID) > MaxWireIDLength {
		return fmt.Errorf("%s: aggregate_id is %d characters; the idempotency_key carries at most %d", Surface, len(aggregateID), MaxWireIDLength)
	}
	if !listHas(a.Events, eventType) {
		return fmt.Errorf("%s: event_type %q is not a %s event", Surface, eventType, aggregateType)
	}
	for _, name := range append(append([]string(nil), a.Required...), a.EventRequired[eventType]...) {
		if _, ok := change[name]; !ok {
			return fmt.Errorf("%s: %s must carry change.%s", Surface, eventType, name)
		}
	}
	if aggregateType == "signing_key" {
		kid, _ := change["kid"].(string)
		if aggregateID != SigningKeyID(kid) {
			return fmt.Errorf("%s: signing_key aggregate_id is not the derived id of change.kid", Surface)
		}
	}
	allowed := a.ChangeMembers(eventType)
	for name, value := range change {
		if !listHas(allowed, name) {
			return fmt.Errorf("%s: %s may not carry change.%s", Surface, aggregateType, name)
		}
		if err := checkChangeMember(name, value); err != nil {
			return fmt.Errorf("%s: change.%s: %w", Surface, name, err)
		}
		if want, ok := a.Fixed[name]; ok && value != want {
			return fmt.Errorf("%s: change.%s of a %s must be %q", Surface, name, aggregateType, want)
		}
	}
	return nil
}

func checkChangeMember(name string, value any) error {
	rule := provisioningChangeMembers[name]
	if rule.array {
		items, ok := value.([]string)
		if !ok {
			return fmt.Errorf("must be a []string")
		}
		if len(items) < rule.minItems || len(items) > rule.maxItems {
			return fmt.Errorf("must hold %d..%d items", rule.minItems, rule.maxItems)
		}
		seen := map[string]bool{}
		for _, item := range items {
			if seen[item] || !memberPattern[name].MatchString(item) {
				return fmt.Errorf("item %q is repeated or outside the pattern", item)
			}
			seen[item] = true
		}
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}
	if rule.enum != nil {
		if !listHas(rule.enum, text) {
			return fmt.Errorf("%q is outside the vocabulary", text)
		}
		return nil
	}
	if !memberPattern[name].MatchString(text) {
		return fmt.Errorf("%q is outside the pattern", text)
	}
	if name == "expires_at" || name == "revoked_before" {
		if _, err := time.Parse(time.RFC3339Nano, text); err != nil {
			return fmt.Errorf("%q is not a calendar instant", text)
		}
	}
	return nil
}

// ChangeAllows reports whether the member value passes its rule,
// for a producer that carries an optional member only when the wire admits it.
func ChangeAllows(name string, value any) bool {
	if _, ok := provisioningChangeMembers[name]; !ok {
		return false
	}
	return checkChangeMember(name, value) == nil
}

func listHas(list []string, v string) bool {
	for _, item := range list {
		if item == v {
			return true
		}
	}
	return false
}
