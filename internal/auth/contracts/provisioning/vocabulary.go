// Package provisioning is the provisioning-event.v1 vocabulary as Go data,
// for producers in a binary that does not ship the schema files
// (contracts.Validate reads them from disk). TestVocabularyMatchesTheSchema
// holds this table to the schema's per-aggregate conditionals and member
// definitions in both directions.
package provisioning

import (
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
	Events   []string
	Change   []string
	Required []string
}

// MaxWireIDLength is the longest aggregate_id the idempotency_key can carry.
const MaxWireIDLength = 64

var provisioningAggregates = map[string]Aggregate{
	"principal": {IDPrefix: "prn_",
		Events: []string{"principal.created", "principal.disabled", "principal.enabled"},
		Change: []string{"status"}},
	"user": {IDPrefix: "prn_",
		Events: []string{"user.created", "user.email_verified", "user.email_unverified", "user.email_changed",
			"user.credentials_changed", "user.tokens_invalidated"}},
	"external_identity": {IDPrefix: "idn_",
		Events: []string{"external_identity.linked", "external_identity.unlinked"},
		Change: []string{"provider", "principal_id"}, Required: []string{"principal_id"}},
	"organization": {IDPrefix: "org_",
		Events: []string{"organization.created", "organization.updated", "organization.suspended", "organization.reactivated"},
		Change: []string{"status", "slug"}},
	"membership": {IDPrefix: "mbr_",
		Events: []string{"membership.invited", "membership.activated", "membership.role_changed", "membership.removed"},
		Change: []string{"status", "role_id"}},
	"platform_role_assignment": {IDPrefix: "pra_",
		Events: []string{"platform_role_assignment.granted", "platform_role_assignment.revoked"},
		Change: []string{"status", "role_id"}},
	"service_account": {IDPrefix: "prn_",
		Events: []string{"service_account.created", "service_account.disabled", "service_account.enabled", "service_account.owner_changed"},
		Change: []string{"status", "owner_principal_id"}},
	"workload": {IDPrefix: "prn_",
		Events: []string{"workload.created", "workload.disabled", "workload.enabled", "workload.audiences_changed"},
		Change: []string{"status", "audiences"}},
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
	"status":             {enum: []string{"active", "disabled", "suspended", "pending", "removed", "revoked", "expired"}},
	"slug":               {pattern: `^[a-z0-9][a-z0-9-]{1,62}$`},
	"role_id":            {pattern: `^rol_[A-Za-z0-9_-]{1,64}$`},
	"owner_principal_id": {pattern: `^prn_[A-Za-z0-9_-]{1,64}$`},
	"principal_id":       {pattern: `^prn_[A-Za-z0-9_-]{1,64}$`},
	"provider":           {pattern: `^[a-z][a-z0-9_-]{1,31}$`},
	"audiences":          {array: true, minItems: 1, maxItems: 16, pattern: `^[a-z][a-z0-9._:-]{0,63}$`},
	"resource_type":      {enum: []string{"organization", "repository"}},
	"resource_key":       {pattern: `^(\*|[a-z0-9._-]{1,100}/[a-z0-9._-]{1,100})$`},
	"action_set":         {array: true, minItems: 1, maxItems: 64, pattern: `^[a-z][a-z0-9_]*([.][a-z][a-z0-9_]*)+$`},
	"effect":             {enum: []string{"allow", "deny"}},
	"expires_at":         {pattern: wireTimestampPattern},
}

var (
	wireIDSuffix  = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	memberPattern = map[string]*regexp.Regexp{}
)

func init() {
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
		!wireIDSuffix.MatchString(aggregateID[len(a.IDPrefix):]) {
		return fmt.Errorf("%s: aggregate_id %q is not a %s id", Surface, aggregateID, a.IDPrefix)
	}
	if len(aggregateID) > MaxWireIDLength {
		return fmt.Errorf("%s: aggregate_id is %d characters; the idempotency_key carries at most %d", Surface, len(aggregateID), MaxWireIDLength)
	}
	if !listHas(a.Events, eventType) {
		return fmt.Errorf("%s: event_type %q is not a %s event", Surface, eventType, aggregateType)
	}
	for _, name := range a.Required {
		if _, ok := change[name]; !ok {
			return fmt.Errorf("%s: %s must carry change.%s", Surface, aggregateType, name)
		}
	}
	for name, value := range change {
		if !listHas(a.Change, name) {
			return fmt.Errorf("%s: %s may not carry change.%s", Surface, aggregateType, name)
		}
		if err := checkChangeMember(name, value); err != nil {
			return fmt.Errorf("%s: change.%s: %w", Surface, name, err)
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
	if name == "expires_at" {
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
