// Package jobruntime binds the versioned job contract registry to executable
// River workers. It deliberately keeps routing and execution policy out of
// handler code so a handler cannot silently drift from the checked-in contract.
package jobruntime

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

// Descriptor is the complete runtime policy for one versioned job kind.
// Migration fields are included because being implemented does not by itself
// make a handler eligible to consume production jobs.
type Descriptor struct {
	Kind              string
	CurrentVersion    int
	SupportedVersions []int
	Profile           string
	Queue             string
	ExecutionMode     string
	Priority          int
	Timeout           time.Duration
	MaxAttempts       int
	RetryPolicy       string
	Cancellation      string
	Delivery          string
	Idempotency       string
	ConcurrencyScope  string
	ConcurrencyLimit  int
	SensitiveFields   []string
	DomainLink        string
	OrganizationScope string
	MigrationState    string
	Route             string
	RollbackRoute     string
}

// HandlerSpec is the declaration compiled into a concrete worker. Startup
// validation compares every field with Descriptor; partial declarations are
// intentionally not accepted.
type HandlerSpec = Descriptor

// StartupSpec is the profile contract supplied by deployment configuration.
// Queues and handlers must cover the profile exactly, with no duplicates or
// undeclared extras.
type StartupSpec struct {
	Profile  string
	Queues   []string
	Handlers []HandlerSpec
}

// Registry is immutable after construction and safe for concurrent reads.
type Registry struct {
	byKind    map[string]Descriptor
	byProfile map[string][]Descriptor
}

// Load reads the checked-in contract and migration artifacts.
func Load(root string) (*Registry, error) {
	contracts, err := jobcontract.LoadRegistry(root)
	if err != nil {
		return nil, fmt.Errorf("load job contracts: %w", err)
	}
	migration, err := jobcontract.LoadMigrationState(root, contracts)
	if err != nil {
		return nil, fmt.Errorf("load job migration state: %w", err)
	}
	return newRegistry(contracts, migration)
}

func newRegistry(contracts jobcontract.Registry, migration jobcontract.MigrationState) (*Registry, error) {
	if err := migration.Validate(contracts); err != nil {
		return nil, fmt.Errorf("validate job migration state: %w", err)
	}

	migrations := make(map[string]jobcontract.MigrationJob, len(migration.Jobs))
	for _, policy := range migration.Jobs {
		if err := validateMigrationPolicy(policy); err != nil {
			return nil, fmt.Errorf("migration policy %s: %w", policy.Kind, err)
		}
		migrations[policy.Kind] = policy
	}

	registry := &Registry{
		byKind:    make(map[string]Descriptor, len(contracts.Jobs)),
		byProfile: make(map[string][]Descriptor),
	}
	for _, contract := range contracts.Jobs {
		policy, ok := migrations[contract.Kind]
		if !ok {
			return nil, fmt.Errorf("job %s has no migration policy", contract.Kind)
		}
		if contract.RetryPolicy != "none" && contract.RetryPolicy != "bounded_exponential_jitter" {
			return nil, fmt.Errorf("job %s uses unsupported retry policy", contract.Kind)
		}
		descriptor := Descriptor{
			Kind:              contract.Kind,
			CurrentVersion:    contract.CurrentVersion,
			SupportedVersions: append([]int(nil), contract.SupportedVersions...),
			Profile:           contract.Profile,
			Queue:             contract.Queue,
			ExecutionMode:     contract.ExecutionMode,
			Priority:          contract.Priority,
			Timeout:           time.Duration(contract.TimeoutSeconds) * time.Second,
			MaxAttempts:       contract.MaxAttempts,
			RetryPolicy:       contract.RetryPolicy,
			Cancellation:      contract.Cancellation,
			Delivery:          contract.Delivery,
			Idempotency:       contract.Idempotency,
			ConcurrencyScope:  contract.Concurrency.Scope,
			ConcurrencyLimit:  contract.Concurrency.Limit,
			SensitiveFields:   append([]string(nil), contract.SensitiveFields...),
			DomainLink:        contract.DomainLink,
			OrganizationScope: contract.OrganizationScope,
			MigrationState:    policy.State,
			Route:             policy.Route,
			RollbackRoute:     policy.RollbackRoute,
		}
		registry.byKind[descriptor.Kind] = descriptor
		registry.byProfile[descriptor.Profile] = append(registry.byProfile[descriptor.Profile], descriptor)
	}
	for profile := range registry.byProfile {
		sort.Slice(registry.byProfile[profile], func(left, right int) bool {
			return registry.byProfile[profile][left].Kind < registry.byProfile[profile][right].Kind
		})
	}
	return registry, nil
}

func validateMigrationPolicy(policy jobcontract.MigrationJob) error {
	allowed := map[string]struct {
		route    string
		rollback string
	}{
		"inventory":            {route: "celery", rollback: "celery"},
		"contract_frozen":      {route: "celery", rollback: "celery"},
		"go_implemented":       {route: "celery", rollback: "celery"},
		"shadow":               {route: "shadow", rollback: "celery"},
		"canary":               {route: "river_canary", rollback: "celery"},
		"go_default":           {route: "river", rollback: "celery"},
		"celery_fallback_only": {route: "river", rollback: "celery"},
		"celery_removed":       {route: "river", rollback: "none"},
	}
	expected, ok := allowed[policy.State]
	if !ok {
		return errors.New("unknown migration state")
	}
	if policy.Route != expected.route || policy.RollbackRoute != expected.rollback {
		return fmt.Errorf("state %s requires route %s and rollback %s", policy.State, expected.route, expected.rollback)
	}
	return nil
}

// Descriptor returns a defensive copy of the policy for kind.
func (registry *Registry) Descriptor(kind string) (Descriptor, bool) {
	if registry == nil {
		return Descriptor{}, false
	}
	descriptor, ok := registry.byKind[kind]
	descriptor.SupportedVersions = append([]int(nil), descriptor.SupportedVersions...)
	descriptor.SensitiveFields = append([]string(nil), descriptor.SensitiveFields...)
	return descriptor, ok
}

// Descriptors returns every registered policy in deterministic kind order.
// The complete enumeration lets bounded infrastructure such as the outbox
// relay distinguish known deferred routes from unknown rows that must still
// be inspected and terminalized.
func (registry *Registry) Descriptors() []Descriptor {
	if registry == nil {
		return nil
	}
	kinds := make([]string, 0, len(registry.byKind))
	for kind := range registry.byKind {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	descriptors := make([]Descriptor, 0, len(kinds))
	for _, kind := range kinds {
		descriptor, _ := registry.Descriptor(kind)
		descriptors = append(descriptors, descriptor)
	}
	return descriptors
}

// Profile returns sorted defensive copies of the profile's job policies.
func (registry *Registry) Profile(profile string) []Descriptor {
	if registry == nil {
		return nil
	}
	descriptors := registry.byProfile[profile]
	result := make([]Descriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		copy, _ := registry.Descriptor(descriptor.Kind)
		result = append(result, copy)
	}
	return result
}

// HasProfile reports whether profile has at least one registered job.
func (registry *Registry) HasProfile(profile string) bool {
	return registry != nil && len(registry.byProfile[profile]) > 0
}

// HasQueue reports whether at least one registered kind uses queue. Multiple
// kinds may intentionally share a logical queue.
func (registry *Registry) HasQueue(queue string) bool {
	if registry == nil {
		return false
	}
	for _, descriptor := range registry.byKind {
		if descriptor.Queue == queue {
			return true
		}
	}
	return false
}

// Executable reports whether migration state permits River execution. Shadow
// and canary are executable only in their explicit routes; a merely compiled
// handler remains disabled while the route is Celery.
func (descriptor Descriptor) Executable() bool {
	return descriptor.Route == "shadow" || descriptor.Route == "river_canary" || descriptor.Route == "river"
}

// ValidateHandler compares a concrete handler declaration with the canonical
// descriptor and returns a field-addressed error without dumping either value.
func (registry *Registry) ValidateHandler(handler HandlerSpec) error {
	expected, ok := registry.Descriptor(handler.Kind)
	if !ok {
		return fmt.Errorf("handler kind %q is not registered", handler.Kind)
	}
	checks := []struct {
		name  string
		equal bool
	}{
		{"current_version", handler.CurrentVersion == expected.CurrentVersion},
		{"supported_versions", reflect.DeepEqual(handler.SupportedVersions, expected.SupportedVersions)},
		{"profile", handler.Profile == expected.Profile},
		{"queue", handler.Queue == expected.Queue},
		{"execution_mode", handler.ExecutionMode == expected.ExecutionMode},
		{"priority", handler.Priority == expected.Priority},
		{"timeout", handler.Timeout == expected.Timeout},
		{"max_attempts", handler.MaxAttempts == expected.MaxAttempts},
		{"retry_policy", handler.RetryPolicy == expected.RetryPolicy},
		{"cancellation", handler.Cancellation == expected.Cancellation},
		{"delivery", handler.Delivery == expected.Delivery},
		{"idempotency", handler.Idempotency == expected.Idempotency},
		{"concurrency_scope", handler.ConcurrencyScope == expected.ConcurrencyScope},
		{"concurrency_limit", handler.ConcurrencyLimit == expected.ConcurrencyLimit},
		{"sensitive_fields", reflect.DeepEqual(handler.SensitiveFields, expected.SensitiveFields)},
		{"domain_link", handler.DomainLink == expected.DomainLink},
		{"organization_scope", handler.OrganizationScope == expected.OrganizationScope},
		{"migration_state", handler.MigrationState == expected.MigrationState},
		{"route", handler.Route == expected.Route},
		{"rollback_route", handler.RollbackRoute == expected.RollbackRoute},
	}
	for _, check := range checks {
		if !check.equal {
			return fmt.Errorf("handler %s drifts from registry field %s", handler.Kind, check.name)
		}
	}
	return nil
}

// ValidateStartup proves exact profile, queue, and handler coverage before a
// River client starts fetching. It is suitable for a readiness dependency.
func (registry *Registry) ValidateStartup(startup StartupSpec) error {
	if registry == nil {
		return errors.New("runtime registry is required")
	}
	expected := registry.Profile(startup.Profile)
	if len(expected) == 0 {
		return fmt.Errorf("profile %q has no registered jobs", startup.Profile)
	}

	queueSet, err := uniqueSet("queue", startup.Queues)
	if err != nil {
		return err
	}
	expectedQueues := make(map[string]struct{})
	for _, descriptor := range expected {
		expectedQueues[descriptor.Queue] = struct{}{}
	}
	if !reflect.DeepEqual(queueSet, expectedQueues) {
		return fmt.Errorf("profile %s queue coverage drifts from registry", startup.Profile)
	}

	handlers := make(map[string]struct{}, len(startup.Handlers))
	for _, handler := range startup.Handlers {
		if handler.Profile != startup.Profile {
			return fmt.Errorf("handler %s belongs to profile %s, not %s", handler.Kind, handler.Profile, startup.Profile)
		}
		if _, duplicate := handlers[handler.Kind]; duplicate {
			return fmt.Errorf("duplicate handler kind %s", handler.Kind)
		}
		handlers[handler.Kind] = struct{}{}
		if err := registry.ValidateHandler(handler); err != nil {
			return err
		}
	}
	if len(handlers) != len(expected) {
		return fmt.Errorf("profile %s handler coverage drifts from registry", startup.Profile)
	}
	for _, descriptor := range expected {
		if _, ok := handlers[descriptor.Kind]; !ok {
			return fmt.Errorf("profile %s is missing handler %s", startup.Profile, descriptor.Kind)
		}
	}
	return nil
}

func uniqueSet(label string, values []string) (map[string]struct{}, error) {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			return nil, fmt.Errorf("%s cannot be empty", label)
		}
		if _, duplicate := result[value]; duplicate {
			return nil, fmt.Errorf("duplicate %s %s", label, value)
		}
		result[value] = struct{}{}
	}
	return result, nil
}
