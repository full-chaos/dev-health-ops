package jobcontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	registryFilename  = "registry.json"
	migrationFilename = "migration-state.json"
)

var (
	profilePattern      = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)
	queuePattern        = regexp.MustCompile(`^[a-z][a-z0-9._-]*$`)
	handlerOwnerPattern = regexp.MustCompile(`^internal/jobs/[a-z0-9_/]+$`)
)

type VersionPolicy struct {
	Compatibility         string `json:"compatibility"`
	MinimumConsumerWindow int    `json:"minimum_consumer_window"`
	SameVersionRollout    string `json:"same_version_rollout"`
}

type ConcurrencyPolicy struct {
	Scope string `json:"scope"`
	Limit int    `json:"limit"`
}

type JobDefinition struct {
	Kind              string              `json:"kind"`
	CurrentVersion    int                 `json:"current_version"`
	SupportedVersions []int               `json:"supported_versions"`
	Profile           string              `json:"profile"`
	Queue             string              `json:"queue"`
	HandlerOwner      string              `json:"handler_owner"`
	ExecutionMode     string              `json:"execution_mode"`
	Priority          int                 `json:"priority"`
	TimeoutSeconds    int                 `json:"timeout_seconds"`
	MaxAttempts       int                 `json:"max_attempts"`
	RetryPolicy       string              `json:"retry_policy"`
	Cancellation      string              `json:"cancellation"`
	Delivery          string              `json:"delivery"`
	Idempotency       string              `json:"idempotency"`
	Concurrency       ConcurrencyPolicy   `json:"concurrency"`
	SensitiveFields   []string            `json:"sensitive_fields"`
	DomainLink        string              `json:"domain_link"`
	OrganizationScope string              `json:"organization_scope"`
	SchemaVersions    map[string]string   `json:"schema_versions"`
	Fixtures          map[string][]string `json:"fixtures"`
}

type Registry struct {
	SchemaVersion  int             `json:"schema_version"`
	ContractFamily string          `json:"contract_family"`
	EnvelopeSchema string          `json:"envelope_schema"`
	VersionPolicy  VersionPolicy   `json:"version_policy"`
	Jobs           []JobDefinition `json:"jobs"`
}

type MigrationJob struct {
	Kind             string   `json:"kind"`
	State            string   `json:"state"`
	ProducerVersion  int      `json:"producer_version"`
	ConsumerVersions []int    `json:"consumer_versions"`
	RequiredProfiles []string `json:"required_profiles"`
	Route            string   `json:"route"`
	RollbackRoute    string   `json:"rollback_route"`
	Evidence         []string `json:"evidence"`
}

type MigrationState struct {
	SchemaVersion int            `json:"schema_version"`
	Jobs          []MigrationJob `json:"jobs"`
}

// LoadRegistry reads and validates the checked-in registry.
func LoadRegistry(root string) (Registry, error) {
	return loadRegistry(root, true)
}

func loadRegistry(root string, checkCompiledTypes bool) (Registry, error) {
	data, err := readContractFile(root, registryFilename)
	if err != nil {
		return Registry{}, err
	}
	var registry Registry
	if err := decodeStrict(data, 512*1024, &registry); err != nil {
		return Registry{}, fmt.Errorf("decode %s: %w", registryFilename, err)
	}
	if err := registry.validate(root, checkCompiledTypes); err != nil {
		return Registry{}, err
	}
	return registry, nil
}

func (registry Registry) Validate(root string) error {
	return registry.validate(root, true)
}

func (registry Registry) validate(root string, checkCompiledTypes bool) error {
	if registry.SchemaVersion != 1 || registry.ContractFamily != "dev-health.jobs" {
		return errors.New("unsupported registry identity")
	}
	if registry.EnvelopeSchema != "envelope.schema.json" {
		return errors.New("registry must use envelope.schema.json")
	}
	if registry.VersionPolicy.Compatibility != "additive_optional_only" ||
		registry.VersionPolicy.MinimumConsumerWindow != 2 ||
		registry.VersionPolicy.SameVersionRollout != "schema_digest_all_live_profiles" {
		return errors.New("unsupported version policy")
	}
	if len(registry.Jobs) == 0 {
		return errors.New("registry has no jobs")
	}

	seen := make(map[string]struct{}, len(registry.Jobs))
	previous := ""
	for _, job := range registry.Jobs {
		if !kindPattern.MatchString(job.Kind) || len(job.Kind) > 96 {
			return fmt.Errorf("invalid job kind %q", job.Kind)
		}
		if job.Kind <= previous {
			return errors.New("registry jobs must be sorted by kind")
		}
		previous = job.Kind
		if _, exists := seen[job.Kind]; exists {
			return fmt.Errorf("duplicate job kind %q", job.Kind)
		}
		seen[job.Kind] = struct{}{}
		if err := validateJobDefinition(root, job); err != nil {
			return fmt.Errorf("registry job %s: %w", job.Kind, err)
		}
		if checkCompiledTypes {
			compiled, ok := definitions[job.Kind]
			if !ok {
				return fmt.Errorf("registry job %s has no Go decoder", job.Kind)
			}
			if compiled.CurrentVersion != job.CurrentVersion ||
				!equalInts(compiled.SupportedVersions, job.SupportedVersions) ||
				compiled.DomainLink != job.DomainLink ||
				compiled.OrganizationScope != job.OrganizationScope {
				return fmt.Errorf("registry job %s drifts from Go contract types", job.Kind)
			}
		}
	}
	if checkCompiledTypes && len(seen) != len(definitions) {
		return errors.New("Go contract type has no registry entry")
	}
	return nil
}

func validateJobDefinition(root string, job JobDefinition) error {
	if job.CurrentVersion < 1 || !strictlyIncreasing(job.SupportedVersions) {
		return errors.New("supported_versions must be sorted unique positive integers")
	}
	if !containsVersion(job.SupportedVersions, job.CurrentVersion) {
		return errors.New("supported_versions omits current_version")
	}
	if job.CurrentVersion > 1 && !containsVersion(job.SupportedVersions, job.CurrentVersion-1) {
		return errors.New("supported_versions omits N-1")
	}
	for versionKey, schemaPath := range job.SchemaVersions {
		version, err := strconv.Atoi(versionKey)
		if err != nil || version < 1 || strconv.Itoa(version) != versionKey {
			return errors.New("schema_versions keys must be positive canonical integers")
		}
		if _, err := readContractFile(root, schemaPath); err != nil {
			return fmt.Errorf("schema version %d: %w", version, err)
		}
	}
	for versionKey, fixtures := range job.Fixtures {
		version, err := strconv.Atoi(versionKey)
		if err != nil || version < 1 || strconv.Itoa(version) != versionKey {
			return errors.New("fixtures keys must be positive canonical integers")
		}
		if len(fixtures) == 0 {
			return fmt.Errorf("version %d has no golden fixture", version)
		}
		for _, fixture := range fixtures {
			if _, err := readContractFile(root, fixture); err != nil {
				return fmt.Errorf("fixture version %d: %w", version, err)
			}
		}
	}
	for _, version := range job.SupportedVersions {
		key := strconv.Itoa(version)
		if _, ok := job.SchemaVersions[key]; !ok {
			return fmt.Errorf("version %d has no schema", version)
		}
		if len(job.Fixtures[key]) == 0 {
			return fmt.Errorf("version %d has no golden fixture", version)
		}
	}
	if !matchesBounded(profilePattern, job.Profile, 32) ||
		!matchesBounded(queuePattern, job.Queue, 96) ||
		!matchesBounded(handlerOwnerPattern, job.HandlerOwner, 128) {
		return errors.New("runtime routing policy has invalid identifiers")
	}
	if job.Priority < 1 || job.Priority > 4 ||
		job.TimeoutSeconds < 1 || job.TimeoutSeconds > 86400 ||
		job.MaxAttempts < 1 || job.MaxAttempts > 25 {
		return errors.New("execution policy has invalid bounds")
	}
	if job.OrganizationScope != "global" && job.OrganizationScope != "tenant" {
		return errors.New("organization_scope must be global or tenant")
	}
	if job.ExecutionMode != "command" && job.ExecutionMode != "coordinator" {
		return errors.New("execution_mode must be command or coordinator")
	}
	if !containsString([]string{"replay_safe_at_least_once", "guarded_at_least_once", "at_most_once", "non_retryable"}, job.Delivery) {
		return errors.New("delivery policy is invalid")
	}
	if !matchesBounded(domainTypePattern, job.RetryPolicy, 64) ||
		!matchesBounded(domainTypePattern, job.Cancellation, 64) ||
		!matchesBounded(domainTypePattern, job.Idempotency, 96) {
		return errors.New("execution policy has invalid identifiers")
	}
	if !containsString([]string{"process", "fleet", "organization"}, job.Concurrency.Scope) {
		return errors.New("concurrency scope is invalid")
	}
	if !matchesBounded(domainTypePattern, job.DomainLink, 64) {
		return errors.New("domain_link is invalid")
	}
	if job.Concurrency.Limit < 1 || job.Concurrency.Limit > 10000 {
		return errors.New("concurrency policy is incomplete")
	}
	seenSensitiveFields := make(map[string]struct{}, len(job.SensitiveFields))
	for _, field := range job.SensitiveFields {
		if !domainTypePattern.MatchString(field) {
			return errors.New("sensitive_fields contains an invalid field")
		}
		if _, exists := seenSensitiveFields[field]; exists {
			return errors.New("sensitive_fields contains a duplicate field")
		}
		seenSensitiveFields[field] = struct{}{}
	}
	return nil
}

func matchesBounded(pattern *regexp.Regexp, value string, maximum int) bool {
	return len(value) > 0 && len(value) <= maximum && pattern.MatchString(value)
}

// LoadMigrationState reads and validates migration-state.json against registry.
func LoadMigrationState(root string, registry Registry) (MigrationState, error) {
	data, err := readContractFile(root, migrationFilename)
	if err != nil {
		return MigrationState{}, err
	}
	var state MigrationState
	if err := decodeStrict(data, 512*1024, &state); err != nil {
		return MigrationState{}, fmt.Errorf("decode %s: %w", migrationFilename, err)
	}
	if err := state.Validate(registry); err != nil {
		return MigrationState{}, err
	}
	return state, nil
}

func (state MigrationState) Validate(registry Registry) error {
	if state.SchemaVersion != 1 {
		return errors.New("unsupported migration-state schema_version")
	}
	definitionsByKind := make(map[string]JobDefinition, len(registry.Jobs))
	for _, definition := range registry.Jobs {
		definitionsByKind[definition.Kind] = definition
	}
	if len(state.Jobs) != len(registry.Jobs) {
		return errors.New("migration state must cover every registry job exactly once")
	}
	previous := ""
	seen := make(map[string]struct{}, len(state.Jobs))
	for _, job := range state.Jobs {
		if job.Kind <= previous {
			return errors.New("migration jobs must be sorted by kind")
		}
		previous = job.Kind
		definition, ok := definitionsByKind[job.Kind]
		if !ok {
			return fmt.Errorf("migration job %s is not registered", job.Kind)
		}
		if _, duplicate := seen[job.Kind]; duplicate {
			return fmt.Errorf("duplicate migration job %s", job.Kind)
		}
		seen[job.Kind] = struct{}{}
		if !containsVersion(definition.SupportedVersions, job.ProducerVersion) {
			return fmt.Errorf("migration job %s producer version is unsupported", job.Kind)
		}
		if !equalInts(job.ConsumerVersions, definition.SupportedVersions) {
			return fmt.Errorf("migration job %s consumer versions drift from registry", job.Kind)
		}
		if !containsString(job.RequiredProfiles, definition.Profile) {
			return fmt.Errorf("migration job %s omits registry profile", job.Kind)
		}
		if !strictlyIncreasing(job.ConsumerVersions) || !sortedUniqueStrings(job.RequiredProfiles) || !sortedUniqueStrings(job.Evidence) {
			return fmt.Errorf("migration job %s has unsorted or duplicate policy values", job.Kind)
		}
		if !containsString([]string{"inventory", "contract_frozen", "go_implemented", "shadow", "canary", "go_default", "celery_fallback_only", "celery_removed"}, job.State) {
			return fmt.Errorf("migration job %s has invalid state", job.Kind)
		}
		if !containsString([]string{"celery", "shadow", "river_canary", "river", "removed"}, job.Route) ||
			!containsString([]string{"celery", "river", "none"}, job.RollbackRoute) {
			return fmt.Errorf("migration job %s has invalid routing", job.Kind)
		}
		if job.State == "contract_frozen" && (!containsString(job.Evidence, "contract_schema") || !containsString(job.Evidence, "cross_language_golden")) {
			return fmt.Errorf("migration job %s lacks contract-frozen evidence", job.Kind)
		}
	}
	return nil
}

// ValidateTree verifies the registry, schemas, migration state, fixture safety,
// and canonical encodings without requiring a JSON Schema runtime dependency.
func ValidateTree(root string) error {
	registry, err := LoadRegistry(root)
	if err != nil {
		return err
	}
	if _, err := LoadMigrationState(root, registry); err != nil {
		return err
	}
	envelopeSchema, err := readContractFile(root, registry.EnvelopeSchema)
	if err != nil {
		return err
	}
	if err := validateEnvelopeSchema(envelopeSchema); err != nil {
		return fmt.Errorf("validate %s: %w", registry.EnvelopeSchema, err)
	}
	for _, artifact := range []string{
		"registry.schema.json",
		"migration-state.schema.json",
		"capability-report.schema.json",
		"deployment-profiles.schema.json",
	} {
		data, err := readContractFile(root, artifact)
		if err != nil {
			return err
		}
		var value any
		if err := decodeGeneric(data, &value); err != nil {
			return fmt.Errorf("validate %s: %w", artifact, err)
		}
	}
	for _, job := range registry.Jobs {
		for _, version := range job.SupportedVersions {
			key := strconv.Itoa(version)
			schemaData, err := readContractFile(root, job.SchemaVersions[key])
			if err != nil {
				return err
			}
			if err := validatePayloadSchema(job.Kind, version, schemaData); err != nil {
				return fmt.Errorf("schema %s: %w", job.SchemaVersions[key], err)
			}
			for _, fixturePath := range job.Fixtures[key] {
				fixture, err := readContractFile(root, fixturePath)
				if err != nil {
					return err
				}
				if err := validateFixtureSafety(fixture); err != nil {
					return fmt.Errorf("fixture %s: %w", fixturePath, err)
				}
				envelope, err := Decode(job.Kind, fixture)
				if err != nil {
					return fmt.Errorf("fixture %s: %w", fixturePath, err)
				}
				canonical, err := MarshalCanonical(envelope)
				if err != nil {
					return fmt.Errorf("fixture %s: %w", fixturePath, err)
				}
				if !bytes.Equal(canonical, fixture) {
					return fmt.Errorf("fixture %s is not canonical", fixturePath)
				}
			}
		}
	}
	return nil
}

func validateEnvelopeSchema(data []byte) error {
	var schema map[string]any
	if err := decodeGeneric(data, &schema); err != nil {
		return err
	}
	expectedProperties := []string{
		"contract_version", "organization_id", "correlation_id",
		"idempotency_key", "domain", "payload",
	}
	expectedRequired := []string{
		"contract_version", "correlation_id", "idempotency_key", "domain", "payload",
	}
	properties, ok := schema["properties"].(map[string]any)
	if schema["type"] != "object" || schema["additionalProperties"] != false || !ok {
		return errors.New("envelope must be a closed bounded object")
	}
	if !equalStringSet(keySet(properties), expectedProperties) || !equalStringSet(stringSet(schema["required"]), expectedRequired) {
		return errors.New("envelope fields drift from compiled type")
	}
	contractVersion, ok := properties["contract_version"].(map[string]any)
	if !ok || contractVersion["type"] != "integer" || fmt.Sprint(contractVersion["minimum"]) != "1" {
		return errors.New("contract_version schema is invalid")
	}
	domain, ok := properties["domain"].(map[string]any)
	if !ok || domain["type"] != "object" || domain["additionalProperties"] != false || !equalStringSet(stringSet(domain["required"]), []string{"type", "id"}) {
		return errors.New("domain schema is invalid")
	}
	payload, ok := properties["payload"].(map[string]any)
	if !ok || payload["type"] != "object" || fmt.Sprint(payload["maxProperties"]) != "32" {
		return errors.New("payload envelope schema is invalid")
	}
	return nil
}

func validatePayloadSchema(kind string, version int, data []byte) error {
	var schema map[string]any
	if err := decodeGeneric(data, &schema); err != nil {
		return err
	}
	if schema["type"] != "object" || schema["additionalProperties"] != false {
		return errors.New("payload schema must be a closed object")
	}
	properties, ok := schema["properties"].(map[string]any)
	if !ok {
		return errors.New("payload schema has no properties object")
	}
	if version != ContractVersionV1 {
		return fmt.Errorf("no compiled schema validator for version %d", version)
	}
	expectedFields := map[string][]string{
		KindBillingNotification:    {"notification_id"},
		KindWebhookDelivery:        {"delivery_id"},
		KindReportExecuteOnDemand:  {"report_id"},
		KindReportExecuteScheduled: {"report_id"},
		KindDailyMetricsDispatch:   {"run_id"},
		KindDailyMetricsPartition:  {"partition_id"},
		KindDailyMetricsFinalize:   {"run_id"},
		KindWorkGraphBuild:         {"request_id"},
		KindInvestmentMaterialize:  {"request_id"},
		KindInvestmentDispatch:     {"request_id"},
		KindInvestmentChunk:        {"chunk_id"},
		KindInvestmentFinalize:     {"run_id"},
		KindHeartbeat:              {"scheduled_for"},
		KindRetentionCleanup:       {"batch_size", "delete_before", "retention_policy"},
	}[kind]
	if expectedFields == nil || !equalStringSet(stringSet(schema["required"]), expectedFields) || !equalStringSet(keySet(properties), expectedFields) {
		return errors.New("schema fields drift from compiled payload type")
	}
	if kind == KindHeartbeat {
		return validateTimestampProperty(properties["scheduled_for"])
	}
	if kind == KindReportExecuteOnDemand || kind == KindReportExecuteScheduled {
		return validateUUIDProperty(properties["report_id"])
	}
	if kind == KindDailyMetricsDispatch || kind == KindDailyMetricsFinalize || kind == KindInvestmentFinalize {
		return validateUUIDProperty(properties["run_id"])
	}
	if kind == KindDailyMetricsPartition {
		return validateUUIDProperty(properties["partition_id"])
	}
	if kind == KindInvestmentChunk {
		return validateUUIDProperty(properties["chunk_id"])
	}
	if kind == KindWorkGraphBuild || kind == KindInvestmentMaterialize || kind == KindInvestmentDispatch {
		return validateUUIDProperty(properties["request_id"])
	}
	if kind == KindBillingNotification {
		return validateUUIDProperty(properties["notification_id"])
	}
	if kind == KindWebhookDelivery {
		return validateUUIDProperty(properties["delivery_id"])
	}
	batch, ok := properties["batch_size"].(map[string]any)
	if !ok || batch["type"] != "integer" || fmt.Sprint(batch["minimum"]) != "1" || fmt.Sprint(batch["maximum"]) != "1000" {
		return errors.New("batch_size schema drifts from compiled bounds")
	}
	if err := validateTimestampProperty(properties["delete_before"]); err != nil {
		return fmt.Errorf("delete_before: %w", err)
	}
	policy, ok := properties["retention_policy"].(map[string]any)
	if !ok || policy["type"] != "string" {
		return errors.New("retention_policy schema drifts from compiled type")
	}
	enum, ok := policy["enum"].([]any)
	if !ok || len(enum) != 1 || enum[0] != RetentionWorkerTerminal {
		return errors.New("retention_policy schema drifts from compiled values")
	}
	return nil
}

func validateUUIDProperty(value any) error {
	property, ok := value.(map[string]any)
	if !ok || property["type"] != "string" || property["format"] != "uuid" ||
		property["pattern"] != "^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" {
		return errors.New("UUID schema drifts from compiled type")
	}
	return nil
}

func validateTimestampProperty(value any) error {
	property, ok := value.(map[string]any)
	if !ok || property["type"] != "string" || property["format"] != "date-time" || property["pattern"] != "Z$" {
		return errors.New("timestamp schema drifts from compiled UTC type")
	}
	return nil
}

func keySet(values map[string]any) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for key := range values {
		result[key] = struct{}{}
	}
	return result
}

func equalStringSet(actual map[string]struct{}, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for _, value := range expected {
		if _, ok := actual[value]; !ok {
			return false
		}
	}
	return true
}

func validateFixtureSafety(data []byte) error {
	var value any
	if err := decodeGeneric(data, &value); err != nil {
		return err
	}
	return walkSafeFixture(value)
}

var forbiddenKeys = map[string]struct{}{
	"access_token": {}, "api_key": {}, "authorization": {}, "cookie": {},
	"credential": {}, "credentials": {}, "database_url": {}, "dsn": {},
	"headers": {}, "password": {}, "private_key": {}, "provider_payload": {},
	"raw_payload": {}, "secret": {}, "sql": {}, "token": {}, "webhook_body": {},
}

func walkSafeFixture(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if _, forbidden := forbiddenKeys[strings.ToLower(key)]; forbidden {
				return fmt.Errorf("forbidden field %q", key)
			}
			if err := walkSafeFixture(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := walkSafeFixture(child); err != nil {
				return err
			}
		}
	case string:
		lower := strings.ToLower(typed)
		for _, marker := range []string{"postgres://", "postgresql://", "redis://", "valkey://", "bearer ", "-----begin", "password="} {
			if strings.Contains(lower, marker) {
				return errors.New("fixture contains a forbidden secret or connection marker")
			}
		}
	}
	return nil
}

func decodeGeneric(data []byte, destination any) error {
	if err := validateJSONTokens(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	return nil
}

func readContractFile(root, relative string) ([]byte, error) {
	if relative == "" || filepath.IsAbs(relative) {
		return nil, errors.New("contract path must be relative")
	}
	clean := filepath.Clean(relative)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return nil, errors.New("contract path escapes root")
	}
	rootAbsolute, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve contract root: %w", err)
	}
	path := filepath.Join(rootAbsolute, clean)
	if path != rootAbsolute && !strings.HasPrefix(path, rootAbsolute+string(filepath.Separator)) {
		return nil, errors.New("contract path escapes root")
	}
	resolvedRoot, err := filepath.EvalSymlinks(rootAbsolute)
	if err != nil {
		return nil, fmt.Errorf("resolve contract root links: %w", err)
	}
	resolvedPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, fmt.Errorf("resolve contract artifact %s: %w", relative, err)
	}
	if resolvedPath != resolvedRoot && !strings.HasPrefix(resolvedPath, resolvedRoot+string(filepath.Separator)) {
		return nil, errors.New("contract path escapes root through a symbolic link")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("read contract artifact %s: %w", relative, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("contract artifact %s must be a regular file", relative)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read contract artifact %s: %w", relative, err)
	}
	return data, nil
}

func strictlyIncreasing(values []int) bool {
	if len(values) == 0 {
		return false
	}
	for index, value := range values {
		if value < 1 || (index > 0 && value <= values[index-1]) {
			return false
		}
	}
	return true
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func sortedUniqueStrings(values []string) bool {
	if len(values) == 0 {
		return false
	}
	for index, value := range values {
		if value == "" || (index > 0 && value <= values[index-1]) {
			return false
		}
	}
	return true
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
