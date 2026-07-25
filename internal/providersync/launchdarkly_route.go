package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const launchDarklyCodeReferenceConfidence = 0.95

type launchDarklyFlagRow struct {
	OrgID       string     `json:"org_id"`
	Provider    string     `json:"provider"`
	FlagKey     string     `json:"flag_key"`
	ProjectKey  string     `json:"project_key"`
	RepoID      string     `json:"repo_id"`
	Environment string     `json:"environment"`
	FlagType    string     `json:"flag_type"`
	CreatedAt   *time.Time `json:"created_at"`
	ArchivedAt  *time.Time `json:"archived_at"`
	LastSynced  time.Time  `json:"last_synced"`
}

type launchDarklyEventRow struct {
	OrgID         string    `json:"org_id"`
	EventType     string    `json:"event_type"`
	FlagKey       string    `json:"flag_key"`
	Environment   string    `json:"environment"`
	RepoID        string    `json:"repo_id"`
	ActorType     string    `json:"actor_type"`
	PrevState     string    `json:"prev_state"`
	NextState     string    `json:"next_state"`
	EventAt       time.Time `json:"event_ts"`
	IngestedAt    time.Time `json:"ingested_at"`
	SourceEventID string    `json:"source_event_id"`
	DedupeKey     string    `json:"dedupe_key"`
}

type launchDarklyLinkRow struct {
	OrgID        string     `json:"org_id"`
	FlagKey      string     `json:"flag_key"`
	TargetType   string     `json:"target_type"`
	TargetID     string     `json:"target_id"`
	Provider     string     `json:"provider"`
	LinkSource   string     `json:"link_source"`
	LinkType     string     `json:"link_type"`
	EvidenceType string     `json:"evidence_type"`
	Confidence   float64    `json:"confidence"`
	ValidFrom    time.Time  `json:"valid_from"`
	ValidTo      *time.Time `json:"valid_to"`
	LastSynced   time.Time  `json:"last_synced"`
}

type launchDarklyEdgeRow struct {
	EdgeID       string    `json:"edge_id"`
	SourceType   string    `json:"source_type"`
	SourceID     string    `json:"source_id"`
	TargetType   string    `json:"target_type"`
	TargetID     string    `json:"target_id"`
	EdgeType     string    `json:"edge_type"`
	RepoID       string    `json:"repo_id"`
	Provider     string    `json:"provider"`
	Provenance   string    `json:"provenance"`
	Confidence   float64   `json:"confidence"`
	Evidence     string    `json:"evidence"`
	DiscoveredAt time.Time `json:"discovered_at"`
	LastSynced   time.Time `json:"last_synced"`
	EventAt      time.Time `json:"event_ts"`
	Day          string    `json:"day"`
	OrgID        string    `json:"org_id"`
}

type LaunchDarklyCodeReferenceResolver interface {
	ResolveLaunchDarklyCodeReferences(
		context.Context,
		Claim,
		string,
		json.RawMessage,
		time.Time,
	) ([]launchDarklyLinkRow, []launchDarklyEdgeRow, error)
}

type LaunchDarklyRouteHandler struct {
	CodeReferences LaunchDarklyCodeReferenceResolver
	MaxFlagPages   int
}

func (handler LaunchDarklyRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "launchdarkly" ||
		claim.Dataset != "feature-flags" || client == nil ||
		client.Provider != "launchdarkly" || handler.CodeReferences == nil ||
		normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	projectKey := credentialString(credential, "project_key")
	if projectKey == "" {
		projectKey = optionString(claim.DatasetOptions, "project_key")
	}
	if projectKey == "" {
		projectKey = optionString(claim.IntegrationConfig, "project_key")
	}
	environment := credentialString(credential, "environment")
	if environment == "" {
		environment = optionString(claim.DatasetOptions, "environment")
	}
	if environment == "" {
		environment = optionString(claim.IntegrationConfig, "environment")
	}
	if projectKey == "" {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	maxPages := handler.MaxFlagPages
	if maxPages == 0 {
		maxPages = nativeMaxPages
	}
	flagsPage, err := providerfoundation.CollectLaunchDarklyOffsetPages(
		ctx,
		client,
		providerfoundation.LaunchDarklyOffsetOptions{
			Path: "/api/v2/flags/" + url.PathEscape(projectKey), MaxPages: maxPages,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, fmt.Errorf("launchdarkly flags pagination: %w", err)
	}
	auditPage, err := providerfoundation.CollectLaunchDarklyAuditPages(
		ctx,
		client,
		providerfoundation.LaunchDarklyAuditOptions{
			Since: claim.SinceAt, MaxItems: 1_000,
		},
	)
	if err != nil {
		return CompleteRouteBatch{}, fmt.Errorf("launchdarkly audit pagination: %w", err)
	}
	codeReferencePayload, codeReferenceErr := fetchLaunchDarklyCodeReferences(
		ctx, client, projectKey,
	)
	if codeReferenceErr != nil {
		if ctx.Err() != nil {
			return CompleteRouteBatch{}, ctx.Err()
		}
		if leaseErr := client.Lease.Assert(ctx); leaseErr != nil {
			return CompleteRouteBatch{}, leaseErr
		}
		codeReferencePayload = json.RawMessage(`{"items":[]}`)
	}
	codeReferences, err := parseLaunchDarklyCodeReferences(codeReferencePayload)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	flags, err := normalizeLaunchDarklyFlags(
		flagsPage.Items, claim.OrgID, environment, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	events, err := normalizeLaunchDarklyEvents(
		auditPage.Items, claim.OrgID, environment, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	links, codeEdges, err := handler.CodeReferences.ResolveLaunchDarklyCodeReferences(
		ctx, claim, projectKey, codeReferencePayload, normalizedAt,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	edges := append(codeEdges, launchDarklyFlagEdges(
		flags, events, projectKey, normalizedAt,
	)...)
	flagEffect, err := effectBatchFromValues(
		"feature_flag", EffectReplaySafe, flags,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	eventEffect, err := effectBatchFromValues(
		"feature_flag_event", EffectReadbackRequired, events,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	linkEffect, err := effectBatchFromValues(
		"feature_flag_link", EffectReplaySafe, links,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	edgeEffect, err := effectBatchFromValues(
		"work_graph_edges", EffectReplaySafe, edges,
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	watermark := claim.BeforeAt
	result := map[string]any{
		"flags_synced":                len(flags),
		"events_synced":               len(events),
		"code_references_synced":      len(codeReferences),
		"code_reference_links_synced": len(links),
		"code_reference_edges_synced": len(codeEdges),
		"code_references_error":       nil,
		"project_key":                 projectKey,
		"environment":                 nil,
	}
	if codeReferenceErr != nil {
		result["code_references_error"] = "provider_request_failed"
	}
	if environment != "" {
		result["environment"] = environment
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{flagEffect, eventEffect, linkEffect, edgeEffect},
		Result:  result, Watermark: watermark,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests:   flagsPage.Pages + auditPage.Pages + 1,
			Pages:      flagsPage.Pages + auditPage.Pages,
			Records:    len(flags) + len(events) + len(links) + len(edges),
			CapReached: flagsPage.CapReached || auditPage.CapReached,
		},
	}, nil
}

func credentialString(credential providerfoundation.Credential, name string) string {
	if value, ok := credential.Secret(name); ok && value.Configured() {
		return strings.TrimSpace(value.Reveal())
	}
	return strings.TrimSpace(credential.Config[name])
}

func optionString(options map[string]any, name string) string {
	value, ok := options[name]
	if !ok || value == nil {
		return ""
	}
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func fetchLaunchDarklyCodeReferences(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	projectKey string,
) (json.RawMessage, error) {
	query := url.Values{
		"withReferencesForDefaultBranch": {"1"},
		"projKey":                        {projectKey},
	}
	response, err := client.Do(
		ctx,
		http.MethodGet,
		"/api/v2/code-refs/repositories?"+query.Encode(),
		nil,
	)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, nativeMaxObjectBytes+1))
	if err != nil || len(body) > nativeMaxObjectBytes || !json.Valid(body) {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	return append(json.RawMessage(nil), body...), nil
}

func normalizeLaunchDarklyFlags(
	items []json.RawMessage,
	orgID string,
	environment string,
	normalizedAt time.Time,
) ([]launchDarklyFlagRow, error) {
	rows := make([]launchDarklyFlagRow, 0, len(items))
	for _, raw := range items {
		var flag map[string]any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&flag) != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		created := parseLaunchDarklyTime(flag["creationDate"])
		rows = append(rows, launchDarklyFlagRow{
			OrgID: orgID, Provider: "launchdarkly",
			FlagKey:     stringValue(flag["key"]),
			ProjectKey:  stringValue(flag["_projectKey"]),
			Environment: environment,
			FlagType:    valueOr(stringValue(flag["kind"]), "boolean"),
			CreatedAt:   created, LastSynced: normalizedAt.UTC(),
		})
	}
	return rows, nil
}

func normalizeLaunchDarklyEvents(
	items []json.RawMessage,
	orgID string,
	environmentOverride string,
	normalizedAt time.Time,
) ([]launchDarklyEventRow, error) {
	kindMap := map[string]string{
		"createFlag": "create", "updateFlag": "update",
		"toggleFlag": "toggle", "updateFlagVariations": "rule",
		"updateFlagDefaultRule": "rollout",
	}
	rows := make([]launchDarklyEventRow, 0, len(items))
	for _, raw := range items {
		var event map[string]any
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.UseNumber()
		if decoder.Decode(&event) != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		rawKind := stringValue(event["kind"])
		eventKind := kindMap[rawKind]
		if eventKind == "" {
			eventKind = rawKind
		}
		member, _ := event["member"].(map[string]any)
		actor := stringValue(member["email"])
		if actor == "" {
			actor = stringValue(member["_id"])
		}
		flagKey, environment := launchDarklyEventTarget(event)
		if flagKey == "" {
			flagKey = stringValue(event["name"])
		}
		if environment == "" {
			environment = environmentOverride
		}
		eventAt := parseLaunchDarklyTime(event["date"])
		if eventAt == nil {
			fallback := normalizedAt.UTC()
			eventAt = &fallback
		}
		entryID := stringValue(event["_id"])
		rows = append(rows, launchDarklyEventRow{
			OrgID: orgID, EventType: eventKind, FlagKey: flagKey,
			Environment: environment, ActorType: actor,
			EventAt: eventAt.UTC(), IngestedAt: normalizedAt.UTC(),
			SourceEventID: entryID, DedupeKey: entryID,
		})
	}
	return rows, nil
}

func launchDarklyEventTarget(event map[string]any) (string, string) {
	target, _ := event["target"].(map[string]any)
	resources, _ := target["resources"].([]any)
	flagKey, environment := "", ""
	for _, raw := range resources {
		resource, ok := raw.(string)
		if !ok {
			continue
		}
		if marker := strings.Index(resource, ":env/"); marker >= 0 {
			part := resource[marker+len(":env/"):]
			environment = strings.SplitN(part, ":", 2)[0]
		}
		if marker := strings.LastIndex(resource, "/flags/"); marker >= 0 {
			flagKey = resource[marker+len("/flags/"):]
			break
		}
		if marker := strings.LastIndex(resource, ":flag/"); marker >= 0 {
			flagKey = resource[marker+len(":flag/"):]
			break
		}
	}
	return flagKey, environment
}

func parseLaunchDarklyTime(value any) *time.Time {
	switch typed := value.(type) {
	case json.Number:
		millis, err := typed.Int64()
		if err != nil {
			return nil
		}
		parsed := time.UnixMilli(millis).UTC()
		return &parsed
	case float64:
		parsed := time.UnixMilli(int64(typed)).UTC()
		return &parsed
	case int64:
		parsed := time.UnixMilli(typed).UTC()
		return &parsed
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, typed)
		if err != nil {
			return nil
		}
		parsed = parsed.UTC()
		return &parsed
	default:
		return nil
	}
}

func launchDarklyFlagEdges(
	flags []launchDarklyFlagRow,
	events []launchDarklyEventRow,
	fallbackProjectKey string,
	normalizedAt time.Time,
) []launchDarklyEdgeRow {
	latest := map[string]launchDarklyEventRow{}
	for _, event := range events {
		current, ok := latest[event.FlagKey]
		if !ok || event.EventAt.After(current.EventAt) {
			latest[event.FlagKey] = event
		}
	}
	edges := make([]launchDarklyEdgeRow, 0, len(flags)*2)
	for _, flag := range flags {
		projectKey := valueOr(flag.ProjectKey, fallbackProjectKey)
		flagID := launchDarklyFeatureFlagID(
			flag.OrgID, flag.Provider, projectKey, flag.FlagKey,
		)
		eventAt := normalizedAt.UTC()
		if flag.CreatedAt != nil {
			eventAt = flag.CreatedAt.UTC()
		}
		edges = append(edges, newLaunchDarklyEdge(
			flag.OrgID, flagID, "feature_flag", flagID, "feature_flag",
			"relates", "", flag.Provider, 1.0,
			"flag:"+flag.Provider+"/"+projectKey+"/"+flag.FlagKey,
			eventAt, normalizedAt,
		))
		if event, ok := latest[flag.FlagKey]; ok {
			evidence := pythonISOTime(event.EventAt) + "|" +
				event.EventType + "|" + event.NextState
			edges = append(edges, newLaunchDarklyEdge(
				flag.OrgID, flagID, "feature_flag", flagID, "feature_flag",
				"config_changed_by", "", flag.Provider, 1.0,
				evidence, event.EventAt, normalizedAt,
			))
		}
	}
	return edges
}

func newLaunchDarklyEdge(
	orgID, sourceID, sourceType, targetID, targetType, edgeType, repoID,
	provider string,
	confidence float64,
	evidence string,
	eventAt time.Time,
	normalizedAt time.Time,
) launchDarklyEdgeRow {
	edgeID := launchDarklyEdgeID(
		sourceType, sourceID, edgeType, targetType, targetID,
	)
	return launchDarklyEdgeRow{
		EdgeID: edgeID, SourceType: sourceType, SourceID: sourceID,
		TargetType: targetType, TargetID: targetID, EdgeType: edgeType,
		RepoID: repoID, Provider: provider, Provenance: "native",
		Confidence: confidence, Evidence: evidence,
		DiscoveredAt: normalizedAt.UTC(), LastSynced: normalizedAt.UTC(),
		EventAt: eventAt.UTC(), Day: eventAt.UTC().Format("2006-01-02"),
		OrgID: orgID,
	}
}

func launchDarklyFeatureFlagID(orgID, provider, projectKey, flagKey string) string {
	digest := sha256.Sum256([]byte(
		"flag:" + orgID + "/" + provider + "/" + projectKey + "/" + flagKey,
	))
	return hex.EncodeToString(digest[:])
}

func launchDarklyEdgeID(
	sourceType, sourceID, edgeType, targetType, targetID string,
) string {
	digest := sha256.Sum256([]byte(
		sourceType + ":" + sourceID + "|" + edgeType + "|" +
			targetType + ":" + targetID,
	))
	return hex.EncodeToString(digest[:])
}

func pythonISOTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.999999-07:00")
}

func valueOr(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func effectBatchFromValues[T any](
	destination string,
	recovery EffectRecoveryPolicy,
	values []T,
) (EffectBatch, error) {
	rows := make([]json.RawMessage, 0, len(values))
	for _, value := range values {
		encoded, err := json.Marshal(value)
		if err != nil {
			return EffectBatch{}, ErrEffectRecoveryUnsafe
		}
		rows = append(rows, encoded)
	}
	return BuildEffectBatch(destination, recovery, rows)
}

var _ CompleteRouteHandler = LaunchDarklyRouteHandler{}
