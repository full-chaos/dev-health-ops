package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitLabFeatureFlagsOracleFlag struct {
	OrgID       string     `json:"org_id"`
	Provider    string     `json:"provider"`
	FlagKey     string     `json:"flag_key"`
	ProjectKey  string     `json:"project_key"`
	RepoID      *string    `json:"repo_id"`
	Environment string     `json:"environment"`
	FlagType    string     `json:"flag_type"`
	CreatedAt   *time.Time `json:"created_at"`
	ArchivedAt  *time.Time `json:"archived_at"`
	LastSynced  time.Time  `json:"last_synced"`
}

type gitLabFeatureFlagsOracleEvent struct {
	OrgID         string    `json:"org_id"`
	EventType     string    `json:"event_type"`
	FlagKey       string    `json:"flag_key"`
	Environment   string    `json:"environment"`
	RepoID        *string   `json:"repo_id"`
	ActorType     *string   `json:"actor_type"`
	PrevState     *string   `json:"prev_state"`
	NextState     *string   `json:"next_state"`
	EventAt       time.Time `json:"event_ts"`
	IngestedAt    time.Time `json:"ingested_at"`
	SourceEventID *string   `json:"source_event_id"`
	DedupeKey     string    `json:"dedupe_key"`
}

type gitLabFeatureFlagsOracleDate string

func (value gitLabFeatureFlagsOracleDate) OracleDate() string { return string(value) }

type gitLabFeatureFlagsOracleEdge struct {
	EdgeID       string                       `json:"edge_id"`
	SourceType   string                       `json:"source_type"`
	SourceID     string                       `json:"source_id"`
	TargetType   string                       `json:"target_type"`
	TargetID     string                       `json:"target_id"`
	EdgeType     string                       `json:"edge_type"`
	RepoID       *string                      `json:"repo_id"`
	Provider     *string                      `json:"provider"`
	Provenance   string                       `json:"provenance"`
	Confidence   float64                      `json:"confidence"`
	Evidence     string                       `json:"evidence"`
	DiscoveredAt time.Time                    `json:"discovered_at"`
	LastSynced   time.Time                    `json:"last_synced"`
	EventAt      time.Time                    `json:"event_ts"`
	Day          gitLabFeatureFlagsOracleDate `json:"day"`
	OrgID        string                       `json:"org_id"`
}

type gitLabFeatureFlagsOracleTrace struct {
	Flags      []gitLabFeatureFlagsOracleFlag  `json:"flags"`
	Events     []gitLabFeatureFlagsOracleEvent `json:"events"`
	Edges      []gitLabFeatureFlagsOracleEdge  `json:"edges"`
	ProjectKey string                          `json:"project_key"`
	Requests   []string                        `json:"requests"`
	Error      string                          `json:"error"`
}

type gitLabFeatureFlagsOracleDoer struct {
	t         *testing.T
	caseInput map[string]any
	requests  []*http.Request
}

func (doer *gitLabFeatureFlagsOracleDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.requests = append(doer.requests, request)
	path := request.URL.Path
	query := request.URL.Query()
	status := http.StatusOK
	bodyValue := any([]any{})
	headers := make(http.Header)
	if stringsHasSuffix(path, "/feature_flags") {
		status = oracleInt(doer.caseInput["flags_status"], http.StatusOK)
		if configured, ok := doer.caseInput["flags_headers"].(map[string]any); ok {
			for key, value := range configured {
				headers.Set(key, oracleString(value))
			}
		}
		page := oracleInt(query.Get("page"), 1)
		pages, _ := doer.caseInput["flags_pages"].([]any)
		if page-1 >= 0 && page-1 < len(pages) {
			bodyValue = pages[page-1]
		}
		if nextPages, ok := doer.caseInput["next_pages"].([]any); ok && page-1 >= 0 && page-1 < len(nextPages) {
			if next := oracleString(nextPages[page-1]); next != "" {
				headers.Set("X-Next-Page", next)
			}
		}
	} else {
		status = oracleInt(doer.caseInput["project_status"], http.StatusOK)
		bodyValue = doer.caseInput["project_payload"]
	}
	encoded, err := json.Marshal(bodyValue)
	if err != nil {
		doer.t.Fatal(err)
	}
	return &http.Response{
		StatusCode: status, Header: headers, Body: ioNopCloser(bytes.NewReader(encoded)), Request: request,
	}, nil
}

func stringsHasSuffix(value, suffix string) bool {
	return len(value) >= len(suffix) && value[len(value)-len(suffix):] == suffix
}

func oracleInt(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case string:
		parsed, err := strconv.Atoi(typed)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func oracleString(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return strconv.FormatFloat(value.(float64), 'f', -1, 64)
}

func ioNopCloser(reader *bytes.Reader) *oracleReadCloser {
	return &oracleReadCloser{Reader: reader}
}

type oracleReadCloser struct{ *bytes.Reader }

func (oracleReadCloser) Close() error { return nil }

func gitLabFeatureFlagsOracleGoError(err error) string {
	if err == nil {
		return ""
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) {
		switch providerErr.Class {
		case providerfoundation.ErrorRateLimited:
			return "rate_limited"
		case providerfoundation.ErrorAuthentication:
			return "authentication"
		default:
			return "api"
		}
	}
	if errors.Is(err, providerfoundation.ErrPaginationInvalid) ||
		errors.Is(err, providerfoundation.ErrNormalizationInvalid) {
		return "api"
	}
	return "api"
}

func buildGitLabFeatureFlagsOracleTrace(t *testing.T, input map[string]any) gitLabFeatureFlagsOracleTrace {
	t.Helper()
	encodedNormalizedAt, ok := input["normalized_at"].(string)
	if !ok {
		t.Fatalf("normalized_at=%T", input["normalized_at"])
	}
	normalizedAt, err := time.Parse(time.RFC3339Nano, encodedNormalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	doer := &gitLabFeatureFlagsOracleDoer{t: t, caseInput: input}
	client := gitLabFeatureFlagsClient(t, doer, providerfoundation.RetryPolicy{
		MaxAttempts: oracleInt(input["max_retries"], 1),
		InitialWait: time.Nanosecond, MaxWait: time.Nanosecond,
	})
	claim := nativeTestClaim("gitlab", "feature-flags")
	claim.OrgID = oracleString(input["org_id"])
	claim.SourceExternalID = oracleString(input["project_id_or_path"])
	claim.SourceName = claim.SourceExternalID
	handler := GitLabFeatureFlagsRouteHandler{
		MaxPages: oracleInt(input["max_pages"], 0),
		PerPage:  oracleInt(input["per_page"], 0),
	}
	batch, collectErr := handler.Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt,
	)
	trace := gitLabFeatureFlagsOracleTrace{
		Flags:      make([]gitLabFeatureFlagsOracleFlag, 0),
		Events:     make([]gitLabFeatureFlagsOracleEvent, 0),
		Edges:      make([]gitLabFeatureFlagsOracleEdge, 0),
		Requests:   make([]string, 0, len(doer.requests)),
		Error:      gitLabFeatureFlagsOracleGoError(collectErr),
		ProjectKey: claim.SourceExternalID,
	}
	for _, request := range doer.requests {
		trace.Requests = append(trace.Requests, request.URL.RequestURI())
	}
	for _, effect := range batch.Effects {
		switch effect.Destination {
		case "feature_flag":
			for _, raw := range effect.Rows {
				var row launchDarklyFlagRow
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				trace.Flags = append(trace.Flags, gitLabFeatureFlagsOracleFlag{
					OrgID: row.OrgID, Provider: row.Provider, FlagKey: row.FlagKey,
					ProjectKey: row.ProjectKey, RepoID: oracleOptionalString(row.RepoID), Environment: row.Environment,
					FlagType: row.FlagType, CreatedAt: row.CreatedAt,
					ArchivedAt: row.ArchivedAt, LastSynced: row.LastSynced,
				})
			}
		case "feature_flag_event":
			for _, raw := range effect.Rows {
				var row launchDarklyEventRow
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				trace.Events = append(trace.Events, gitLabFeatureFlagsOracleEvent{
					OrgID: row.OrgID, EventType: row.EventType, FlagKey: row.FlagKey,
					Environment: row.Environment, RepoID: oracleOptionalString(row.RepoID),
					ActorType: oracleOptionalString(row.ActorType), PrevState: oracleOptionalString(row.PrevState),
					NextState: oracleOptionalString(row.NextState), EventAt: row.EventAt,
					IngestedAt: row.IngestedAt, SourceEventID: oracleOptionalString(row.SourceEventID),
					DedupeKey: row.DedupeKey,
				})
			}
		case "work_graph_edges":
			for _, raw := range effect.Rows {
				var row launchDarklyEdgeRow
				if err := json.Unmarshal(raw, &row); err != nil {
					t.Fatal(err)
				}
				trace.Edges = append(trace.Edges, gitLabFeatureFlagsOracleEdge{
					EdgeID: row.EdgeID, SourceType: row.SourceType, SourceID: row.SourceID,
					TargetType: row.TargetType, TargetID: row.TargetID, EdgeType: row.EdgeType,
					RepoID: oracleOptionalString(row.RepoID), Provider: oracleOptionalString(row.Provider),
					Provenance: row.Provenance, Confidence: row.Confidence, Evidence: row.Evidence,
					DiscoveredAt: row.DiscoveredAt, LastSynced: row.LastSynced, EventAt: row.EventAt,
					Day: gitLabFeatureFlagsOracleDate(row.Day), OrgID: row.OrgID,
				})
			}
		}
	}
	if project, ok := batch.Result["project_key"].(string); ok {
		trace.ProjectKey = project
	}
	sort.Slice(trace.Edges, func(i, j int) bool {
		if trace.Edges[i].EdgeID != trace.Edges[j].EdgeID {
			return trace.Edges[i].EdgeID < trace.Edges[j].EdgeID
		}
		return trace.Edges[i].EdgeType < trace.Edges[j].EdgeType
	})
	return trace
}

func oracleOptionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func oracleGitLabFeatureFlagsCases() []oracleCase {
	return []oracleCase{
		{ID: "multi_scope_and_full_page_fallback", Input: map[string]any{
			"org_id": "org-acme", "project_id_or_path": "group/project", "per_page": 2,
			"normalized_at": "2026-08-10T12:00:00.123Z",
			"flags_pages": []any{
				[]any{
					map[string]any{
						"name": "checkout", "key": "checkout-key", "active": true, "version": "new_version_flag",
						"created_at": "2026-08-01T10:00:00Z", "updated_at": "2026-08-09T10:00:00Z",
						"strategies": []any{map[string]any{
							"scopes": []any{
								map[string]any{"environment_scope": "production"},
								map[string]any{"environment_scope": "production"},
								map[string]any{"environment_scope": "staging"},
							},
						}},
					},
					map[string]any{
						"name": "search", "active": false,
						"created_at": "2026-08-02T10:00:00Z", "updated_at": "2026-08-09T11:00:00Z",
						"strategies": []any{map[string]any{
							"scopes": []any{map[string]any{"environment_scope": "*"}},
						}},
					},
				},
				[]any{map[string]any{
					"key": "billing", "active": true, "updated_at": "2026-08-09T12:00:00Z",
					"strategies": []any{},
				}},
			},
			"project_payload": map[string]any{"path_with_namespace": "acme/api", "path": "acme/api-short"},
		}},
		{ID: "next_header_and_non_object_project_fallback", Input: map[string]any{
			"org_id": "org-acme", "project_id_or_path": "123", "per_page": 2,
			"normalized_at": "2026-08-10T12:00:00.123Z",
			"flags_pages": []any{
				[]any{map[string]any{
					"name": "checkout", "active": true,
					"created_at": "2026-08-01T10:00:00Z", "updated_at": "2026-08-09T10:00:00Z",
					"strategies": []any{map[string]any{
						"scopes": []any{map[string]any{"environment_scope": "production"}},
					}},
				}},
				[]any{},
			},
			"next_pages": []any{"2", ""}, "project_payload": []any{},
		}},
		{ID: "non_list_flags_is_error", Input: map[string]any{
			"org_id": "org-acme", "project_id_or_path": "123",
			"normalized_at":   "2026-08-10T12:00:00.123Z",
			"flags_pages":     []any{map[string]any{"items": []any{}}},
			"project_payload": map[string]any{"path": "acme/api"},
		}},
		{ID: "plain_forbidden_is_authentication_error", Input: map[string]any{
			"org_id": "org-acme", "project_id_or_path": "123",
			"normalized_at": "2026-08-10T12:00:00.123Z", "flags_status": 403,
			"flags_pages": []any{[]any{}}, "project_payload": map[string]any{},
		}},
		{ID: "qualified_forbidden_is_rate_limited", Input: map[string]any{
			"org_id": "org-acme", "project_id_or_path": "123",
			"normalized_at": "2026-08-10T12:00:00.123Z", "flags_status": 403,
			"flags_headers": map[string]any{"Retry-After": "0"},
			"flags_pages":   []any{[]any{}}, "project_payload": map[string]any{},
		}},
	}
}

func TestGenericOracleMatchesLivePythonGitLabFeatureFlagsRoute(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/feature/flags", oracleGitLabFeatureFlagsCases(),
		buildGitLabFeatureFlagsOracleTrace, nil,
	)
}
