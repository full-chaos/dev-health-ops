package operationaledges

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// mappingConfidence and mappingProvenance mirror operational_edges.py's
// _mapping_confidence/_mapping_provenance helpers.
func mappingConfidence(raw *float64) float32 {
	if raw == nil {
		return 0
	}
	return float32(*raw)
}

func mappingProvenance(source string) string {
	if source == "bounded_service_repository_heuristic" {
		return edges.ProvenanceHeuristic
	}
	return edges.ProvenanceNative
}

// MappingRow is one operational_service_repository_mappings row.
type MappingRow struct {
	ServiceID              string
	RepoID                 *uuid.UUID
	Provider               string
	RelationshipProvenance string
	RelationshipConfidence *float64
	MappingKind            string
	RuleID                 string
	SourceURL              string
}

// ReadServiceRepositoryMappings ports operational_edges.py's `mappings` read
// (operational_current.py table `operational_service_repository_mappings`).
//
// CHAOS-4269 NULL-guard, port-with-fix (approved by team-lead, same as
// LoadIncidentsStarted/daily/incident_native_clickhouse.go): Python's own
// `valid_from <= {now}` has NO NULL-OK guard, unlike the symmetric
// `(valid_to IS NULL OR valid_to > {now})` two lines below it in the source.
// map_issue_incidents, the only writer of mapping_kind="repository_derived"
// rows, never sets valid_from (NULL by dataclass default), so ClickHouse's
// three-valued logic silently drops every one of those mappings from a
// faithful port -- confirmed live on CHAOS-4269/CHAOS-4295. This reader adds
// `(valid_from IS NULL OR valid_from <= {now})`, mirroring valid_to's own
// shape, per the standing port-with-fix order (no Python patch, the fix
// lands only in the native Go port).
func ReadServiceRepositoryMappings(
	ctx context.Context, conn driver.Conn, organizationID string, now time.Time, repoID *uuid.UUID,
) ([]MappingRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("mapping ordering contract: %w", err)
	}
	slog.Default().InfoContext(ctx, "workgraph operationaledges: resolved ordering contract",
		"org_id", organizationID, "table", "operational_service_repository_mappings", "contract", contract)

	filters := []string{
		"is_active = 1",
		"(valid_from IS NULL OR valid_from <= {now:DateTime64(6, 'UTC')})",
		"(valid_to IS NULL OR valid_to > {now:DateTime64(6, 'UTC')})",
	}
	if repoID != nil {
		filters = append(filters, "repo_id = {repo_id:UUID}")
	}
	query := "SELECT service_id, repo_id, provider, relationship_provenance, " +
		"relationship_confidence, mapping_kind, rule_id, source_url FROM " +
		remaining.CurrentOperationalRowsSQL("operational_service_repository_mappings", filters, contract)

	args := []any{
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("now", remaining.DateTime64Argument(now, remaining.DateTime64MicrosecondPrecision)),
	}
	if repoID != nil {
		args = append(args, clickhouse.Named("repo_id", repoID.String()))
	}

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read operational_service_repository_mappings: %w", err)
	}
	defer rows.Close()

	var out []MappingRow
	for rows.Next() {
		var (
			r          MappingRow
			repoIDStr  *string
			confidence *float64
		)
		if err := rows.Scan(
			&r.ServiceID, &repoIDStr, &r.Provider, &r.RelationshipProvenance,
			&confidence, &r.MappingKind, &r.RuleID, &r.SourceURL,
		); err != nil {
			return nil, fmt.Errorf("scan operational_service_repository_mappings row: %w", err)
		}
		r.RelationshipConfidence = confidence
		if repoIDStr != nil && *repoIDStr != "" {
			parsed, err := uuid.Parse(*repoIDStr)
			if err != nil {
				return nil, fmt.Errorf("mapping row %s: parse repo_id %q: %w", r.ServiceID, *repoIDStr, err)
			}
			r.RepoID = &parsed
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operational_service_repository_mappings: %w", err)
	}
	return out, nil
}

// IncidentRow is one operational_incidents row (this package's own shape --
// distinct from daily.IncidentRow, which serves the DORA/daily-metrics join).
type IncidentRow struct {
	ID                 string
	ServiceID          string
	EscalationPolicyID string
	StartedAt          *time.Time
	SourceURL          string
}

// ReadIncidents ports operational_edges.py's `incidents` read.
func ReadIncidents(
	ctx context.Context, conn driver.Conn, organizationID string,
	fromDate, toDate *time.Time,
) ([]IncidentRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("incident ordering contract: %w", err)
	}
	slog.Default().InfoContext(ctx, "workgraph operationaledges: resolved ordering contract",
		"org_id", organizationID, "table", "operational_incidents", "contract", contract)

	filters := []string{"is_deleted = 0"}
	args := []any{clickhouse.Named("org_id", organizationID)}
	if fromDate != nil {
		filters = append(filters, "started_at >= {from_date:DateTime64(3, 'UTC')}")
		args = append(args, clickhouse.Named("from_date", remaining.DateTime64Argument(*fromDate, remaining.DateTime64MillisecondPrecision)))
	}
	if toDate != nil {
		filters = append(filters, "started_at <= {to_date:DateTime64(3, 'UTC')}")
		args = append(args, clickhouse.Named("to_date", remaining.DateTime64Argument(*toDate, remaining.DateTime64MillisecondPrecision)))
	}
	query := "SELECT id, service_id, escalation_policy_id, started_at, source_url FROM " +
		remaining.CurrentOperationalRowsSQL("operational_incidents", filters, contract)

	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read operational_incidents: %w", err)
	}
	defer rows.Close()

	var out []IncidentRow
	for rows.Next() {
		var (
			r         IncidentRow
			startedAt *time.Time
		)
		if err := rows.Scan(&r.ID, &r.ServiceID, &r.EscalationPolicyID, &startedAt, &r.SourceURL); err != nil {
			return nil, fmt.Errorf("scan operational_incidents row: %w", err)
		}
		r.StartedAt = startedAt
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operational_incidents: %w", err)
	}
	return out, nil
}

// ServiceRow is one operational_services row.
type ServiceRow struct {
	ID                 string
	OwningTeamID       string
	EscalationPolicyID string
}

// ReadServices ports operational_edges.py's `services` read.
func ReadServices(ctx context.Context, conn driver.Conn, organizationID string) ([]ServiceRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("service ordering contract: %w", err)
	}
	query := "SELECT id, owning_team_id, escalation_policy_id FROM " +
		remaining.CurrentOperationalRowsSQL("operational_services", []string{"is_deleted = 0"}, contract)
	rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", organizationID))
	if err != nil {
		return nil, fmt.Errorf("read operational_services: %w", err)
	}
	defer rows.Close()

	var out []ServiceRow
	for rows.Next() {
		var r ServiceRow
		if err := rows.Scan(&r.ID, &r.OwningTeamID, &r.EscalationPolicyID); err != nil {
			return nil, fmt.Errorf("scan operational_services row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operational_services: %w", err)
	}
	return out, nil
}

// directRow is the common shape of alerts/timeline/responders: an incident_id
// FK, an id, evidence, and an event timestamp candidate.
type directRow struct {
	ID         string
	IncidentID string
	SourceURL  string
	EventAt    *time.Time
	// ActorID (timeline) / UserID (responders) -- whichever this row's table
	// carries, empty for alerts.
	PersonID string
	// Body -- timeline/notes only, empty for alerts/responders.
	Body string
}

// incidentIDFilter returns a post-selection filter restricting a table to a
// known set of incident_id values, pushing the equality join against
// operational_incidents into ClickHouse instead of loading every row for the
// org and filtering in Go memory (chris's standing rule: a plain-equality
// join on org-scoped tables belongs in the database, not Go). Returns ("",
// false) when incidentIDs is empty -- the caller skips the query entirely in
// that case, since no row could possibly match.
func incidentIDFilter(incidentIDs []string) (filter string, args []any, ok bool) {
	if len(incidentIDs) == 0 {
		return "", nil, false
	}
	return "incident_id IN {incident_ids:Array(String)}", []any{clickhouse.Named("incident_ids", incidentIDs)}, true
}

// ReadAlerts ports operational_edges.py's `alerts` read, restricted to
// incidentIDs (see incidentIDFilter) -- Python loads every alert for the org
// and filters to known incidents in memory; this pushes that filter into the
// query instead.
func ReadAlerts(ctx context.Context, conn driver.Conn, organizationID string, incidentIDs []string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	filter, filterArgs, ok := incidentIDFilter(incidentIDs)
	if !ok {
		return nil, nil
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("alert ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, source_url, triggered_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_alerts", []string{"is_deleted = 0", filter}, contract)
	args := append([]any{clickhouse.Named("org_id", organizationID)}, filterArgs...)
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read operational_alerts: %w", err)
	}
	defer rows.Close()

	var out []directRow
	for rows.Next() {
		var (
			r           directRow
			triggeredAt *time.Time
		)
		if err := rows.Scan(&r.ID, &r.IncidentID, &r.SourceURL, &triggeredAt); err != nil {
			return nil, fmt.Errorf("scan operational_alerts row: %w", err)
		}
		r.EventAt = triggeredAt
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operational_alerts: %w", err)
	}
	return out, nil
}

// ReadTimelineEvents ports operational_edges.py's `timeline` read -- NO
// is_deleted filter, matching Python exactly (current_operational_rows_sql
// called with no post_selection_filters for this table), restricted to
// incidentIDs (see incidentIDFilter / ReadAlerts' doc comment).
func ReadTimelineEvents(ctx context.Context, conn driver.Conn, organizationID string, incidentIDs []string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	filter, filterArgs, ok := incidentIDFilter(incidentIDs)
	if !ok {
		return nil, nil
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("timeline ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, actor_id, body, source_url, occurred_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_incident_timeline_events", []string{filter}, contract)
	args := append([]any{clickhouse.Named("org_id", organizationID)}, filterArgs...)
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read operational_incident_timeline_events: %w", err)
	}
	defer rows.Close()

	var out []directRow
	for rows.Next() {
		var (
			r          directRow
			occurredAt *time.Time
		)
		if err := rows.Scan(&r.ID, &r.IncidentID, &r.PersonID, &r.Body, &r.SourceURL, &occurredAt); err != nil {
			return nil, fmt.Errorf("scan operational_incident_timeline_events row: %w", err)
		}
		r.EventAt = occurredAt
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operational_incident_timeline_events: %w", err)
	}
	return out, nil
}

// ReadNotes ports operational_edges.py's `notes` read -- NO is_deleted
// filter, same as timeline -- restricted to incidentIDs.
func ReadNotes(ctx context.Context, conn driver.Conn, organizationID string, incidentIDs []string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	filter, filterArgs, ok := incidentIDFilter(incidentIDs)
	if !ok {
		return nil, nil
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("note ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, body, author_user_id, source_url, created_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_incident_notes", []string{filter}, contract)
	args := append([]any{clickhouse.Named("org_id", organizationID)}, filterArgs...)
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read operational_incident_notes: %w", err)
	}
	defer rows.Close()

	var out []directRow
	for rows.Next() {
		var (
			r         directRow
			createdAt *time.Time
		)
		// Python maps this table's columns onto the SAME generic
		// (incident_id, body, source_url, event) shape as timeline via
		// [*timeline, *notes] below -- author_user_id is read here but never
		// used (notes never feed _append_user, only timeline does).
		var authorUserID string
		if err := rows.Scan(&r.ID, &r.IncidentID, &r.Body, &authorUserID, &r.SourceURL, &createdAt); err != nil {
			return nil, fmt.Errorf("scan operational_incident_notes row: %w", err)
		}
		r.EventAt = createdAt
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operational_incident_notes: %w", err)
	}
	return out, nil
}

// ReadResponders ports operational_edges.py's `responders` read -- NO
// is_deleted filter, same as timeline/notes -- restricted to incidentIDs.
func ReadResponders(ctx context.Context, conn driver.Conn, organizationID string, incidentIDs []string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	filter, filterArgs, ok := incidentIDFilter(incidentIDs)
	if !ok {
		return nil, nil
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("responder ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, user_id, source_url, assigned_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_incident_responders", []string{filter}, contract)
	args := append([]any{clickhouse.Named("org_id", organizationID)}, filterArgs...)
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read operational_incident_responders: %w", err)
	}
	defer rows.Close()

	var out []directRow
	for rows.Next() {
		var (
			r          directRow
			assignedAt *time.Time
		)
		if err := rows.Scan(&r.ID, &r.IncidentID, &r.PersonID, &r.SourceURL, &assignedAt); err != nil {
			return nil, fmt.Errorf("scan operational_incident_responders row: %w", err)
		}
		r.EventAt = assignedAt
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operational_incident_responders: %w", err)
	}
	return out, nil
}

// RepoRow is one repos row: id + full "owner/repo" name, as needed to match
// github.com/owner/repo/pull/N URLs found in incident evidence text.
type RepoRow struct {
	ID   uuid.UUID
	Repo string
}

// ReadRepos ports operational_edges.py's `repos` read: plain FINAL, not part
// of the operational_current family (repos is not an operational_* table).
// Python loads every repo for the org; this restricts to candidateFullNames
// (the owner/repo strings actually found in incident evidence text) instead
// of materializing the whole org's repo list for a lookup that only ever
// resolves a handful of names -- same equality-join-to-SQL rule as the
// incident-scoped reads above. Returns (nil, nil) for an empty candidate set.
func ReadRepos(ctx context.Context, conn driver.Conn, organizationID string, candidateFullNames []string) ([]RepoRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	if len(candidateFullNames) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx,
		"SELECT id, repo FROM repos FINAL WHERE org_id = {org_id:String} AND repo IN {repos:Array(String)}",
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("repos", candidateFullNames))
	if err != nil {
		return nil, fmt.Errorf("read repos: %w", err)
	}
	defer rows.Close()

	var out []RepoRow
	for rows.Next() {
		var (
			id   uuid.UUID
			repo string
		)
		if err := rows.Scan(&id, &repo); err != nil {
			return nil, fmt.Errorf("scan repos row: %w", err)
		}
		out = append(out, RepoRow{ID: id, Repo: repo})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate repos: %w", err)
	}
	return out, nil
}

// DeploymentRow is one deployments row.
type DeploymentRow struct {
	RepoID       uuid.UUID
	DeploymentID string
	Environment  string
	DeployedAt   time.Time
}

// ReadDeployments ports operational_edges.py's `deployments` read: plain
// FINAL, org-scoped, optionally repo- and window-scoped, ADDITIONALLY
// restricted to mappedRepoIDs (the repos actually reachable through a
// service-repository mapping). Python loads every deployment matching the
// org/repoID/window scope and then only ever looks one up per mapped repo
// (`for mapped_repo_id in service_repos... for deployment in deployments`) --
// a deployment for a repo with no mapping can never be joined to anything,
// so restricting to mappedRepoIDs removes only rows that would be discarded
// in Go memory anyway (same equality-join-to-SQL rule as the incident-scoped
// reads above). Returns (nil, nil) when mappedRepoIDs is empty -- no mapping
// means no possible linked_incident edge, so the query is skipped entirely.
func ReadDeployments(
	ctx context.Context, conn driver.Conn, organizationID string,
	repoID *uuid.UUID, mappedRepoIDs []uuid.UUID, fromDate, toDate *time.Time,
) ([]DeploymentRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	if len(mappedRepoIDs) == 0 {
		return nil, nil
	}
	query := "SELECT repo_id, deployment_id, environment, deployed_at FROM deployments FINAL " +
		"WHERE org_id = {org_id:String}"
	mappedRepoIDStrings := make([]string, len(mappedRepoIDs))
	for i, id := range mappedRepoIDs {
		mappedRepoIDStrings[i] = id.String()
	}
	args := []any{
		clickhouse.Named("org_id", organizationID),
	}
	query += " AND repo_id IN {mapped_repo_ids:Array(UUID)}"
	args = append(args, clickhouse.Named("mapped_repo_ids", mappedRepoIDStrings))
	if repoID != nil {
		query += " AND repo_id = {repo_id:UUID}"
		args = append(args, clickhouse.Named("repo_id", repoID.String()))
	}
	if fromDate != nil {
		query += " AND deployed_at >= {from_date:DateTime64(3, 'UTC')}"
		args = append(args, clickhouse.Named("from_date", remaining.DateTime64Argument(*fromDate, remaining.DateTime64MillisecondPrecision)))
	}
	if toDate != nil {
		query += " AND deployed_at <= {to_date:DateTime64(3, 'UTC')}"
		args = append(args, clickhouse.Named("to_date", remaining.DateTime64Argument(*toDate, remaining.DateTime64MillisecondPrecision)))
	}
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read deployments: %w", err)
	}
	defer rows.Close()

	var out []DeploymentRow
	for rows.Next() {
		var r DeploymentRow
		if err := rows.Scan(&r.RepoID, &r.DeploymentID, &r.Environment, &r.DeployedAt); err != nil {
			return nil, fmt.Errorf("scan deployments row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate deployments: %w", err)
	}
	return out, nil
}

// ReadWorkItemIDs checks which of candidateWorkItemIDs (the "jira:<key>"
// strings derived from jira-key matches in incident evidence text) actually
// exist, as the known-work-items set operational_edges.py checks jira-key
// matches against. Python loads EVERY work_item_id for the org (thousands of
// rows on a live org) just to build a membership set for a handful of
// extracted keys; this restricts the read to those candidates instead --
// same equality-join-to-SQL rule as the incident-scoped reads above. Returns
// (nil, nil) for an empty candidate set.
func ReadWorkItemIDs(ctx context.Context, conn driver.Conn, organizationID string, candidateWorkItemIDs []string) (map[string]bool, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	if len(candidateWorkItemIDs) == 0 {
		return nil, nil
	}
	rows, err := conn.Query(ctx,
		"SELECT work_item_id FROM work_items FINAL WHERE org_id = {org_id:String} "+
			"AND work_item_id IN {work_item_ids:Array(String)}",
		clickhouse.Named("org_id", organizationID),
		clickhouse.Named("work_item_ids", candidateWorkItemIDs))
	if err != nil {
		return nil, fmt.Errorf("read work_items: %w", err)
	}
	defer rows.Close()

	out := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan work_items row: %w", err)
		}
		out[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_items: %w", err)
	}
	return out, nil
}

// incidentEdgeBuilder holds the in-memory join state, mirroring
// build_operational_incident_edges' local variables. Kept as a struct rather
// than a long parameter list threaded through free functions.
type incidentEdgeBuilder struct {
	orgID               string
	now                 time.Time
	heuristicDaysWindow int
	heuristicConfidence float32
	repoScope           *uuid.UUID
	serviceRepos        map[string][]uuid.UUID
	serviceTeams        map[string]string
	servicePolicies     map[string]string
	incidentByID        map[string]IncidentRow
	knownWorkItems      map[string]bool
	repoIDsByFullName   map[string]uuid.UUID
	deploymentsByRepo   map[uuid.UUID][]DeploymentRow
	edges               []edges.Row
}

// defaultOperationalProvider mirrors operational_edges.py's `_edge()`
// helper, whose `provider: str = "pagerduty"` parameter default applies to
// EVERY call site in that file except the one for maps_to_repository (which
// passes the mapping row's own provider, itself "pagerduty" unless a future
// non-PagerDuty operational source is onboarded). A nil provider argument
// to this method means "use Python's default", not "no provider" -- this
// producer's edges are never provider-less.
var defaultOperationalProvider = "pagerduty"

func (b *incidentEdgeBuilder) edge(
	sourceType, sourceID, edgeType, targetType, targetID string,
	provenance string, confidence float32, evidence string,
	repoID *uuid.UUID, eventTs time.Time, provider *string,
) {
	if provider == nil {
		provider = &defaultOperationalProvider
	}
	id := edges.EdgeID(sourceType, sourceID, edgeType, targetType, targetID)
	b.edges = append(b.edges, edges.Row{
		EdgeID:       id,
		SourceType:   sourceType,
		SourceID:     sourceID,
		TargetType:   targetType,
		TargetID:     targetID,
		EdgeType:     edgeType,
		Provenance:   provenance,
		Confidence:   confidence,
		Evidence:     evidence,
		DiscoveredAt: b.now,
		LastSynced:   b.now,
		EventTs:      eventTs,
		Day:          edges.DayFor(eventTs),
		RepoID:       repoID,
		Provider:     provider,
	})
}

// BuildOperationalIncidentEdges ports operational_edges.py's
// build_operational_incident_edges end to end.
//
// discovered_at/last_synced are stamped from `now` for EVERY edge (the
// caller's batch clock), a DELIBERATE divergence from the deployed Python,
// which stamps both from WorkGraphEdge's per-instance datetime.now() default
// (operational_edges.py's _edge() never passes either field explicitly). True
// per-edge wall-clock cannot be golden-tested and is not a meaningful
// distinction Python intended -- it is a dataclass-default artifact, not a
// designed property -- so this port uses the SAME batch-clock convention
// edges/edges.go documents for the sibling _build_issue_issue_edges producer,
// rather than reproducing non-determinism nothing depends on.
func BuildOperationalIncidentEdges(
	ctx context.Context, conn driver.Conn, organizationID string, now time.Time,
	heuristicDaysWindow int, heuristicConfidence float32,
	fromDate, toDate *time.Time, repoID *uuid.UUID,
) ([]edges.Row, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}

	// Reads below are ordered so every equality join against an org-scoped
	// table is pushed into ClickHouse via a candidate-set WHERE/IN, rather
	// than loading a whole table and joining in Go memory (chris's standing
	// rule). That means the candidate set has to exist BEFORE the dependent
	// read: mappings/incidents/services first (nothing depends on them),
	// then incident-scoped tables filtered by the resulting incident id set,
	// then work_items/repos filtered by the jira-key/PR-URL candidates found
	// in THOSE tables' text. Measured row bounds on org 70d529e0 (2026-09-05,
	// disclosed in the PR body): mappings 0, incidents 1, services 1, work_items
	// 4944 (why the candidate-filtered read matters), deployments 699, repos 11.
	mappings, err := ReadServiceRepositoryMappings(ctx, conn, organizationID, now, repoID)
	if err != nil {
		return nil, err
	}
	incidents, err := ReadIncidents(ctx, conn, organizationID, fromDate, toDate)
	if err != nil {
		return nil, err
	}
	services, err := ReadServices(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}

	b := &incidentEdgeBuilder{
		orgID: organizationID, now: now,
		heuristicDaysWindow: heuristicDaysWindow, heuristicConfidence: heuristicConfidence,
		repoScope: repoID,
	}

	// preferred_mappings: highest-confidence mapping per (service_id, repo_id).
	type mappingKey struct {
		serviceID string
		repoID    uuid.UUID
	}
	preferred := make(map[mappingKey]MappingRow)
	for _, m := range mappings {
		if m.RepoID == nil || m.ServiceID == "" {
			continue
		}
		key := mappingKey{serviceID: m.ServiceID, repoID: *m.RepoID}
		if current, ok := preferred[key]; !ok || mappingConfidence(m.RelationshipConfidence) > mappingConfidence(current.RelationshipConfidence) {
			preferred[key] = m
		}
	}

	b.serviceRepos = make(map[string][]uuid.UUID)
	for key, m := range preferred {
		b.serviceRepos[key.serviceID] = append(b.serviceRepos[key.serviceID], key.repoID)
		confidence := mappingConfidence(m.RelationshipConfidence)
		evidence := strings.Join([]string{m.RelationshipProvenance, m.MappingKind, m.RuleID, m.SourceURL}, ":")
		provider := m.Provider
		if provider == "" {
			provider = "pagerduty"
		}
		b.edge(
			edges.NodeTypeOperationalService, key.serviceID,
			edges.EdgeTypeMapsToRepository,
			edges.NodeTypeRepository, key.repoID.String(),
			mappingProvenance(m.RelationshipProvenance), confidence, evidence,
			&key.repoID, b.now, &provider,
		)
	}

	b.serviceTeams = make(map[string]string)
	b.servicePolicies = make(map[string]string)
	for _, s := range services {
		if s.OwningTeamID != "" {
			b.serviceTeams[s.ID] = s.OwningTeamID
		}
		if s.EscalationPolicyID != "" {
			b.servicePolicies[s.ID] = s.EscalationPolicyID
		}
	}

	b.incidentByID = make(map[string]IncidentRow, len(incidents))
	for _, inc := range incidents {
		if repoID != nil {
			// Python: `if repo_id is None or str(row.get("service_id") or "") in service_repos`
			if _, ok := b.serviceRepos[inc.ServiceID]; !ok {
				continue
			}
		}
		b.incidentByID[inc.ID] = inc
	}

	incidentIDs := make([]string, 0, len(b.incidentByID))
	for id := range b.incidentByID {
		incidentIDs = append(incidentIDs, id)
	}

	alerts, err := ReadAlerts(ctx, conn, organizationID, incidentIDs)
	if err != nil {
		return nil, err
	}
	timeline, err := ReadTimelineEvents(ctx, conn, organizationID, incidentIDs)
	if err != nil {
		return nil, err
	}
	notes, err := ReadNotes(ctx, conn, organizationID, incidentIDs)
	if err != nil {
		return nil, err
	}
	responders, err := ReadResponders(ctx, conn, organizationID, incidentIDs)
	if err != nil {
		return nil, err
	}

	mappedRepoIDSeen := make(map[uuid.UUID]bool)
	var mappedRepoIDs []uuid.UUID
	for _, repos := range b.serviceRepos {
		for _, id := range repos {
			if !mappedRepoIDSeen[id] {
				mappedRepoIDSeen[id] = true
				mappedRepoIDs = append(mappedRepoIDs, id)
			}
		}
	}
	deployments, err := ReadDeployments(ctx, conn, organizationID, repoID, mappedRepoIDs, fromDate, toDate)
	if err != nil {
		return nil, err
	}

	b.deploymentsByRepo = make(map[uuid.UUID][]DeploymentRow, len(deployments))
	for _, d := range deployments {
		b.deploymentsByRepo[d.RepoID] = append(b.deploymentsByRepo[d.RepoID], d)
	}

	for incidentID, inc := range b.incidentByID {
		eventAt := b.now
		if inc.StartedAt != nil {
			eventAt = *inc.StartedAt
		}
		if inc.ServiceID != "" {
			b.edge(
				edges.NodeTypeOperationalService, inc.ServiceID,
				edges.EdgeTypeHasIncident,
				edges.NodeTypeIncident, incidentID,
				edges.ProvenanceNative, 1.0, sourceURLOr(inc.SourceURL, "operational_incident.service_id"),
				nil, eventAt, nil,
			)
		}
		policyID := inc.EscalationPolicyID
		if policyID == "" {
			policyID = b.servicePolicies[inc.ServiceID]
		}
		if policyID != "" {
			b.edge(
				edges.NodeTypeIncident, incidentID,
				edges.EdgeTypeEscalatesWith,
				edges.NodeTypeEscalationPolicy, policyID,
				edges.ProvenanceNative, 1.0, "operational_incident.escalation_policy_id",
				nil, eventAt, nil,
			)
		}
		if teamID := b.serviceTeams[inc.ServiceID]; teamID != "" {
			b.edge(
				edges.NodeTypeIncident, incidentID,
				edges.EdgeTypeAssignedTo,
				edges.NodeTypeTeam, teamID,
				edges.ProvenanceNative, 1.0, "operational_service.owning_team_id",
				nil, eventAt, nil,
			)
		}
		if inc.StartedAt != nil {
			for _, mappedRepoID := range b.serviceRepos[inc.ServiceID] {
				for _, d := range b.deploymentsByRepo[mappedRepoID] {
					environment := strings.ToLower(strings.TrimSpace(d.Environment))
					if environment == "" || environment == "unknown" || environment == "unspecified" {
						continue
					}
					if d.DeployedAt.After(*inc.StartedAt) {
						continue
					}
					if inc.StartedAt.Sub(d.DeployedAt) > time.Duration(heuristicDaysWindow)*24*time.Hour {
						continue
					}
					evidence := fmt.Sprintf("rule:operational_service_mapped_deployment_window.v1;environment:%s", environment)
					mappedRepoID := mappedRepoID
					b.edge(
						edges.NodeTypeDeployment, d.DeploymentID,
						edges.EdgeTypeLinkedIncident,
						edges.NodeTypeIncident, incidentID,
						edges.ProvenanceHeuristic, heuristicConfidence, evidence,
						&mappedRepoID, d.DeployedAt, nil,
					)
				}
			}
		}
	}

	// alerts/timeline/notes/responders are already restricted to incidentIDs
	// at the SQL level (ReadAlerts et al.), so every row here is guaranteed
	// known -- no Go-side membership check needed or performed.
	for _, a := range alerts {
		b.appendDirect(a, edges.NodeTypeOperationalAlert, edges.EdgeTypeHasAlert)
	}
	for _, t := range timeline {
		b.appendDirect(t, edges.NodeTypeIncidentTimelineEvent, edges.EdgeTypeHasTimelineEvent)
		b.appendUser(t)
	}
	for _, r := range responders {
		b.appendDirect(r, edges.NodeTypeIncidentResponder, edges.EdgeTypeHasResponder)
		b.appendUser(r)
	}

	// Candidate sets for the work_items/repos reads: derived from the text
	// this build already loaded (timeline + notes bodies), not from a
	// separate unbounded scan -- see ReadWorkItemIDs/ReadRepos' doc comments.
	textRows := concatRows(timeline, notes)
	candidateWorkItemIDs := make([]string, 0)
	seenWorkItemCandidate := make(map[string]bool)
	candidateFullNames := make([]string, 0)
	seenFullNameCandidate := make(map[string]bool)
	for _, row := range textRows {
		for _, key := range jiraKeyMatches(row.Body) {
			workItemID := "jira:" + key
			if !seenWorkItemCandidate[workItemID] {
				seenWorkItemCandidate[workItemID] = true
				candidateWorkItemIDs = append(candidateWorkItemIDs, workItemID)
			}
		}
		for _, ref := range githubPRURLMatches(row.Body) {
			fullName := ref.Owner + "/" + ref.Repo
			if !seenFullNameCandidate[fullName] {
				seenFullNameCandidate[fullName] = true
				candidateFullNames = append(candidateFullNames, fullName)
			}
		}
	}

	knownWorkItems, err := ReadWorkItemIDs(ctx, conn, organizationID, candidateWorkItemIDs)
	if err != nil {
		return nil, err
	}
	repos, err := ReadRepos(ctx, conn, organizationID, candidateFullNames)
	if err != nil {
		return nil, err
	}

	b.knownWorkItems = knownWorkItems
	b.repoIDsByFullName = make(map[string]uuid.UUID, len(repos))
	for _, r := range repos {
		b.repoIDsByFullName[r.Repo] = r.ID
	}

	for _, row := range textRows {
		eventAt := b.now
		if row.EventAt != nil {
			eventAt = *row.EventAt
		}
		for _, key := range jiraKeyMatches(row.Body) {
			workItemID := "jira:" + key
			if !b.knownWorkItems[workItemID] {
				continue
			}
			edgeType := edges.EdgeTypeReferences
			if strings.Contains(pythonparity.Fold(row.Body), "remediat") {
				edgeType = edges.EdgeTypeRemediatedBy
			}
			b.edge(
				edges.NodeTypeIncident, row.IncidentID,
				edgeType,
				edges.NodeTypeIssue, workItemID,
				edges.ProvenanceExplicitText, 0.9, "incident_evidence:"+key,
				nil, eventAt, nil,
			)
		}
		for _, ref := range githubPRURLMatches(row.Body) {
			prRepoID, ok := b.repoIDsByFullName[ref.Owner+"/"+ref.Repo]
			if !ok {
				continue
			}
			if repoID != nil && prRepoID != *repoID {
				continue
			}
			number := parsePRNumber(ref.Number)
			prID := edges.GeneratePRID(prRepoID, number)
			prRepoIDCopy := prRepoID
			b.edge(
				edges.NodeTypeIncident, row.IncidentID,
				edges.EdgeTypeReferences,
				edges.NodeTypePR, prID,
				edges.ProvenanceExplicitText, 0.9,
				fmt.Sprintf("incident_evidence:https://github.com/%s/%s/pull/%s", ref.Owner, ref.Repo, ref.Number),
				&prRepoIDCopy, eventAt, nil,
			)
		}
	}

	return b.edges, nil
}

func sourceURLOr(sourceURL, fallback string) string {
	if sourceURL != "" {
		return sourceURL
	}
	return fallback
}

func (b *incidentEdgeBuilder) appendDirect(row directRow, targetType, edgeType string) {
	if row.IncidentID == "" || row.ID == "" {
		return
	}
	eventAt := b.now
	if row.EventAt != nil {
		eventAt = *row.EventAt
	}
	evidence := row.SourceURL
	if evidence == "" {
		evidence = edgeType
	}
	b.edge(
		edges.NodeTypeIncident, row.IncidentID,
		edgeType,
		targetType, row.ID,
		edges.ProvenanceNative, 1.0, evidence,
		nil, eventAt, nil,
	)
}

func (b *incidentEdgeBuilder) appendUser(row directRow) {
	if row.IncidentID == "" || row.PersonID == "" {
		return
	}
	eventAt := b.now
	if row.EventAt != nil {
		eventAt = *row.EventAt
	}
	b.edge(
		edges.NodeTypeIncident, row.IncidentID,
		edges.EdgeTypeAssignedTo,
		edges.NodeTypeUser, row.PersonID,
		edges.ProvenanceNative, 1.0, sourceURLOr(row.SourceURL, "assigned_at"),
		nil, eventAt, nil,
	)
}

func concatRows(a, b []directRow) []directRow {
	out := make([]directRow, 0, len(a)+len(b))
	out = append(out, a...)
	out = append(out, b...)
	return out
}

func parsePRNumber(raw string) int {
	n := 0
	for _, r := range raw {
		if r < '0' || r > '9' {
			return n
		}
		n = n*10 + int(r-'0')
	}
	return n
}
