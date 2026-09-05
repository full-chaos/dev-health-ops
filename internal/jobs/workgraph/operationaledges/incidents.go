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
		clickhouse.Named("now", now.UTC()),
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
		args = append(args, clickhouse.Named("from_date", fromDate.UTC()))
	}
	if toDate != nil {
		filters = append(filters, "started_at <= {to_date:DateTime64(3, 'UTC')}")
		args = append(args, clickhouse.Named("to_date", toDate.UTC()))
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

// ReadAlerts ports operational_edges.py's `alerts` read.
func ReadAlerts(ctx context.Context, conn driver.Conn, organizationID string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("alert ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, source_url, triggered_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_alerts", []string{"is_deleted = 0"}, contract)
	rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", organizationID))
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
// called with no post_selection_filters for this table).
func ReadTimelineEvents(ctx context.Context, conn driver.Conn, organizationID string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("timeline ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, actor_id, body, source_url, occurred_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_incident_timeline_events", nil, contract)
	rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", organizationID))
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
// filter, same as timeline.
func ReadNotes(ctx context.Context, conn driver.Conn, organizationID string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("note ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, body, author_user_id, source_url, created_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_incident_notes", nil, contract)
	rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", organizationID))
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
// is_deleted filter, same as timeline/notes.
func ReadResponders(ctx context.Context, conn driver.Conn, organizationID string) ([]directRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	contract, err := remaining.ConfiguredOperationalOrderingContract()
	if err != nil {
		return nil, fmt.Errorf("responder ordering contract: %w", err)
	}
	query := "SELECT id, incident_id, user_id, source_url, assigned_at FROM " +
		remaining.CurrentOperationalRowsSQL("operational_incident_responders", nil, contract)
	rows, err := conn.Query(ctx, query, clickhouse.Named("org_id", organizationID))
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
func ReadRepos(ctx context.Context, conn driver.Conn, organizationID string) ([]RepoRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx,
		"SELECT id, repo FROM repos FINAL WHERE org_id = {org_id:String}",
		clickhouse.Named("org_id", organizationID))
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
// FINAL, org-scoped, optionally repo- and window-scoped.
func ReadDeployments(
	ctx context.Context, conn driver.Conn, organizationID string,
	repoID *uuid.UUID, fromDate, toDate *time.Time,
) ([]DeploymentRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	query := "SELECT repo_id, deployment_id, environment, deployed_at FROM deployments FINAL " +
		"WHERE org_id = {org_id:String}"
	args := []any{clickhouse.Named("org_id", organizationID)}
	if repoID != nil {
		query += " AND repo_id = {repo_id:UUID}"
		args = append(args, clickhouse.Named("repo_id", repoID.String()))
	}
	if fromDate != nil {
		query += " AND deployed_at >= {from_date:DateTime64(3, 'UTC')}"
		args = append(args, clickhouse.Named("from_date", fromDate.UTC()))
	}
	if toDate != nil {
		query += " AND deployed_at <= {to_date:DateTime64(3, 'UTC')}"
		args = append(args, clickhouse.Named("to_date", toDate.UTC()))
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

// WorkItemIDRow is a bare work_item_id, as read for the known-work-items set
// operational_edges.py checks jira-key matches against.
func ReadWorkItemIDs(ctx context.Context, conn driver.Conn, organizationID string) (map[string]bool, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx,
		"SELECT work_item_id FROM work_items FINAL WHERE org_id = {org_id:String}",
		clickhouse.Named("org_id", organizationID))
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

func (b *incidentEdgeBuilder) edge(
	sourceType, sourceID, edgeType, targetType, targetID string,
	provenance string, confidence float32, evidence string,
	repoID *uuid.UUID, eventTs time.Time, provider *string,
) {
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
	alerts, err := ReadAlerts(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	timeline, err := ReadTimelineEvents(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	notes, err := ReadNotes(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	responders, err := ReadResponders(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	knownWorkItems, err := ReadWorkItemIDs(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	repos, err := ReadRepos(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	deployments, err := ReadDeployments(ctx, conn, organizationID, repoID, fromDate, toDate)
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

	for _, a := range alerts {
		if !a.hasKnownIncident(b.incidentByID) {
			continue
		}
		b.appendDirect(a, edges.NodeTypeOperationalAlert, edges.EdgeTypeHasAlert)
	}
	for _, t := range timeline {
		if !t.hasKnownIncident(b.incidentByID) {
			continue
		}
		b.appendDirect(t, edges.NodeTypeIncidentTimelineEvent, edges.EdgeTypeHasTimelineEvent)
		b.appendUser(t)
	}
	for _, r := range responders {
		if !r.hasKnownIncident(b.incidentByID) {
			continue
		}
		b.appendDirect(r, edges.NodeTypeIncidentResponder, edges.EdgeTypeHasResponder)
		b.appendUser(r)
	}

	b.knownWorkItems = knownWorkItems
	b.repoIDsByFullName = make(map[string]uuid.UUID, len(repos))
	for _, r := range repos {
		b.repoIDsByFullName[r.Repo] = r.ID
	}

	for _, row := range concatRows(timeline, notes) {
		if !row.hasKnownIncident(b.incidentByID) {
			continue
		}
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

func (r directRow) hasKnownIncident(known map[string]IncidentRow) bool {
	_, ok := known[r.IncidentID]
	return ok
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
