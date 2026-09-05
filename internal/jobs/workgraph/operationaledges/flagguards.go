// Package operationaledges is the Go port of builder.py's operational-edges
// slices (CHAOS-4924): _build_flag_guards_edges, _build_operational_incident_edges
// and operational_edges.py. It is a sibling of internal/jobs/workgraph/edges
// (CHAOS-4766, the issue-issue edge producer) rather than an extension of it,
// but reuses edges.Row/WriteEdges/EdgeID/GeneratePRID/GenerateFeatureFlagID and
// the shared NodeType/EdgeType constants so work_graph_edges has exactly one
// writer regardless of which sub-builder produced a row.
package operationaledges

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/edges"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/textrefs"
)

// flagTextRefConfidence mirrors builder.py's FLAG_TEXT_REF_CONFIDENCE.
const flagTextRefConfidence float32 = 0.6

// FlagRegistryRow is one feature_flag row, as read by ReadFeatureFlagRegistry.
type FlagRegistryRow struct {
	FlagKey    string
	Provider   string
	ProjectKey string
}

// ReadFeatureFlagRegistry ports the flag_query read in _build_flag_guards_edges
// (builder.py:541-544): the org's real flag registry, env-agnostic identity.
func ReadFeatureFlagRegistry(
	ctx context.Context, conn driver.Conn, organizationID string,
) ([]FlagRegistryRow, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx,
		`SELECT flag_key, provider, project_key FROM feature_flag FINAL WHERE org_id = {org_id:String}`,
		clickhouse.Named("org_id", organizationID))
	if err != nil {
		return nil, fmt.Errorf("read feature_flag: %w", err)
	}
	defer rows.Close()

	var out []FlagRegistryRow
	for rows.Next() {
		var r FlagRegistryRow
		if err := rows.Scan(&r.FlagKey, &r.Provider, &r.ProjectKey); err != nil {
			return nil, fmt.Errorf("scan feature_flag row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate feature_flag: %w", err)
	}
	return out, nil
}

// WorkItemText is one work_items row's title+description, as read by
// ReadWorkItemText.
type WorkItemText struct {
	WorkItemID  string
	Title       string
	Description string
}

// ReadWorkItemText ports the wi_query read in _build_flag_guards_edges
// (builder.py:568-571).
func ReadWorkItemText(
	ctx context.Context, conn driver.Conn, organizationID string,
) ([]WorkItemText, error) {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx,
		`SELECT work_item_id, title, description FROM work_items FINAL WHERE org_id = {org_id:String}`,
		clickhouse.Named("org_id", organizationID))
	if err != nil {
		return nil, fmt.Errorf("read work_items: %w", err)
	}
	defer rows.Close()

	var out []WorkItemText
	for rows.Next() {
		var r WorkItemText
		if err := rows.Scan(&r.WorkItemID, &r.Title, &r.Description); err != nil {
			return nil, fmt.Errorf("scan work_items row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_items: %w", err)
	}
	return out, nil
}

// FeatureFlagLinkRow is one feature_flag_link row
// (034_feature_flag_user_impact_tables.sql). evidence_type is a non-Nullable
// String column in the live schema even though Python's model types it
// `str | None` -- ci.py's writer coerces None to "" at the boundary
// (`r.evidence_type or ""`), so this type carries plain string, not *string.
type FeatureFlagLinkRow struct {
	OrgID        string
	FlagKey      string
	TargetType   string
	TargetID     string
	Provider     string
	LinkSource   string
	LinkType     string
	EvidenceType string
	Confidence   float32
	ValidFrom    *time.Time
	ValidTo      *time.Time
	LastSynced   time.Time
}

// WriteFeatureFlagLinks inserts into feature_flag_link. This is a SECOND
// table _build_flag_guards_edges writes to, alongside work_graph_edges
// (builder.py:635-636: `self.sink.write_feature_flag_links(links)`) -- easy to
// miss because the sub-builder's return value is only the edge count.
func WriteFeatureFlagLinks(
	ctx context.Context, conn driver.Conn, rows []FeatureFlagLinkRow,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO feature_flag_link ("+
			"org_id, flag_key, target_type, target_id, provider, "+
			"link_source, link_type, evidence_type, confidence, "+
			"valid_from, valid_to, last_synced)")
	if err != nil {
		return 0, fmt.Errorf("prepare feature_flag_link batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.FlagKey, row.TargetType, row.TargetID, row.Provider,
			row.LinkSource, row.LinkType, row.EvidenceType, row.Confidence,
			row.ValidFrom, row.ValidTo, row.LastSynced,
		); err != nil {
			return 0, fmt.Errorf("append feature_flag_link row (flag=%s target=%s): %w",
				row.FlagKey, row.TargetID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send feature_flag_link batch: %w", err)
	}
	return len(rows), nil
}

// flagIdentity is one (provider, project_key, flag_id) triple for a flag_key
// -- a key can in principle exist under more than one provider/project,
// mirroring flag_identities' list-valued dict in Python (builder.py:551-563).
type flagIdentity struct {
	Provider   string
	ProjectKey string
	FlagID     string
}

// BuildFlagGuardsEdges ports _build_flag_guards_edges (builder.py:527-642) end
// to end: reads the flag registry and work-item text, matches registry-known
// flag keys against issue title+description via textrefs.ExtractFlagKeyRefs,
// and returns the GUARDS edges plus the feature_flag_link rows -- both must be
// written (edges.WriteEdges, WriteFeatureFlagLinks) for parity; a caller that
// only writes the edges silently drops the link half.
//
// Unlike _build_operational_incident_edges, Python's flag-guards path has NO
// `if not org_id: return 0` guard of its own -- an unscoped run would read
// every tenant's registry and work items into one build. This port refuses
// instead, via investment.RequireOrganizationScope in the two readers above,
// the SAME deliberate divergence edges.Divergences documents for
// _build_issue_issue_edges (CHAOS-4441 RequireOrganizationScope) -- one
// scope guard, uniformly applied, not a second bespoke one.
func BuildFlagGuardsEdges(
	ctx context.Context, conn driver.Conn, organizationID string, now time.Time,
) ([]edges.Row, []FeatureFlagLinkRow, error) {
	flagRows, err := ReadFeatureFlagRegistry(ctx, conn, organizationID)
	if err != nil {
		return nil, nil, err
	}
	if len(flagRows) == 0 {
		return nil, nil, nil
	}

	identitiesByKey := make(map[string][]flagIdentity, len(flagRows))
	knownKeys := make([]string, 0, len(flagRows))
	for _, row := range flagRows {
		if row.FlagKey == "" {
			continue
		}
		if _, exists := identitiesByKey[row.FlagKey]; !exists {
			knownKeys = append(knownKeys, row.FlagKey)
		}
		flagID := edges.GenerateFeatureFlagID(organizationID, row.Provider, row.ProjectKey, row.FlagKey)
		identitiesByKey[row.FlagKey] = append(identitiesByKey[row.FlagKey],
			flagIdentity{Provider: row.Provider, ProjectKey: row.ProjectKey, FlagID: flagID})
	}

	workItems, err := ReadWorkItemText(ctx, conn, organizationID)
	if err != nil {
		return nil, nil, err
	}
	if len(workItems) == 0 {
		return nil, nil, nil
	}

	var edgeRows []edges.Row
	var linkRows []FeatureFlagLinkRow
	seenEdges := make(map[string]bool)

	for _, wi := range workItems {
		if wi.WorkItemID == "" {
			continue
		}
		text := strings.TrimSpace(wi.Title + " " + wi.Description)
		if text == "" {
			continue
		}
		for _, ref := range textrefs.ExtractFlagKeyRefs(text, knownKeys, textrefs.FlagKeyMinLength) {
			for _, identity := range identitiesByKey[ref.FlagKey] {
				edgeID := edges.EdgeID(
					edges.NodeTypeFeatureFlag, identity.FlagID,
					edges.EdgeTypeGuards,
					edges.NodeTypeIssue, wi.WorkItemID,
				)
				if seenEdges[edgeID] {
					continue
				}
				seenEdges[edgeID] = true

				var provider *string
				if identity.Provider != "" {
					p := identity.Provider
					provider = &p
				}

				edgeRows = append(edgeRows, edges.Row{
					EdgeID:       edgeID,
					SourceType:   edges.NodeTypeFeatureFlag,
					SourceID:     identity.FlagID,
					TargetType:   edges.NodeTypeIssue,
					TargetID:     wi.WorkItemID,
					EdgeType:     edges.EdgeTypeGuards,
					Provenance:   edges.ProvenanceExplicitText,
					Confidence:   flagTextRefConfidence,
					Evidence:     "flagref:" + ref.RawMatch,
					DiscoveredAt: now,
					LastSynced:   now,
					EventTs:      now,
					Day:          edges.DayFor(now),
					OrgID:        "", // never used: WriteEdges stamps the run's scope. See edges/scope.go.
					Provider:     provider,
				})
				linkRows = append(linkRows, FeatureFlagLinkRow{
					OrgID:        organizationID,
					FlagKey:      ref.FlagKey,
					TargetType:   "issue",
					TargetID:     wi.WorkItemID,
					Provider:     identity.Provider,
					LinkSource:   "explicit_text",
					LinkType:     "tracks",
					EvidenceType: "issue_text",
					Confidence:   flagTextRefConfidence,
					ValidFrom:    &now,
					ValidTo:      nil,
					LastSynced:   now,
				})
			}
		}
	}
	return edgeRows, linkRows, nil
}
