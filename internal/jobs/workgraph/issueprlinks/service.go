package issueprlinks

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Observer receives the outcome of one mapping production. It is the CHAOS-4757
// telemetry surface, deliberately shaped like
// jobruntime.TeamRepoOwnershipDerivationObserver (its sibling Go-native
// derivation) so the two report through the same mechanism.
//
// A nil Observer is legal -- the structured log line is emitted either way, so
// the outcome is never invisible.
type Observer interface {
	ObserveIssuePRLinks(Outcome)
}

// Outcome is one run's full accounting.
//
// The five-way rejection breakdown is not decoration: the live proof asserts
// DependenciesRead == Written + RejectedTotal, so a gate added without a
// counter turns into a failing test rather than a silently shrinking output.
// Without it, "we wrote 2476 rows" is a number with no denominator, and the
// 2817-candidate/2476-written gap in the proof org would be unexplained.
type Outcome struct {
	OrganizationID    string
	DependenciesRead  int
	AdmittedByRawKind map[string]int
	Written           int
	Rejected          map[RejectionReason]int
	// ReservedSeenByRawKind counts rows whose raw kind is recognised but held
	// in ReservedAdmissions -- the signal that activating one would now do
	// something. See ReservedAdmissions for the sequencing this protects.
	ReservedSeenByRawKind map[string]int
	Duration              time.Duration
	// Balanced is false when the read count does not equal written plus
	// rejected -- an accounting bug in this package, not a data condition.
	Balanced bool
}

// Service loads, derives and writes the provider-attached mapping for one org.
type Service struct {
	loader   *Loader
	writer   *Writer
	logger   *slog.Logger
	observer Observer
	now      func() time.Time
}

// NewService wires the production path. A nil logger falls back to
// slog.Default() so a run's outcome is never swallowed.
func NewService(loader *Loader, writer *Writer, logger *slog.Logger) (*Service, error) {
	if loader == nil || writer == nil {
		return nil, ErrUnavailable
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{loader: loader, writer: writer, logger: logger, now: time.Now}, nil
}

// SetObserver wires the optional counter sink.
func (service *Service) SetObserver(observer Observer) {
	if service == nil {
		return
	}
	service.observer = observer
}

// SetClock overrides the fallback clock. Test-only seam: the clock is reached
// only for a dependency row whose last_synced is the zero value, which the
// live table cannot produce (the column is non-nullable and every writer stamps
// it), so production behaviour does not depend on it.
func (service *Service) SetClock(now func() time.Time) {
	if service == nil || now == nil {
		return
	}
	service.now = now
}

// Produce runs the whole mapping production for one org and window.
//
// It is the ONLY write-capable implementation of `work_graph_issue_pr`'s
// `native` rows once the Python producer is retired. It does not delete: the
// table is a ReplacingMergeTree keyed on
// (org_id, repo_id, work_item_id, pr_number), so re-running replaces each row
// in place, exactly as the Python producer did. A link the provider has since
// removed is therefore NOT retracted by either plane -- a pre-existing property
// of this table, not something this port introduces, and out of scope here.
func (service *Service) Produce(ctx context.Context, orgID string, window Window) (Outcome, error) {
	if service == nil || service.loader == nil || service.writer == nil {
		return Outcome{}, ErrUnavailable
	}
	started := service.now()

	inputs, err := service.loader.Load(ctx, orgID, window)
	if err != nil {
		return Outcome{OrganizationID: orgID}, err
	}
	result := Derive(inputs)

	// A dependency row with a zero last_synced cannot come from the live table,
	// but a fixture or a future writer could produce one, and a zero timestamp
	// in a ReplacingMergeTree's version column would make that row permanently
	// lose every merge. Python coerces the same case to "now"
	// (builder.py:768-785); do it here rather than writing 1970.
	fallback := service.now().UTC()
	for index := range result.Links {
		if result.Links[index].LastSynced.IsZero() {
			result.Links[index].LastSynced = fallback
		}
	}

	if err := service.writer.Write(ctx, result.Links); err != nil {
		return Outcome{OrganizationID: orgID}, fmt.Errorf("write issue-pr links: %w", err)
	}

	outcome := Outcome{
		OrganizationID:        orgID,
		DependenciesRead:      result.DependenciesRead,
		AdmittedByRawKind:     result.AdmittedByRawKind,
		Written:               result.Written(),
		Rejected:              result.Rejected,
		ReservedSeenByRawKind: result.ReservedSeenByRawKind,
		Duration:              service.now().Sub(started),
		Balanced:              result.Balanced(),
	}
	service.observe(outcome)
	return outcome, nil
}

func (service *Service) observe(outcome Outcome) {
	attributes := []any{
		slog.String("organization_id", outcome.OrganizationID),
		slog.Int("dependencies_read", outcome.DependenciesRead),
		slog.Int("rows_written", outcome.Written),
		slog.Bool("accounting_balanced", outcome.Balanced),
		slog.Duration("duration", outcome.Duration),
	}
	// Fixed order, from AllRejectionReasons -- ranging a map here would make
	// the log line's field order vary run to run and defeat diffing.
	for _, reason := range AllRejectionReasons {
		attributes = append(attributes, slog.Int("rejected_"+string(reason), outcome.Rejected[reason]))
	}
	for _, admission := range DefaultAdmissions {
		attributes = append(attributes, slog.Int(
			"admitted_"+admission.RelationshipTypeRaw,
			outcome.AdmittedByRawKind[admission.RelationshipTypeRaw],
		))
	}
	for _, reserved := range ReservedAdmissions {
		attributes = append(attributes, slog.Int(
			"reserved_seen_"+reserved.RelationshipTypeRaw,
			outcome.ReservedSeenByRawKind[reserved.RelationshipTypeRaw],
		))
	}

	if !outcome.Balanced {
		// Loud, because it means this package's own accounting is wrong: a
		// dependency row was neither written nor counted as rejected.
		service.logger.Error("issue-pr link accounting does not balance", attributes...)
	} else {
		service.logger.Info("issue-pr links produced", attributes...)
	}
	if service.observer != nil {
		service.observer.ObserveIssuePRLinks(outcome)
	}
}
