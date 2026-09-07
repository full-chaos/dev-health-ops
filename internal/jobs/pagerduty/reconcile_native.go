package pagerduty

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NativeReconciler replaces HTTPCompatibilityReconciler (CHAOS-4105). Where
// that type forwarded the payload to Python and interpreted a status string,
// this one performs the reconciliation: it locks the binding graph in
// Postgres, re-checks the canonical-incident entitlement, and writes the
// canonical rows through the same providersync effect sinks the native pull
// route already uses.
//
// The four PermanentError reasons are unchanged and still mean what a DLQ
// triager expects them to mean -- re-enable a feature, fix a binding, chase a
// producer bug -- because the terminal outcomes they name still exist; only
// the component that decides them moved from Python to here.
type NativeReconciler struct {
	pool        *pgxpool.Pool
	entitlement providersync.IncidentEntitlement
	sinks       func(providerInstanceID string, lease providerfoundation.LeaseGuard) providersync.PagerDutyWebhookSinks
	hydrator    func(graph LockedGraph, lease providerfoundation.LeaseGuard) providersync.PagerDutyIncidentHydrator
	receipts    ReceiptFence
	metrics     *providerfoundation.Metrics
	now         func() time.Time
}

// pagerDutyWebhookEntitlementDataset labels the refusal counter. The webhook
// is not a dataset sync, but the counter's dataset label is a bounded
// vocabulary shared with the pull route, and "incidents" is the dataset whose
// entitlement this actually is -- inventing a "webhook" label would mint a
// second series for the same policy decision (CHAOS-4219's one-series rule).
const pagerDutyWebhookEntitlementDataset = "incidents"

// ReceiptFence proves this process still owns the receipt. The effect sinks
// take a LeaseGuard and call it before every ClickHouse send; backing that
// guard with the receipt claim is what stops a reconciler whose lease was
// stolen mid-flight from writing rows the new holder is also writing.
type ReceiptFence interface {
	Assert(context.Context, ReceiptClaim) error
}

// NativeReconcilerConfig is the constructor's argument bag; every field is
// required, because a nil one is a wiring defect that must fail at startup
// rather than at the first delivery.
type NativeReconcilerConfig struct {
	Pool        *pgxpool.Pool
	Entitlement providersync.IncidentEntitlement
	Receipts    ReceiptFence
	// Sinks builds the effect writers for one provider instance, fenced by
	// the caller's lease. It is a function because neither the provider
	// instance nor the receipt lease exists until the delivery is in hand.
	Sinks func(providerInstanceID string, lease providerfoundation.LeaseGuard) providersync.PagerDutyWebhookSinks
	// Hydrator builds the REST client used only when a webhook payload is too
	// sparse to normalize, fenced by the same lease.
	Hydrator func(graph LockedGraph, lease providerfoundation.LeaseGuard) providersync.PagerDutyIncidentHydrator
	// Metrics carries the shared provider-foundation fragment the entitlement
	// refusal counter lands on. A nil pointer is safe (the counter is
	// nil-receiver guarded) but means the refusal is invisible to a scrape,
	// so it is required like every other dependency here.
	Metrics *providerfoundation.Metrics
}

func NewNativeReconciler(config NativeReconcilerConfig) (*NativeReconciler, error) {
	if config.Pool == nil || config.Entitlement == nil || config.Receipts == nil ||
		config.Sinks == nil || config.Hydrator == nil || config.Metrics == nil {
		return nil, errUnavailable
	}
	return &NativeReconciler{
		pool: config.Pool, entitlement: config.Entitlement, receipts: config.Receipts,
		sinks: config.Sinks, hydrator: config.Hydrator, metrics: config.Metrics,
		now: time.Now,
	}, nil
}

// LockedGraph is the authorized binding graph: the row set Python's
// lock_active_pagerduty_webhook_graph selected FOR UPDATE, having passed
// _trusted_graph's predicates. Holding it means this org, this source and
// this credential all agree that this binding may write.
type LockedGraph struct {
	BindingID          string
	OrgID              string
	IntegrationID      string
	SourceID           string
	CredentialID       string
	ProviderInstanceID string
}

// lockGraph ports lock_active_pagerduty_webhook_graph + _trusted_graph.
//
// Python took four separate SELECT ... FOR UPDATE statements and then applied
// the trust predicates in memory; this is one join with the predicates in the
// WHERE clause and a single FOR UPDATE over the joined rows. The lock set is
// the same four rows and the accepted set is the same: every predicate below
// is one of _trusted_graph's, in its order, and the provider_instance_id
// fallback (account_id, else subdomain) with the source.external_id equality
// check is preserved exactly -- it is the check that stops a renamed or
// re-pointed source from writing under another account's instance id.
func (reconciler *NativeReconciler) lockGraph(
	ctx context.Context, tx pgx.Tx, bindingID string,
) (LockedGraph, error) {
	var graph LockedGraph
	var accountID, subdomain *string
	err := tx.QueryRow(ctx, `
SELECT b.id::text, s.org_id, i.id::text, s.id::text, b.credential_id::text,
       c.config->>'account_id', c.config->>'subdomain'
FROM public.pagerduty_webhook_bindings b
JOIN public.integration_sources s ON s.id = b.integration_source_id
JOIN public.integrations i ON i.id = s.integration_id
JOIN public.integration_credentials c ON c.id = b.credential_id
WHERE b.id = $1::uuid
  AND b.status = 'active'
  AND s.org_id = b.org_id::text
  AND i.org_id = s.org_id
  AND c.org_id = s.org_id
  AND s.provider = 'pagerduty'
  AND i.provider = 'pagerduty'
  AND c.provider = 'pagerduty'
  AND i.credential_id = b.credential_id
  AND s.is_enabled
  AND i.is_active
  AND c.is_active
FOR UPDATE OF b, s, i, c`, bindingID).Scan(
		&graph.BindingID, &graph.OrgID, &graph.IntegrationID, &graph.SourceID,
		&graph.CredentialID, &accountID, &subdomain,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return LockedGraph{}, errBindingRevoked
	}
	if err != nil {
		return LockedGraph{}, fmt.Errorf("lock pagerduty webhook graph: %w", err)
	}
	instance := ""
	if accountID != nil {
		instance = strings.TrimSpace(*accountID)
	}
	if instance == "" && subdomain != nil {
		instance = strings.TrimSpace(*subdomain)
	}
	if instance == "" {
		return LockedGraph{}, errBindingRevoked
	}
	var sourceExternalID string
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(external_id, '') FROM public.integration_sources WHERE id = $1::uuid`,
		graph.SourceID,
	).Scan(&sourceExternalID); err != nil {
		return LockedGraph{}, fmt.Errorf("read pagerduty source external id: %w", err)
	}
	if sourceExternalID != instance {
		// _trusted_graph's last predicate. A source whose external id no
		// longer matches the credential's account is not a malformed payload
		// and not a disabled feature -- the binding no longer authorizes this
		// instance, which is what "revoked" names.
		return LockedGraph{}, errBindingRevoked
	}
	graph.ProviderInstanceID = instance
	return graph, nil
}

// Terminal outcomes the dead-letter record must keep distinguishable. TRD
// section 10.5 requires feature-disabled, malformed, revoked-binding, and
// retry-exhausted to stay separable: an operator triaging the DLQ needs to
// know whether to re-enable a feature, fix a binding, or chase a producer
// bug, and a single collapsed reason answers none of those.
//
// These four moved here verbatim from compatibility.go, which CHAOS-4105
// deleted. reasonBridgeRejected went with it: there is no bridge left to
// reject anything, and keeping a reason nothing can emit would leave a
// permanently empty bucket in DLQ triage.
const (
	reasonFeatureDisabled = "pagerduty_feature_disabled"
	reasonBindingRevoked  = "pagerduty_binding_revoked"
	reasonSchemaInvalid   = "pagerduty_schema_invalid"
)

var errBindingRevoked = errors.New("pagerduty webhook binding no longer authorizes this instance")

// webhookEnvelope is the parsed v3 envelope. The stream entry carries the
// whole webhook body; only these fields drive reconciliation.
type webhookEnvelope struct {
	Event struct {
		ID         string          `json:"id"`
		EventType  string          `json:"event_type"`
		OccurredAt time.Time       `json:"occurred_at"`
		Data       json.RawMessage `json:"data"`
	} `json:"event"`
}

// Reconcile commits the canonical rows for one webhook, or returns a
// classified failure.
//
// The receipt claim is threaded through so the effect sinks' lease guard can
// be the receipt itself. HTTPCompatibilityReconciler did not need it -- the
// Python bridge owned its own transaction -- which is why the Reconciler seam
// grew the parameter with this port.
func (reconciler *NativeReconciler) Reconcile(
	ctx context.Context, event Event, receipt ReceiptClaim,
) error {
	if reconciler == nil || reconciler.pool == nil {
		// Safe on a nil receiver: this is a package function, not a method,
		// precisely because the receiver is what may be missing here.
		logTransient(ctx, event, "", errUnavailable)
		return errUnavailable
	}
	// These two returns quarantine a delivery permanently, and both ran
	// silently until codex r1 caught it: a dead-letter row appeared with no
	// line in the log explaining it, which is the one thing the reason codes
	// exist to make explainable. They fire BEFORE the graph lock, so there is
	// no org or provider instance to name yet -- the binding and receipt are
	// what an operator has to work with, and they are enough to find the
	// delivery.
	if event.BindingID == "" || len(event.Payload) == 0 {
		reconciler.logMalformed(ctx, event, "",
			errors.New("stream entry has no binding id or no payload"))
		return &streamrunner.PermanentError{Reason: reasonSchemaInvalid}
	}
	var envelope webhookEnvelope
	if err := json.Unmarshal(event.Payload, &envelope); err != nil ||
		strings.TrimSpace(envelope.Event.ID) == "" ||
		strings.TrimSpace(envelope.Event.EventType) == "" ||
		envelope.Event.OccurredAt.IsZero() || len(envelope.Event.Data) == 0 {
		reconciler.logMalformed(ctx, event, envelope.Event.EventType,
			errors.New("webhook envelope is missing id, event_type, occurred_at, or data"))
		return &streamrunner.PermanentError{Reason: reasonSchemaInvalid}
	}
	started := reconciler.now()

	tx, err := reconciler.pool.Begin(ctx)
	if err != nil {
		logTransient(ctx, event, envelope.Event.EventType, err)
		return fmt.Errorf("begin pagerduty webhook transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	graph, err := reconciler.lockGraph(ctx, tx, event.BindingID)
	if err != nil {
		if errors.Is(err, errBindingRevoked) {
			reconciler.logRefusal(ctx, event, envelope, "", reasonBindingRevoked, err)
			return &streamrunner.PermanentError{Reason: reasonBindingRevoked}
		}
		reconciler.logFailure(ctx, event, envelope, "", err)
		return err
	}

	// The canonical-incident entitlement, re-checked at execution exactly as
	// Python's _canonical_incident_ingestion_allowed was. A disabled feature
	// is terminal, not retryable: replaying cannot change the answer, and the
	// DLQ reason is what tells an operator to re-enable rather than debug.
	if err := reconciler.entitlement.Require(ctx, graph.OrgID); err != nil {
		if errors.Is(err, providersync.ErrIncidentEntitlementDisabled) {
			// Count as well as log. providersync's own requireIncidentEntitlement
			// owns this counter for the sink seam, but this pre-sink check calls
			// Require directly and therefore bypasses it -- so without this line
			// dev_health_provider_incident_entitlement_refused_total reads zero
			// for every webhook a disabled org refuses, which is the most
			// trustworthy-looking way for a metric to be wrong (codex r1).
			reconciler.metrics.RecordIncidentEntitlementRefused(
				"pagerduty", pagerDutyWebhookEntitlementDataset,
				providersync.IncidentEntitlementSeamCollect,
			)
			reconciler.logRefusal(ctx, event, envelope, graph.OrgID, reasonFeatureDisabled, err)
			return &streamrunner.PermanentError{Reason: reasonFeatureDisabled}
		}
		// Unevaluable is infrastructure, not policy: keep it retryable so a
		// domain-Postgres blip cannot dead-letter a healthy delivery.
		reconciler.logFailure(ctx, event, envelope, graph.OrgID, err)
		return fmt.Errorf("evaluate pagerduty incident entitlement: %w", err)
	}

	lease := ReceiptLeaseGuard{Fence: reconciler.receipts, Receipt: receipt}
	claim, err := reconciler.claimFor(graph, event, receipt)
	if err != nil {
		reconciler.logFailure(ctx, event, envelope, graph.OrgID, err)
		return err
	}
	outcome, err := providersync.ReconcilePagerDutyWebhook(
		ctx, claim, graph.ProviderInstanceID,
		providersync.PagerDutyWebhookEvent{
			EventID:    envelope.Event.ID,
			EventType:  envelope.Event.EventType,
			OccurredAt: envelope.Event.OccurredAt,
			ReceivedAt: event.Received,
			Data:       envelope.Event.Data,
		},
		reconciler.sinks(graph.ProviderInstanceID, lease),
		reconciler.hydrator(graph, lease),
	)
	if err != nil {
		switch {
		case errors.Is(err, providersync.ErrPagerDutyWebhookUnsupported),
			errors.Is(err, providersync.ErrPagerDutyWebhookMalformed):
			reconciler.logRefusal(ctx, event, envelope, graph.OrgID, reasonSchemaInvalid, err)
			return &streamrunner.PermanentError{Reason: reasonSchemaInvalid}
		case errors.Is(err, providersync.ErrIncidentEntitlementDisabled):
			// The sinks re-check the same entitlement at the write boundary.
			// A disable committed between this handler's check and the write
			// lands here, and must classify as the policy refusal it is.
			reconciler.logRefusal(ctx, event, envelope, graph.OrgID, reasonFeatureDisabled, err)
			return &streamrunner.PermanentError{Reason: reasonFeatureDisabled}
		default:
			reconciler.logFailure(ctx, event, envelope, graph.OrgID, err)
			return fmt.Errorf("reconcile pagerduty webhook: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		// The ClickHouse rows are already committed; only the advisory row
		// locks are lost. Say so, rather than logging a bare commit error
		// that reads like the reconciliation failed.
		reconciler.logFailure(ctx, event, envelope, graph.OrgID,
			fmt.Errorf("release pagerduty binding locks after a committed write: %w", err))
		return fmt.Errorf("commit pagerduty webhook transaction: %w", err)
	}
	reconciler.logSuccess(ctx, event, envelope, graph, outcome, reconciler.now().Sub(started))
	return nil
}

// claimFor mints the synthetic sync claim the effect sinks validate against.
//
// The unit and run ids are UUIDv5 over the receipt id, so they are stable
// across redelivery of the same event: Claim.GenerationKey() is derived from
// the unit id, and a changing key would present each retry as a new
// generation. Every other id is the real one from the locked graph.
func (reconciler *NativeReconciler) claimFor(
	graph LockedGraph, event Event, receipt ReceiptClaim,
) (providersync.Claim, error) {
	owner, err := uuid.Parse(receipt.Token)
	if err != nil {
		// A claimed receipt always carries a uuid token; anything else means
		// the handler called Reconcile without ownership.
		return providersync.Claim{}, errUnavailable
	}
	unitID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("pagerduty-webhook-unit:"+event.ReceiptID))
	runID := uuid.NewSHA1(uuid.NameSpaceURL, []byte("pagerduty-webhook-run:"+event.ReceiptID))
	return providersync.Claim{
		Unit: providersync.Unit{
			ID: unitID.String(), SyncRunID: runID.String(), OrgID: graph.OrgID,
			IntegrationID: graph.IntegrationID, SourceID: graph.SourceID,
			SourceExternalID: graph.ProviderInstanceID, SourceName: graph.ProviderInstanceID,
			Provider: "pagerduty", Mode: "incremental",
			CredentialID: graph.CredentialID, AuthSource: "integration_credential",
		},
		Owner:          owner.String(),
		Attempt:        1,
		LeaseExpiresAt: reconciler.now().UTC().Add(ReceiptLease),
	}, nil
}

const (
	webhookMalformedEvent  = "pagerduty webhook rejected before the binding lock"
	webhookTransientEvent  = "pagerduty webhook failed before the binding lock"
	webhookReconciledEvent = "pagerduty webhook reconciled"
	webhookRefusedEvent    = "pagerduty webhook refused"
	webhookFailedEvent     = "pagerduty webhook failed"
)

// One Info line per reconciled webhook, carrying the identity an operator
// needs to find it again and the row counts per table. The raw payload never
// appears: stream.go's Event doc comment is the standing rule and this
// honours it.
func (reconciler *NativeReconciler) logSuccess(
	ctx context.Context, event Event, envelope webhookEnvelope,
	graph LockedGraph, outcome providersync.PagerDutyWebhookOutcome, elapsed time.Duration,
) {
	attributes := []any{
		slog.String("provider", "pagerduty"),
		slog.String("org_id", graph.OrgID),
		slog.String("binding_id", graph.BindingID),
		slog.String("provider_instance_id", graph.ProviderInstanceID),
		slog.String("event_type", envelope.Event.EventType),
		slog.String("event_id", envelope.Event.ID),
		slog.String("receipt_id", event.ReceiptID),
		slog.Int("rows", outcome.Rows()),
		slog.Bool("hydrated_via_rest", outcome.Hydrated),
		slog.Duration("elapsed", elapsed),
	}
	for _, write := range outcome.Writes {
		attributes = append(attributes, slog.Int("rows_"+write.Destination, write.Rows))
	}
	slog.InfoContext(ctx, webhookReconciledEvent, attributes...)
}

// logTransient explains a delivery that failed before the binding graph was
// locked and will be retried.
//
// streamrunner already surfaces the returned error (runner.go's process ->
// "stream runner cycle failed"), so the failure itself was never invisible.
// What was missing is the DELIVERY IDENTITY: an operator reading
// "begin pagerduty webhook transaction: ..." could not tell which binding or
// receipt it belonged to, which is the difference between a line you can act
// on and one you can only count. Reported by codex r2 as the last open
// finding on CHAOS-4105.
//
// It is a package function rather than a method because one of its two call
// sites is the nil-receiver guard itself.
func logTransient(ctx context.Context, event Event, eventType string, cause error) {
	slog.ErrorContext(ctx, webhookTransientEvent,
		slog.String("provider", "pagerduty"),
		slog.String("binding_id", event.BindingID),
		slog.String("event_type", eventType),
		slog.String("event_id", event.EventID),
		slog.String("receipt_id", event.ReceiptID),
		slog.String("error", cause.Error()),
	)
}

// logMalformed explains a delivery quarantined before the graph lock, where
// no org or provider instance is known yet. It is a WARN for the same reason
// logRefusal is: a producer sending malformed entries is an expected,
// operator-visible state, not an infrastructure fault.
func (reconciler *NativeReconciler) logMalformed(
	ctx context.Context, event Event, eventType string, cause error,
) {
	slog.WarnContext(ctx, webhookMalformedEvent,
		slog.String("provider", "pagerduty"),
		slog.String("binding_id", event.BindingID),
		slog.String("event_type", eventType),
		slog.String("receipt_id", event.ReceiptID),
		slog.String("reason", reasonSchemaInvalid),
		slog.String("error", cause.Error()),
	)
}

// A terminal refusal is a WARN: it is an expected operator- or
// producer-driven state, and it is the line that explains a dead-letter row.
func (reconciler *NativeReconciler) logRefusal(
	ctx context.Context, event Event, envelope webhookEnvelope,
	orgID, reason string, cause error,
) {
	slog.WarnContext(ctx, webhookRefusedEvent,
		slog.String("provider", "pagerduty"),
		slog.String("org_id", orgID),
		slog.String("binding_id", event.BindingID),
		slog.String("event_type", envelope.Event.EventType),
		slog.String("event_id", envelope.Event.ID),
		slog.String("receipt_id", event.ReceiptID),
		slog.String("reason", reason),
		slog.String("error", cause.Error()),
	)
}

// Every retryable failure gets its own line with the same identity, so a
// swallowed error is impossible on this path: no branch returns without
// either logging or being logged by its caller.
func (reconciler *NativeReconciler) logFailure(
	ctx context.Context, event Event, envelope webhookEnvelope, orgID string, cause error,
) {
	slog.ErrorContext(ctx, webhookFailedEvent,
		slog.String("provider", "pagerduty"),
		slog.String("org_id", orgID),
		slog.String("binding_id", event.BindingID),
		slog.String("event_type", envelope.Event.EventType),
		slog.String("event_id", envelope.Event.ID),
		slog.String("receipt_id", event.ReceiptID),
		slog.String("error", cause.Error()),
	)
}

// CredentialIncidentHydrator is the REST fallback for a webhook payload too
// sparse to normalize -- Python's PagerDutyClient.get_incident. It resolves
// the binding's own credential through the same resolver the pull route
// uses, so OAuth refresh, region selection and decryption are not
// reimplemented here.
type CredentialIncidentHydrator struct {
	Resolver     providerfoundation.CredentialResolver
	Doer         providerfoundation.HTTPDoer
	Retry        providerfoundation.RetryPolicy
	Lease        providerfoundation.LeaseGuard
	OrgID        string
	CredentialID string
}

const maxHydratedIncidentBytes = 1 << 20

func (hydrator CredentialIncidentHydrator) HydrateIncident(
	ctx context.Context, incidentID string,
) (json.RawMessage, error) {
	if strings.TrimSpace(incidentID) == "" {
		return nil, errors.New("pagerduty incident hydration needs an incident id")
	}
	// The fallback means the producer sent a payload thinner than the
	// contract promises, and it costs a synchronous API call inside the
	// webhook's own budget. Warn so a provider that starts doing it at volume
	// is visible before it shows up as latency.
	slog.WarnContext(ctx, "pagerduty webhook payload was too sparse, hydrating over REST",
		slog.String("provider", "pagerduty"),
		slog.String("org_id", hydrator.OrgID),
		slog.String("incident_id", incidentID),
	)
	credential, err := hydrator.Resolver.Resolve(ctx, hydrator.Lease, providerfoundation.TenantScope{
		OrgID: hydrator.OrgID, Provider: "pagerduty", CredentialID: hydrator.CredentialID,
	})
	if err != nil {
		return nil, fmt.Errorf("resolve pagerduty credential for hydration: %w", err)
	}
	client, err := providerfoundation.NewPagerDutyClient(
		credential, hydrator.Doer, hydrator.Retry, hydrator.Lease,
	)
	if err != nil {
		return nil, fmt.Errorf("build pagerduty client for hydration: %w", err)
	}
	response, err := client.Do(ctx, http.MethodGet, "/incidents/"+incidentID, nil)
	if err != nil {
		return nil, fmt.Errorf("fetch pagerduty incident %s: %w", incidentID, err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxHydratedIncidentBytes+1))
	if err != nil || len(body) > maxHydratedIncidentBytes {
		return nil, fmt.Errorf("read pagerduty incident %s response", incidentID)
	}
	var envelope struct {
		Incident json.RawMessage `json:"incident"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || len(envelope.Incident) == 0 {
		return nil, fmt.Errorf("pagerduty incident %s response has no incident object", incidentID)
	}
	return envelope.Incident, nil
}

// ReceiptLeaseGuard adapts the receipt claim to the effect sinks' LeaseGuard.
// It is what makes "the receipt is the lease" true rather than aspirational:
// every ClickHouse send re-asserts that this process still holds the receipt.
type ReceiptLeaseGuard struct {
	Fence   ReceiptFence
	Receipt ReceiptClaim
}

func (guard ReceiptLeaseGuard) Assert(ctx context.Context) error {
	if guard.Fence == nil {
		return errUnavailable
	}
	return guard.Fence.Assert(ctx, guard.Receipt)
}

var _ providerfoundation.LeaseGuard = ReceiptLeaseGuard{}
var _ providersync.PagerDutyIncidentHydrator = CredentialIncidentHydrator{}
