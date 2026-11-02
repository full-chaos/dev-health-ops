package outboxprojection

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/auth/restorefence"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Config builds an Engine.
type Config struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	// ProjectOrganization makes auth.organizations hold the organization before
	// the store step (a real organization can be absent from the read projection
	// until its first sign-in). found is false when it exists in neither
	// projection source. Nil skips the step.
	ProjectOrganization func(ctx context.Context, organization uuid.UUID) (found bool, err error)
	Logger              *slog.Logger
	// Meter registers the engine's counter; nil uses the process meter.
	Meter metric.Meter
}

// Engine projects external credential lifecycle events. It is safe for
// concurrent use.
type Engine struct {
	cfg     Config
	q       string
	counter metric.Int64Counter
}

// ErrInvalidConfig reports an unusable Config.
var ErrInvalidConfig = errors.New("outboxprojection: invalid configuration")

// New returns an Engine.
func New(cfg Config) (*Engine, error) {
	if cfg.Pool == nil {
		return nil, fmt.Errorf("%w: no pool", ErrInvalidConfig)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	meter := cfg.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection")
	}
	counter, err := meter.Int64Counter("devhealth_auth_credential_projection_total",
		metric.WithDescription("Credential lifecycle events projected, by domain and closed outcome."))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}
	return &Engine{cfg: cfg, q: authschema.Quote(cfg.Schema), counter: counter}, nil
}

// Result is what Project decided. State is the projected state after an
// applied event, and the stored state otherwise.
type Result struct {
	Outcome Outcome
	Class   Class
	State   pushlifecycle.State
}

func (e *Engine) result(ctx context.Context, in Input, outcome Outcome, state pushlifecycle.State) Result {
	e.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("domain", string(in.Domain)), attribute.String("outcome", string(outcome))))
	return Result{Outcome: outcome, Class: ClassOf(outcome), State: state}
}

// Project is the one call: the restore gate, the organization projection, the
// digest conflict check, pushlifecycle.Apply, the state, the registry row and
// the audit row, in one transaction. The error is infrastructure only; every
// decision is a Result. A retry outcome writes nothing: the caller parks the
// event (the reader) or answers 409 (the route).
func (e *Engine) Project(ctx context.Context, in Input) (Result, error) {
	if err := in.Check(); err != nil {
		e.cfg.Logger.WarnContext(ctx, "credential projection refused a malformed event", "domain", in.Domain, "reason", "malformed")
		return e.result(ctx, in, OutcomeMalformed, pushlifecycle.State{}), nil
	}
	if err := restorefence.Check(ctx, e.cfg.Pool, e.cfg.Schema); err != nil {
		if errors.Is(err, restorefence.ErrNotFenced) {
			return e.result(ctx, in, OutcomeUnfenced, pushlifecycle.State{}), nil
		}
		return Result{}, err
	}
	organization, _ := ParseOrganization(in.Event.Payload.OrganizationID)
	if in.Domain == DomainAcr {
		// For acr the fingerprint is derived, not read.
		in.Event.Payload.Change.Fingerprint = AcrFingerprint(in.Event.AggregateID)
	}
	if e.cfg.ProjectOrganization != nil {
		found, err := e.cfg.ProjectOrganization(ctx, organization)
		if err != nil {
			return Result{}, fmt.Errorf("outboxprojection: projecting the organization: %w", err)
		}
		if !found {
			return e.result(ctx, in, OutcomeOrganizationUnknown, pushlifecycle.State{}), nil
		}
	}

	orgText := organization.String()
	var actor *string
	if in.Actor != uuid.Nil {
		text := in.Actor.String()
		actor = &text
	}
	// The audit row's outcome is fixed before Apply decides. A refused decision is denied, anything else allowed: Apply
	// refuses to write under the wrong outcome and the call runs again under the right one, once.
	var res Result
	final, err := runTries(func(denied bool) (Result, error) {
		attrs := map[string]any{"target_kind": "credential", "credential_class": auditClass(in.Domain), "revision": int64(in.Event.Payload.Revision)}
		outcome := audit.OutcomeAllowed
		if denied {
			outcome = audit.OutcomeDenied
		}
		err := audit.Commit(ctx, e.cfg.Pool, e.cfg.Schema, audit.Mutation{
			Apply: func(ctx context.Context, tx audit.TxOps) error {
				var err error
				res, err = e.apply(ctx, tx, in, organization, attrs, denied)
				return err
			},
			// Commit reads the attributes after Apply, which sets the reason and status it decided.
			Audit:     audit.AuditEvent{EventType: "auth.credential_projection.decided", Outcome: outcome, ActorPrincipalID: actor, OrganizationID: &orgText, Attributes: attrs},
			OffStream: audit.OffStreamCredentialProjection,
		})
		if err != nil && !errors.Is(err, errOutcomeMismatch) && !errors.Is(err, audit.ErrNoStateChange) {
			return Result{}, err
		}
		if errors.Is(err, errOutcomeMismatch) {
			return Result{}, errOutcomeMismatch
		}
		if res.Outcome == "" {
			return Result{}, fmt.Errorf("outboxprojection: the mutation decided nothing")
		}
		return res, nil
	})
	if err != nil {
		return Result{}, err
	}
	return e.result(ctx, in, final.Outcome, final.State), nil
}

// runTries runs try under an allowed audit row, and, when it reports that the decision was a refusal
// (errOutcomeMismatch), once more under a denied row. If that second try decides something other than a refusal the
// state moved: there is no third try, nothing was written, and the answer is a retry.
func runTries(try func(denied bool) (Result, error)) (Result, error) {
	res, err := try(false)
	if !errors.Is(err, errOutcomeMismatch) {
		return res, err
	}
	res, err = try(true)
	if errors.Is(err, errOutcomeMismatch) {
		return Result{Outcome: OutcomeStateChanged, Class: ClassRetry}, nil
	}
	return res, err
}

func auditClass(d Domain) string {
	if d == DomainAcr {
		return "acr_client"
	}
	return "external_push"
}
