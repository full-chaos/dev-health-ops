package workload

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// The Compose bootstrap credential: a named secret, delivered to one workload
// in a restricted file and accepted only at the workload-token exchange. The
// registry keeps its SHA-256 digest; the value itself is never stored.
const (
	BootstrapClass         = "workload_bootstrap_credential"
	BootstrapPrefix        = "fcwlb_"
	BootstrapHashAlgorithm = "sha256"
)

// bootstrapPattern is the value's shape: the prefix and 32 random bytes as
// unpadded base64url.
var bootstrapPattern = regexp.MustCompile(`^fcwlb_[A-Za-z0-9_-]{43}$`)

// ValidBootstrapShape reports whether value has the bootstrap credential's
// shape. A value outside it is refused before any store read.
func ValidBootstrapShape(value string) bool { return bootstrapPattern.MatchString(value) }

// BootstrapDigest is the digest the registry stores for value: SHA-256 of the
// whole value, prefix included.
func BootstrapDigest(value string) [sha256.Size]byte { return sha256.Sum256([]byte(value)) }

// BootstrapRegistration is what ResolveBootstrap returns for a live
// bootstrap credential. Audiences is the workload's audiences narrowed to the
// credential's own; BindingID is zero.
type BootstrapRegistration struct {
	Registration
	CredentialID uuid.UUID
	// ExpiresAt is the credential's expiry; zero when it has none.
	ExpiresAt time.Time
}

const (
	reasonWrongClass  reason = "wrong_class"
	reasonNotPlatform reason = "not_platform"
	reasonRevoked     reason = "revoked"
	reasonExpired     reason = "expired"
)

// ResolveBootstrap maps the digest of a presented bootstrap credential to its
// workload. It reads the current rows on every call and caches nothing; it
// writes nothing. The credential is live while it is unrevoked and now is
// before its expiry. Any refusal and any store error is a *RefusalError, as
// Resolve's.
func (s *Store) ResolveBootstrap(ctx context.Context, digest [sha256.Size]byte, now time.Time) (BootstrapRegistration, error) {
	reg, why, err := s.resolveBootstrap(ctx, digest, now)
	switch {
	case err != nil:
		s.resolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "error")))
		s.logger.ErrorContext(ctx, "workload: bootstrap resolve store failure", logging.ErrorAttr(err))
		return BootstrapRegistration{}, &RefusalError{Reason: ReasonStoreError}
	case why != "":
		s.resolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "refused")))
		s.logger.InfoContext(ctx, "workload: bootstrap resolve refused", "reason", string(why))
		return BootstrapRegistration{}, &RefusalError{Reason: publicReason(why)}
	}
	s.resolves.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", "ok")))
	return reg, nil
}

// bootstrapRow is the one row ResolveBootstrap reads: the credential, its
// workload and its principal.
type bootstrapRow struct {
	credentialID        uuid.UUID
	class, owner        string
	revoked             bool
	expiresAt           *time.Time
	credentialAudiences []string
	principalID         uuid.UUID
	name                string
	audiences, actions  []string
	kind                string
	workloadDisabled    bool
	principalDisabled   bool
	revision            int64
}

func (s *Store) resolveBootstrap(ctx context.Context, digest [sha256.Size]byte, now time.Time) (BootstrapRegistration, reason, error) {
	var row bootstrapRow
	err := s.pool.QueryRow(ctx, `SELECT c.id, c.credential_class, c.domain_owner, c.revoked_at IS NOT NULL, c.expires_at,
c.audiences::text[], w.principal_id, w.name, w.audiences, w.allowed_actions, p.kind,
w.disabled_at IS NOT NULL, p.disabled_at IS NOT NULL, COALESCE(r.revision, 0)
FROM `+s.q+`.credential_registry c
JOIN `+s.q+`.workloads w ON w.principal_id = c.principal_id
JOIN `+s.q+`.principals p ON p.id = c.principal_id
LEFT JOIN `+s.q+`.aggregate_revisions r ON r.aggregate_type = 'workload' AND r.aggregate_id = 'prn_' || w.principal_id::text
WHERE c.secret_hash = $1`, digest[:]).
		Scan(&row.credentialID, &row.class, &row.owner, &row.revoked, &row.expiresAt, &row.credentialAudiences,
			&row.principalID, &row.name, &row.audiences, &row.actions, &row.kind, &row.workloadDisabled, &row.principalDisabled, &row.revision)
	if errors.Is(err, pgx.ErrNoRows) {
		return BootstrapRegistration{}, reasonUnknown, nil
	}
	if err != nil {
		return BootstrapRegistration{}, "", fmt.Errorf("bootstrap resolve: %w", err)
	}
	reg, why := row.decide(now)
	return reg, why, nil
}

// decide is the whole rule for a read row: the class, platform ownership,
// workload kind, revocation, expiry (live while now is before it), the
// workload and principal state, and at least one audience both the workload
// and the credential hold.
func (row bootstrapRow) decide(now time.Time) (BootstrapRegistration, reason) {
	switch {
	case row.class != BootstrapClass:
		return BootstrapRegistration{}, reasonWrongClass
	case row.owner != "platform":
		return BootstrapRegistration{}, reasonNotPlatform
	case row.kind != "workload":
		return BootstrapRegistration{}, reasonWrongKind
	case row.revoked:
		return BootstrapRegistration{}, reasonRevoked
	case row.expiresAt != nil && !now.Before(*row.expiresAt):
		return BootstrapRegistration{}, reasonExpired
	case row.workloadDisabled:
		return BootstrapRegistration{}, reasonWorkloadDisabled
	case row.principalDisabled:
		return BootstrapRegistration{}, reasonPrincipalOff
	}
	reg := BootstrapRegistration{
		Registration: Registration{PrincipalID: row.principalID, Name: row.name, Audiences: intersect(row.audiences, row.credentialAudiences),
			AllowedActions: row.actions, Revision: row.revision},
		CredentialID: row.credentialID,
	}
	if len(reg.Audiences) == 0 {
		return BootstrapRegistration{}, reasonNoAudience
	}
	if row.expiresAt != nil {
		reg.ExpiresAt = *row.expiresAt
	}
	return reg, ""
}

// intersect keeps the members of a that b also holds, in a's order.
func intersect(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, x := range b {
		in[x] = true
	}
	out := []string{}
	for _, x := range a {
		if in[x] {
			out = append(out, x)
		}
	}
	return out
}
