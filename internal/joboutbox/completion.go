package joboutbox

import (
	"context"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const maxCompletionKeyLength = 256

var completionDomainPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,95}$`)

// CompletionKey names the committed success of one durable domain object.
// It is safe to persist in an outbox row before that success exists.
func CompletionKey(domain string, id string) (string, error) {
	domain = strings.TrimSpace(domain)
	parsed, err := uuid.Parse(id)
	if err != nil || !completionDomainPattern.MatchString(domain) {
		return "", ErrContractRejected
	}
	key := domain + ":" + parsed.String()
	if len(key) > maxCompletionKeyLength {
		return "", ErrContractRejected
	}
	return key, nil
}

// MarkCompletionTx makes dependent handoffs relay-eligible in the same
// transaction that commits their predecessor's terminal success.
func MarkCompletionTx(ctx context.Context, tx pgx.Tx, completionKey string) error {
	if tx == nil || !validCompletionKey(completionKey) {
		return ErrInvalidConfiguration
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO public.worker_job_completion_fences (completion_key)
VALUES ($1)
ON CONFLICT (completion_key) DO NOTHING`, completionKey); err != nil {
		return ErrUnavailable
	}
	return nil
}

func validCompletionKey(completionKey string) bool {
	if completionKey == "" || len(completionKey) > maxCompletionKeyLength {
		return false
	}
	domain, id, found := strings.Cut(completionKey, ":")
	if !found || strings.Contains(id, ":") || !completionDomainPattern.MatchString(domain) {
		return false
	}
	parsed, err := uuid.Parse(id)
	return err == nil && parsed.String() == id
}
