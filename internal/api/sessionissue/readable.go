package sessionissue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/repokey"
)

// PathReadableRepositories is auth-service's read listing of one user principal in one organization (acp ruling Q1 on
// CHAOS-13561): the repositories the principal may read through acr.context.read, by the rule of
// POST /v1/authorize:repositories, on go-api's own workload token.
const PathReadableRepositories = "/internal/v1/principals/readable-repositories"

const readableSchema = "readable_repositories.v1"

// ReadableScope is the scope member of the listing.
type ReadableScope string

const (
	// ReadableOrganization: the principal may read every repository of the organization. No list is sent.
	ReadableOrganization ReadableScope = "organization"
	// ReadableRepositories: the principal may read exactly Readable.Repositories ([] for none).
	ReadableRepositories ReadableScope = "repositories"
	// ReadableDenied: the read decision denies the listing; Readable.Reason names why.
	ReadableDenied ReadableScope = "denied"
)

// ReasonAllowlistTooLarge is the deny of a principal that may read more repositories than one answer carries
// (MaxReadableRepositories): the list is refused, never truncated.
const ReasonAllowlistTooLarge = "repository_allowlist_too_large"

// MaxReadableRepositories is the most keys one listing carries (authorization.MaxRepositoryAllowlist).
const MaxReadableRepositories = 256

// Readable is one listing.
type Readable struct {
	Scope ReadableScope
	// Repositories is set, sorted and unique, only for ReadableRepositories.
	Repositories []string
	// Reason is set only for ReadableDenied.
	Reason string
}

// ReadableReader reads one principal's read listing.
type ReadableReader interface {
	ReadableRepositories(ctx context.Context, principalID, organizationID uuid.UUID) (Readable, error)
}

var _ ReadableReader = (*Client)(nil)

// maxReadableAnswerBytes bounds the listing's answer: MaxReadableRepositories keys of repokey.MaxBytes, each quoted
// and comma-separated, and the members around them. It is above maxAnswerBytes: 256 keys of 255 bytes do not fit 64 KiB.
const maxReadableAnswerBytes = MaxReadableRepositories*(repokey.MaxBytes+3) + 1<<10

// ReadableRepositories asks auth-service for the read listing of principalID in organizationID with go-api's workload
// token. Every failure (the route unreachable or slow, any status but 200, or an answer that is not exactly one
// readable_repositories.v1 document) wraps ErrUnavailable: it is never an empty listing.
func (c *Client) ReadableRepositories(ctx context.Context, principalID, organizationID uuid.UUID) (Readable, error) {
	raw, err := json.Marshal(map[string]string{"principal_id": principalID.String(), "organization_id": organizationID.String()})
	if err != nil {
		return Readable{}, fmt.Errorf("%w: encoding the listing request: %w", ErrUnavailable, err)
	}
	out, err := http.NewRequestWithContext(ctx, http.MethodPost, c.readableEndpoint, bytes.NewReader(raw))
	if err != nil {
		return Readable{}, fmt.Errorf("%w: building the listing request: %w", ErrUnavailable, err)
	}
	bearer, err := c.bearer(ctx)
	if err != nil {
		return Readable{}, err
	}
	out.Header.Set("Authorization", "Bearer "+bearer)
	out.Header.Set("Content-Type", "application/json")
	out.Header.Set("Accept", "application/json")
	response, err := c.http.Do(out)
	if err != nil {
		return Readable{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer response.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(response.Body, maxReadableAnswerBytes+1))
	if err != nil {
		return Readable{}, fmt.Errorf("%w: reading the listing answer: %w", ErrUnavailable, err)
	}
	if response.StatusCode != http.StatusOK {
		if refused := refusalIn(response.StatusCode, answer); refused != nil {
			return Readable{}, fmt.Errorf("%w: listing route refused: %w", ErrUnavailable, refused)
		}
		return Readable{}, fmt.Errorf("%w: listing route status %d", ErrUnavailable, response.StatusCode)
	}
	if len(answer) > maxReadableAnswerBytes {
		return Readable{}, fmt.Errorf("%w: listing answer over %d bytes", ErrUnavailable, maxReadableAnswerBytes)
	}
	listing, err := parseReadable(answer)
	if err != nil {
		return Readable{}, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return listing, nil
}

var errReadableAnswer = errors.New("the listing answer is not exactly one readable_repositories.v1 document")

// parseReadable accepts exactly: schema_version readable_repositories.v1 and scope organization with no other member;
// scope repositories with a repositories array of at most MaxReadableRepositories canonical keys, sorted and unique;
// or scope denied with a reason_code. A repeated, missing or unknown member is refused.
func parseReadable(answer []byte) (Readable, error) {
	members, ok := objectMembers(answer)
	if !ok {
		return Readable{}, errReadableAnswer
	}
	var schema, scope string
	if json.Unmarshal(members["schema_version"], &schema) != nil || schema != readableSchema ||
		json.Unmarshal(members["scope"], &scope) != nil {
		return Readable{}, errReadableAnswer
	}
	switch ReadableScope(scope) {
	case ReadableOrganization:
		if len(members) != 2 {
			return Readable{}, errReadableAnswer
		}
		return Readable{Scope: ReadableOrganization}, nil
	case ReadableRepositories:
		raw, present := members["repositories"]
		if len(members) != 3 || !present || string(raw) == "null" {
			return Readable{}, errReadableAnswer
		}
		var keys []string
		if json.Unmarshal(raw, &keys) != nil || len(keys) > MaxReadableRepositories {
			return Readable{}, errReadableAnswer
		}
		for i, key := range keys {
			if !repokey.Valid(key) || key == "*" || (i > 0 && keys[i-1] >= key) {
				return Readable{}, errReadableAnswer
			}
		}
		return Readable{Scope: ReadableRepositories, Repositories: slices.Clip(append([]string{}, keys...))}, nil
	case ReadableDenied:
		var reason string
		if len(members) != 3 || json.Unmarshal(members["reason_code"], &reason) != nil || !reasonCodePattern.MatchString(reason) {
			return Readable{}, errReadableAnswer
		}
		return Readable{Scope: ReadableDenied, Reason: reason}, nil
	}
	return Readable{}, errReadableAnswer
}
