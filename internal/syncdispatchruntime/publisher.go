package syncdispatchruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

var (
	ErrInvalidPublisher = errors.New("invalid sync dispatch River publisher")
	ErrRiverInsert      = errors.New("sync dispatch River insert failed")
	ErrInsertRejected   = errors.New("sync dispatch River insert was rejected")
)

var queuePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// InsertClient is the smallest River API surface required for atomic
// publishing. The publisher cannot begin, commit, roll back, or use a more
// privileged database handle than the transaction its caller supplies.
type InsertClient interface {
	InsertTx(context.Context, pgx.Tx, river.JobArgs, *river.InsertOpts) (*rivertype.JobInsertResult, error)
}

// PublisherOptions are explicit because this package does not own dispatch
// queue policy. Callers must choose a bounded queue and retry limit during a
// separately approved composition step.
type PublisherOptions struct {
	Queue       string
	MaxAttempts int
}

func (options PublisherOptions) valid() bool {
	return queuePattern.MatchString(options.Queue) && options.MaxAttempts >= 1 && options.MaxAttempts <= 100
}

// Publisher performs exactly one supported River InsertTx call. It has no
// route selection, claim mutation, logging, or handler activation behavior.
type Publisher struct {
	client  InsertClient
	options PublisherOptions
}

func NewPublisher(client InsertClient, options PublisherOptions) (*Publisher, error) {
	if !present(client) || !options.valid() {
		return nil, ErrInvalidPublisher
	}
	return &Publisher{client: client, options: options}, nil
}

func (publisher *Publisher) valid() bool {
	return publisher != nil && present(publisher.client) && publisher.options.valid()
}

// Publish inserts a concrete v1 River job in the caller's already-open
// least-privilege transaction. A failure is intentionally opaque so callers
// do not persist or log encoded arguments or driver details.
func (publisher *Publisher) Publish(
	ctx context.Context,
	tx pgx.Tx,
	claim Claim,
	reference DomainReference,
) (string, error) {
	if !publisher.valid() || ctx == nil || !present(tx) {
		return "", ErrInvalidPublisher
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	args, err := Convert(claim, reference)
	if err != nil {
		return "", err
	}
	result, err := publisher.client.InsertTx(ctx, tx, args, &river.InsertOpts{
		Queue:       publisher.options.Queue,
		MaxAttempts: publisher.options.MaxAttempts,
		UniqueOpts: river.UniqueOpts{
			ByArgs:  true,
			ByState: rivertype.JobStates(),
		},
	})
	if err != nil {
		return "", ErrRiverInsert
	}
	if err := verifyInsert(result, args, publisher.options); err != nil {
		return "", err
	}
	return strconv.FormatInt(result.Job.ID, 10), nil
}

func verifyInsert(result *rivertype.JobInsertResult, args Args, options PublisherOptions) error {
	if result == nil || result.Job == nil || result.Job.ID <= 0 {
		return fmt.Errorf("%w: missing job identity", ErrInsertRejected)
	}
	if result.Job.Kind != args.Kind() || result.Job.Queue != options.Queue || result.Job.MaxAttempts != options.MaxAttempts {
		return fmt.Errorf("%w: returned job policy mismatch", ErrInsertRejected)
	}
	// river_job.encoded_args is jsonb, so PostgreSQL may normalize whitespace
	// in a returned projection. Decode it strictly instead of byte-comparing
	// formatting, while still rejecting unknown fields or any value drift.
	if len(result.Job.EncodedArgs) == 0 || !matchesReturnedArgs(result.Job.EncodedArgs, args) {
		return fmt.Errorf("%w: returned argument shape mismatch", ErrInsertRejected)
	}
	return nil
}

func matchesReturnedArgs(encoded []byte, expected Args) bool {
	var returned TransportArgs
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&returned); err != nil || returned.valid() != nil {
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return false
	}
	return returned.Version == expected.ContractVersion() && returned.OrgID == expected.OrganizationID() &&
		returned.RunID == expected.SyncRunID() && returned.DispatchOutbox == expected.OutboxID() &&
		returned.RouteGeneration == expected.RouteGeneration()
}
