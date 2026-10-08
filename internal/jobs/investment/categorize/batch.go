package categorize

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// BatchJobStatus is llm/providers/batch.py's BatchJobStatus, narrowed to the
// values a provider reports. created/submitting were the Python store's own
// states before a provider job existed.
type BatchJobStatus string

const (
	BatchJobSubmitted BatchJobStatus = "submitted"
	BatchJobRunning   BatchJobStatus = "running"
	BatchJobSucceeded BatchJobStatus = "succeeded"
	BatchJobFailed    BatchJobStatus = "failed"
	BatchJobCancelled BatchJobStatus = "cancelled"
	BatchJobExpired   BatchJobStatus = "expired"
)

// Terminal reports whether the provider will not change the job any more.
func (s BatchJobStatus) Terminal() bool {
	switch s {
	case BatchJobSucceeded, BatchJobFailed, BatchJobCancelled, BatchJobExpired:
		return true
	}
	return false
}

// BatchItem is one request of a provider batch (batch.py's BatchItemRequest).
// CustomID is how its result is found again.
type BatchItem struct {
	CustomID string
	Request  CompletionRequest
}

// BatchSubmission is a provider job that was created (BatchJobSubmission).
type BatchSubmission struct {
	ProviderJobID string
	InputFileID   string
	ItemCount     int
}

// BatchState is one poll of a provider job (BatchJobState).
type BatchState struct {
	ProviderJobID  string
	Status         BatchJobStatus
	ProviderStatus string
	TotalCount     int
	CompletedCount int
	FailedCount    int
	OutputFileID   string
	ErrorFileID    string
}

// BatchItemResult is one line of a provider job's output or error file
// (BatchItemResult plus the keys of its provider_metadata).
type BatchItemResult struct {
	CustomID string
	// Text is the completion text, "" when the line held none.
	Text string
	// ErrorCode is set when the line is not a completion: the provider's
	// error code, or http_<status> for a response with no completion text.
	ErrorCode    string
	ErrorMessage string

	LineID            string
	RequestID         string
	StatusCode        *int
	InputTokens       *int
	OutputTokens      *int
	CachedInputTokens *int
	ProviderErrorCode string
	ProviderErrorType string
}

// Succeeded is batch.py's BatchItemResult.succeeded.
func (r BatchItemResult) Succeeded() bool {
	return r.ErrorCode == "" && r.Text != ""
}

// BatchProvider is the provider Batch API (batch.py's BatchProvider): submit
// many requests at once, at a lower price, answered within a completion
// window instead of at once. It is optional: a caller finds it with
// AsBatchProvider, and a Provider without it has no batch mode. Provider
// itself does not change.
type BatchProvider interface {
	SubmitBatch(ctx context.Context, items []BatchItem) (BatchSubmission, error)
	PollBatch(ctx context.Context, providerJobID string) (BatchState, error)
	// FetchBatchResults returns the lines of the output file, then of the
	// error file. A custom id may be missing; the caller decides what that
	// means.
	FetchBatchResults(ctx context.Context, providerJobID string) ([]BatchItemResult, error)
	CancelBatch(ctx context.Context, providerJobID string) error
}

// AsBatchProvider returns the Batch API of provider, if it has one.
func AsBatchProvider(provider Provider) (BatchProvider, bool) {
	batch, ok := provider.(BatchProvider)
	return batch, ok
}

// ErrEmptyBatch is returned for a batch with no items.
var ErrEmptyBatch = errors.New("categorize: cannot submit an empty provider batch")

// validateBatchItems is BatchItemRequest.__post_init__ over a whole batch,
// plus one rule Python's store enforced by a unique key: no custom id twice.
func validateBatchItems(items []BatchItem) error {
	if len(items) == 0 {
		return ErrEmptyBatch
	}
	seen := make(map[string]struct{}, len(items))
	for index, item := range items {
		if strings.TrimSpace(item.CustomID) == "" {
			return fmt.Errorf("categorize: batch item %d: custom id is required", index)
		}
		if strings.IndexFunc(item.CustomID, unicode.IsSpace) >= 0 {
			return fmt.Errorf("categorize: batch item %d: custom id must not contain whitespace", index)
		}
		if item.Request.Prompt == "" {
			return fmt.Errorf("categorize: batch item %d: prompt is required", index)
		}
		if _, dup := seen[item.CustomID]; dup {
			return fmt.Errorf("categorize: batch item %d: custom id %q appears twice", index, item.CustomID)
		}
		seen[item.CustomID] = struct{}{}
	}
	return nil
}
