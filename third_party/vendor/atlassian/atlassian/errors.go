package atlassian

import (
	"fmt"
	"strings"
	"time"
)

type TransportError struct {
	StatusCode  int
	BodySnippet string
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("unexpected HTTP status %d", e.StatusCode)
}

type RateLimitError struct {
	RetryAfter     time.Time
	Attempts       int
	HeaderValue    string
	WaitSeconds    float64
	MaxWaitSeconds float64
}

func (e *RateLimitError) Error() string {
	builder := strings.Builder{}
	builder.WriteString(fmt.Sprintf("rate limited after %d attempt(s)", e.Attempts))
	if !e.RetryAfter.IsZero() {
		builder.WriteString("; retry_at=")
		builder.WriteString(e.RetryAfter.UTC().Format(time.RFC3339))
	}
	if e.HeaderValue != "" {
		builder.WriteString("; retry-after=")
		builder.WriteString(e.HeaderValue)
	}
	if e.WaitSeconds > 0 && e.MaxWaitSeconds > 0 {
		builder.WriteString(fmt.Sprintf("; wait_seconds=%.3f max_wait_seconds=%.3f", e.WaitSeconds, e.MaxWaitSeconds))
	}
	return builder.String()
}

type LocalRateLimitError struct {
	EstimatedCost  float64
	WaitSeconds    float64
	MaxWaitSeconds float64
}

func (e *LocalRateLimitError) Error() string {
	return fmt.Sprintf(
		"local rate limit exceeded; estimated_cost=%.2f; wait_seconds=%.3f exceeds max_wait_seconds=%.3f",
		e.EstimatedCost,
		e.WaitSeconds,
		e.MaxWaitSeconds,
	)
}

type GraphQLOperationError struct {
	Errors      []GraphQLError
	PartialData map[string]any
}

func (e *GraphQLOperationError) Error() string {
	if len(e.Errors) == 0 {
		return "graphql operation failed"
	}
	first := e.Errors[0]
	builder := strings.Builder{}
	builder.WriteString(first.Message)
	if len(first.Path) > 0 {
		builder.WriteString(" path=")
		builder.WriteString(fmt.Sprint(first.Path))
	}
	if first.Extensions != nil {
		for _, key := range []string{"requiredScopes", "required_scopes", "required_scopes_any", "required_scopes_all"} {
			if val, ok := first.Extensions[key]; ok && val != nil {
				builder.WriteString(" required_scopes=")
				builder.WriteString(fmt.Sprint(val))
				break
			}
		}
	}
	return builder.String()
}

type JSONError struct {
	Err error
}

func (e *JSONError) Error() string {
	return fmt.Sprintf("decode response: %v", e.Err)
}

func (e *JSONError) Unwrap() error {
	return e.Err
}
