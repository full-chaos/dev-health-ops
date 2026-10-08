package categorize

import (
	"context"
	"io"
	"net/http"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// One span per HTTP attempt to an LLM provider. Every provider sends through
// tracedLLMDo, so no provider has its own copy of this.
const (
	llmSpanTracer = "dev_health/llm"
	llmSpanName   = "dev_health.llm.request"

	llmAttrProvider = "llm.provider"
	llmAttrModel    = "llm.model"
	llmAttrRole     = "llm.role"
	llmAttrAttempt  = "llm.attempt"
	llmAttrStatus   = "http.response.status_code"
	llmAttrClass    = "error.class"
	llmAttrBatchOp  = "llm.batch.operation"

	llmSpanMaxModelLen = 128
)

// LLMRole says why a request is made. Absent from the context, the role
// attribute is left off.
type LLMRole string

const (
	LLMRoleServed LLMRole = "served"
	LLMRoleShadow LLMRole = "shadow"
)

// llmSpanClass is the closed set of error.class values. The words timeout,
// rate_limit, server, auth, model_not_found and invalid_request are the
// SystemOneClass words.
type llmSpanClass string

const (
	llmSpanClassTimeout       llmSpanClass = "timeout"
	llmSpanClassRefused       llmSpanClass = "refused"
	llmSpanClassServer        llmSpanClass = "server"
	llmSpanClassRateLimit     llmSpanClass = "rate_limit"
	llmSpanClassAuth          llmSpanClass = "auth"
	llmSpanClassModelNotFound llmSpanClass = "model_not_found"
	llmSpanClassInvalid       llmSpanClass = "invalid_request"
	llmSpanClassInvalidAnswer llmSpanClass = "invalid_answer"
	llmSpanClassCanceled      llmSpanClass = "canceled"
	llmSpanClassOther         llmSpanClass = "other"
)

var llmSpanClasses = []llmSpanClass{
	llmSpanClassTimeout, llmSpanClassRefused, llmSpanClassServer, llmSpanClassRateLimit,
	llmSpanClassAuth, llmSpanClassModelNotFound, llmSpanClassInvalid, llmSpanClassInvalidAnswer, llmSpanClassCanceled, llmSpanClassOther,
}

type (
	llmRoleKey    struct{}
	llmAttemptKey struct{}
	llmBatchOpKey struct{}
)

// WithLLMRole marks the requests made under ctx as served or shadow.
func WithLLMRole(ctx context.Context, role LLMRole) context.Context {
	return context.WithValue(ctx, llmRoleKey{}, role)
}

// withLLMAttempt sets the 1-based attempt number of the request about to be sent.
func withLLMAttempt(ctx context.Context, attempt int) context.Context {
	return context.WithValue(ctx, llmAttemptKey{}, attempt)
}

// withLLMBatchOperation names the Batch API call about to be sent (upload,
// create, retrieve, content, cancel). Absent, the attribute is left off.
func withLLMBatchOperation(ctx context.Context, operation string) context.Context {
	return context.WithValue(ctx, llmBatchOpKey{}, operation)
}

// classifyLLMSpan maps one finished exchange to a class. "" means success.
func classifyLLMSpan(status int, transportErr error, ctxErr error) llmSpanClass {
	if transportErr != nil {
		if ctxErr != nil {
			return llmSpanClassCanceled
		}
		switch logging.TransportClass(transportErr) {
		case "timeout", "deadline":
			return llmSpanClassTimeout
		case "refused":
			return llmSpanClassRefused
		case "canceled":
			return llmSpanClassCanceled
		}
		return llmSpanClassOther
	}
	switch {
	case status == http.StatusOK:
		return ""
	case status == http.StatusTooManyRequests:
		return llmSpanClassRateLimit
	case status == http.StatusUnauthorized, status == http.StatusPaymentRequired, status == http.StatusForbidden:
		return llmSpanClassAuth
	case status == http.StatusNotFound:
		return llmSpanClassModelNotFound
	case status == http.StatusBadRequest, status == http.StatusUnprocessableEntity:
		return llmSpanClassInvalid
	case status >= 500:
		return llmSpanClassServer
	}
	return llmSpanClassOther
}

// tracedLLMDo sends req on client (never following a redirect) inside one span.
// The span ends when the caller closes the response body, so a failure while
// the body is read is part of it. provider and model are the configured values.
func tracedLLMDo(client *http.Client, req *http.Request, provider ProviderKind, model string) (*http.Response, error) {
	ctx := req.Context()
	if len(model) > llmSpanMaxModelLen {
		model = model[:llmSpanMaxModelLen]
	}
	attrs := []attribute.KeyValue{
		attribute.String(llmAttrProvider, string(provider)),
		attribute.String(llmAttrModel, model),
	}
	if role, ok := ctx.Value(llmRoleKey{}).(LLMRole); ok && (role == LLMRoleServed || role == LLMRoleShadow) {
		attrs = append(attrs, attribute.String(llmAttrRole, string(role)))
	}
	if attempt, ok := ctx.Value(llmAttemptKey{}).(int); ok && attempt > 0 {
		attrs = append(attrs, attribute.Int(llmAttrAttempt, attempt))
	}
	if operation, ok := ctx.Value(llmBatchOpKey{}).(string); ok && operation != "" {
		attrs = append(attrs, attribute.String(llmAttrBatchOp, operation))
	}
	spanCtx, span := otel.Tracer(llmSpanTracer).Start(ctx, llmSpanName, oteltrace.WithAttributes(attrs...))

	resp, err := httpguard.NoRedirects(client).Do(req.WithContext(spanCtx)) // the API key rides this request
	if err != nil {
		endLLMSpan(span, 0, classifyLLMSpan(0, err, ctx.Err()))
		return nil, logging.TransportFailure(err)
	}
	resp.Body = &llmSpanBody{ReadCloser: resp.Body, span: span, status: resp.StatusCode, ctx: ctx}
	return resp, nil
}

func endLLMSpan(span oteltrace.Span, status int, class llmSpanClass) {
	if status > 0 {
		span.SetAttributes(attribute.Int(llmAttrStatus, status))
	}
	if class != "" {
		span.SetAttributes(attribute.String(llmAttrClass, string(class)))
		span.SetStatus(codes.Error, string(class))
	}
	span.End()
}

type llmSpanBody struct {
	io.ReadCloser
	span    oteltrace.Span
	status  int
	ctx     context.Context
	once    sync.Once
	readErr error
	invalid bool
}

func (b *llmSpanBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF && b.readErr == nil {
		b.readErr = err
	}
	return n, err
}

func (b *llmSpanBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		class := classifyLLMSpan(b.status, nil, nil)
		if class == "" && b.readErr != nil {
			class = classifyLLMSpan(b.status, b.readErr, b.ctx.Err())
		}
		if class == "" && b.invalid {
			class = llmSpanClassInvalidAnswer
		}
		endLLMSpan(b.span, b.status, class)
	})
	return err
}

// markInvalidAnswer records that a 200 response did not hold a usable answer
// (undecodable, oversized or incomplete). It does nothing for a response that
// did not come from tracedLLMDo.
func markInvalidAnswer(resp *http.Response) {
	if resp == nil {
		return
	}
	if body, ok := resp.Body.(*llmSpanBody); ok {
		body.invalid = true
	}
}
