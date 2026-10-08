package categorize

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// The OpenAI Batch API (openai.py's submit_batch/poll_batch/fetch_batch_results/
// cancel_batch). Every call goes through tracedLLMDo with the batch operation
// as a span attribute, and is retried once on a retryable failure like the
// synchronous call.
const (
	// openAIBatchEndpoint is the endpoint every line is sent to. It is the
	// Responses API for every model because the synchronous call of this
	// provider is: openai.py chose /v1/chat/completions below gpt-5, a model
	// family this provider has no port for.
	openAIBatchEndpoint         = "/v1/responses"
	openAIBatchCompletionWindow = "24h"
	openAIBatchFileName         = "investment-categorization-batch.jsonl"
	openAIBatchSource           = "investment_materialize"

	batchOpUpload   = "upload"
	batchOpCreate   = "create"
	batchOpRetrieve = "retrieve"
	batchOpContent  = "content"
	batchOpCancel   = "cancel"
)

var _ BatchProvider = (*OpenAIProvider)(nil)

type openAIBatchLine struct {
	CustomID string                 `json:"custom_id"`
	Method   string                 `json:"method"`
	URL      string                 `json:"url"`
	Body     openAIResponsesRequest `json:"body"`
}

// batchLine is openai.py's _batch_line: the body is the synchronous request
// body, from the same builder, at the first attempt's token budget.
func (p *OpenAIProvider) batchLine(item BatchItem) ([]byte, error) {
	return json.Marshal(openAIBatchLine{
		CustomID: item.CustomID,
		Method:   http.MethodPost,
		URL:      openAIBatchEndpoint,
		Body:     p.responsesBody(item.Request, p.initialMaxOutputTokens(item.Request)),
	})
}

// SubmitBatch uploads one JSONL file (purpose batch) and creates the batch.
func (p *OpenAIProvider) SubmitBatch(ctx context.Context, items []BatchItem) (BatchSubmission, error) {
	if err := validateBatchItems(items); err != nil {
		return BatchSubmission{}, err
	}
	var payload bytes.Buffer
	for index, item := range items {
		line, err := p.batchLine(item)
		if err != nil {
			return BatchSubmission{}, fmt.Errorf("encode batch line: %w", err)
		}
		if index > 0 {
			payload.WriteByte('\n')
		}
		payload.Write(line)
	}

	var uploaded struct {
		ID string `json:"id"`
	}
	if err := p.batchCall(ctx, batchOpUpload, func(ctx context.Context) (*http.Request, error) {
		return p.uploadRequest(ctx, payload.Bytes())
	}, &uploaded); err != nil {
		return BatchSubmission{}, err
	}
	if uploaded.ID == "" {
		return BatchSubmission{}, errors.New("categorize: batch file upload returned no file id")
	}

	create, err := json.Marshal(map[string]any{
		"input_file_id":     uploaded.ID,
		"endpoint":          openAIBatchEndpoint,
		"completion_window": openAIBatchCompletionWindow,
		"metadata":          map[string]string{"source": openAIBatchSource},
	})
	if err != nil {
		return BatchSubmission{}, fmt.Errorf("encode batch create: %w", err)
	}
	var created struct {
		ID string `json:"id"`
	}
	if err := p.batchCall(ctx, batchOpCreate, func(ctx context.Context) (*http.Request, error) {
		return p.jsonRequest(ctx, http.MethodPost, "/batches", create)
	}, &created); err != nil {
		return BatchSubmission{}, err
	}
	if created.ID == "" {
		return BatchSubmission{}, errors.New("categorize: batch create returned no batch id")
	}
	return BatchSubmission{ProviderJobID: created.ID, InputFileID: uploaded.ID, ItemCount: len(items)}, nil
}

type openAIBatchObject struct {
	Status        string `json:"status"`
	OutputFileID  string `json:"output_file_id"`
	ErrorFileID   string `json:"error_file_id"`
	RequestCounts *struct {
		Total     int `json:"total"`
		Completed int `json:"completed"`
		Failed    int `json:"failed"`
	} `json:"request_counts"`
}

func (p *OpenAIProvider) retrieveBatch(ctx context.Context, providerJobID string) (openAIBatchObject, error) {
	var batch openAIBatchObject
	err := p.batchCall(ctx, batchOpRetrieve, func(ctx context.Context) (*http.Request, error) {
		return p.jsonRequest(ctx, http.MethodGet, "/batches/"+url.PathEscape(providerJobID), nil)
	}, &batch)
	return batch, err
}

// PollBatch is openai.py's poll_batch.
func (p *OpenAIProvider) PollBatch(ctx context.Context, providerJobID string) (BatchState, error) {
	batch, err := p.retrieveBatch(ctx, providerJobID)
	if err != nil {
		return BatchState{}, err
	}
	state := BatchState{
		ProviderJobID:  providerJobID,
		Status:         mapOpenAIBatchStatus(batch.Status),
		ProviderStatus: batch.Status,
		OutputFileID:   batch.OutputFileID,
		ErrorFileID:    batch.ErrorFileID,
	}
	if counts := batch.RequestCounts; counts != nil {
		state.TotalCount, state.CompletedCount, state.FailedCount = counts.Total, counts.Completed, counts.Failed
	}
	return state, nil
}

// FetchBatchResults is openai.py's fetch_batch_results: the output file's
// lines, then the error file's.
func (p *OpenAIProvider) FetchBatchResults(ctx context.Context, providerJobID string) ([]BatchItemResult, error) {
	batch, err := p.retrieveBatch(ctx, providerJobID)
	if err != nil {
		return nil, err
	}
	var results []BatchItemResult
	for _, fileID := range []string{batch.OutputFileID, batch.ErrorFileID} {
		if fileID == "" {
			continue
		}
		var content []byte
		if err := p.batchCall(ctx, batchOpContent, func(ctx context.Context) (*http.Request, error) {
			return p.jsonRequest(ctx, http.MethodGet, "/files/"+url.PathEscape(fileID)+"/content", nil)
		}, &content); err != nil {
			return nil, err
		}
		parsed, err := parseOpenAIBatchLines(content)
		if err != nil {
			return nil, err
		}
		results = append(results, parsed...)
	}
	return results, nil
}

// CancelBatch is openai.py's cancel_batch.
func (p *OpenAIProvider) CancelBatch(ctx context.Context, providerJobID string) error {
	return p.batchCall(ctx, batchOpCancel, func(ctx context.Context) (*http.Request, error) {
		return p.jsonRequest(ctx, http.MethodPost, "/batches/"+url.PathEscape(providerJobID)+"/cancel", nil)
	}, nil)
}

func (p *OpenAIProvider) jsonRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.cfg.BaseURL+path, reader)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+p.cfg.APIKey.Reveal())
	return req, nil
}

func (p *OpenAIProvider) uploadRequest(ctx context.Context, payload []byte) (*http.Request, error) {
	var body bytes.Buffer
	form := multipart.NewWriter(&body)
	if err := form.WriteField("purpose", "batch"); err != nil {
		return nil, fmt.Errorf("encode upload: %w", err)
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", `form-data; name="file"; filename="`+openAIBatchFileName+`"`)
	header.Set("Content-Type", "application/jsonl")
	part, err := form.CreatePart(header)
	if err != nil {
		return nil, fmt.Errorf("encode upload: %w", err)
	}
	if _, err := part.Write(payload); err != nil {
		return nil, fmt.Errorf("encode upload: %w", err)
	}
	if err := form.Close(); err != nil {
		return nil, fmt.Errorf("encode upload: %w", err)
	}
	req, err := p.jsonRequest(ctx, http.MethodPost, "/files", body.Bytes())
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", form.FormDataContentType())
	return req, nil
}

// batchCall sends one Batch API request, retried once on a retryable failure
// (never the create).
// A 2xx answer that is not readable fails as the synchronous call's does.
// out is a *[]byte for the raw body, a pointer to decode JSON into, or nil.
// A failure is the classified *llmError the synchronous call returns, so a
// caller tells a deterministic failure (bad key, unknown model, quota) the
// same way.
func (p *OpenAIProvider) batchCall(ctx context.Context, operation string, build func(context.Context) (*http.Request, error), out any) error {
	ctx = withLLMBatchOperation(ctx, operation)
	retries := openAIMaxRetries
	if operation == batchOpCreate {
		// A batch create is billed and not idempotent: a retry after a lost
		// answer would start a second batch and orphan the first (openai.py
		// sent it once too, max_retries=0).
		retries = 0
	}
	var lastErr *llmError
	for attempt := 0; attempt <= retries; attempt++ {
		req, err := build(withLLMAttempt(ctx, attempt+1))
		if err != nil {
			return err
		}
		err = p.batchExchange(req, out)
		if err == nil {
			return nil
		}
		classified := classifyProviderError(err, statusCodeOf(err), headerOf(err), "openai", p.cfg.Model)
		lastErr = classified
		if isRetryable(classified) && attempt < retries {
			if !sleepForRetry(ctx, retryDelayFor(classified, attempt)) {
				return ctx.Err()
			}
			continue
		}
		return classified
	}
	return lastErr
}

func (p *OpenAIProvider) batchExchange(req *http.Request, out any) error {
	resp, err := tracedLLMDo(p.client, req, ProviderKindOpenAI, p.cfg.Model)
	if err != nil {
		return &httpTransportError{cause: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &httpTransportError{cause: err}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return &httpStatusError{statusCode: resp.StatusCode, header: resp.Header, body: string(body)}
	}
	switch target := out.(type) {
	case nil:
		return nil
	case *[]byte:
		*target = body
		return nil
	default:
		if err := json.Unmarshal(body, target); err != nil {
			markInvalidAnswer(resp)
			return logging.DecodeFailure(err)
		}
		return nil
	}
}

// mapOpenAIBatchStatus is openai.py's _map_openai_batch_status.
func mapOpenAIBatchStatus(status string) BatchJobStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "validating", "in_progress", "finalizing":
		return BatchJobRunning
	case "completed":
		return BatchJobSucceeded
	case "failed":
		return BatchJobFailed
	case "expired":
		return BatchJobExpired
	case "cancelling", "cancelled":
		return BatchJobCancelled
	}
	return BatchJobSubmitted
}

type openAIBatchOutputLine struct {
	ID       json.RawMessage `json:"id"`
	CustomID json.RawMessage `json:"custom_id"`
	Response json.RawMessage `json:"response"`
	Error    json.RawMessage `json:"error"`
}

type openAIBatchLineResponse struct {
	StatusCode json.RawMessage `json:"status_code"`
	RequestID  json.RawMessage `json:"request_id"`
	Body       json.RawMessage `json:"body"`
}

type openAIBatchLineError struct {
	Code    json.RawMessage `json:"code"`
	Type    json.RawMessage `json:"type"`
	Message json.RawMessage `json:"message"`
}

// parseOpenAIBatchLines is openai.py's _parse_openai_batch_lines. Each
// response body goes through parseResponsesBody, the parser of the
// synchronous call; the completion text is openai.py's
// `validate_json_or_empty(text) or text`. A line that is not JSON fails the
// whole file, as in Python; a line with neither an error object nor a
// response object is skipped, as in Python.
func parseOpenAIBatchLines(content []byte) ([]BatchItemResult, error) {
	var results []BatchItemResult
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 0, 64*1024), len(content)+1)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		raw := scanner.Bytes()
		if len(bytes.TrimSpace(raw)) == 0 {
			continue
		}
		var line openAIBatchOutputLine
		trimmed := bytes.TrimSpace(raw)
		if trimmed[0] != '{' {
			return nil, fmt.Errorf("categorize: batch result line %d is not a JSON object", lineNumber)
		}
		if err := json.Unmarshal(trimmed, &line); err != nil {
			return nil, fmt.Errorf("categorize: batch result line %d is not a JSON object: %w", lineNumber, err)
		}
		result := BatchItemResult{CustomID: jsonScalarText(line.CustomID), LineID: jsonScalarText(line.ID)}

		if lineErr, ok := decodeObject[openAIBatchLineError](line.Error); ok {
			code, errType := jsonScalarText(lineErr.Code), jsonScalarText(lineErr.Type)
			result.ProviderErrorCode, result.ProviderErrorType = code, errType
			result.ErrorCode = firstNonEmpty(code, errType, "error")
			result.ErrorMessage = firstNonEmpty(jsonScalarText(lineErr.Message), result.ErrorCode)
			results = append(results, result)
			continue
		}
		response, ok := decodeObject[openAIBatchLineResponse](line.Response)
		if !ok {
			continue
		}
		result.RequestID = jsonScalarText(response.RequestID)
		result.StatusCode = jsonInt(response.StatusCode)
		var bodyErr error
		if len(bytes.TrimSpace(response.Body)) > 0 {
			parsed, _, err := parseResponsesBody(response.Body)
			bodyErr = err
			if err == nil {
				result.InputTokens = parsed.inputTokens
				result.OutputTokens = parsed.outputTokens
				result.CachedInputTokens = parsed.cachedInputTokens
				if cleaned := validateJSONOrEmpty(parsed.text); cleaned != "" {
					result.Text = cleaned
				} else {
					result.Text = parsed.text
				}
			}
		}
		if result.Text == "" {
			status := "unknown"
			if result.StatusCode != nil && *result.StatusCode != 0 {
				status = strconv.Itoa(*result.StatusCode)
			}
			result.ErrorCode = "http_" + status
			result.ErrorMessage = "Batch response did not contain completion text"
			if bodyErr != nil {
				// Not swallowed: the reason rides the line's error into the
				// unit's failure.
				result.ErrorMessage += " (the body is not a Responses API body: " + logging.DecodeFailure(bodyErr).Error() + ")"
			}
		}
		results = append(results, result)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("categorize: batch result file: %w", err)
	}
	return results, nil
}

// decodeObject decodes raw when it holds a JSON object.
func decodeObject[T any](raw json.RawMessage) (T, bool) {
	var zero T
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return zero, false
	}
	var value T
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return zero, false
	}
	return value, true
}

// jsonScalarText is the text of a JSON string or number, "" for anything else.
func jsonScalarText(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	if trimmed[0] == '"' {
		var text string
		if json.Unmarshal(trimmed, &text) == nil {
			return text
		}
		return ""
	}
	var number json.Number
	if json.Unmarshal(trimmed, &number) == nil {
		return number.String()
	}
	return ""
}

func jsonInt(raw json.RawMessage) *int {
	var value int
	if json.Unmarshal(bytes.TrimSpace(raw), &value) != nil {
		return nil
	}
	return &value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
