package decisioneval

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Replayed is a classification recomputed offline from stored raw responses.
type Replayed struct {
	State    string
	Status   string
	Outcome  categorize.CategorizationOutcome
	Interp   *Interpretation
	Terminal *DecisionTerminal
}

// storedAttempt is one stored raw response read back from disk.
type storedAttempt struct {
	rec     AttemptRecord
	status  int
	header  http.Header
	body    []byte
	errText string // set for a transport error (no response)
}

func readStored(dir string, a AttemptRecord) (storedAttempt, error) {
	s := storedAttempt{rec: a, header: http.Header{}}
	if a.RawHeadersPath != "" {
		data, err := os.ReadFile(filepath.Join(dir, a.RawHeadersPath))
		if err != nil {
			return s, fmt.Errorf("replay: attempt %s: raw headers: %w", a.AttemptID, err)
		}
		sc := bufio.NewScanner(bytes.NewReader(data))
		first := true
		for sc.Scan() {
			line := sc.Text()
			if first {
				first = false
				switch {
				case strings.HasPrefix(line, "STATUS "):
					s.status, _ = strconv.Atoi(strings.TrimPrefix(line, "STATUS "))
				case strings.HasPrefix(line, "ERROR "):
					s.errText = strings.TrimPrefix(line, "ERROR ")
				}
				continue
			}
			if name, val, ok := strings.Cut(line, ": "); ok {
				s.header.Add(name, val)
			}
		}
	}
	if a.RawResponsePath != "" {
		body, err := os.ReadFile(filepath.Join(dir, a.RawResponsePath))
		if err != nil {
			return s, fmt.Errorf("replay: attempt %s: raw response: %w", a.AttemptID, err)
		}
		s.body = body
	}
	return s, nil
}

// ReplaySender serves the stored final response of a classification: the last
// attempt (a retry follows a failed attempt, so the last one is the outcome).
type ReplaySender struct {
	Dir      string
	Attempts []AttemptRecord
	// Err is set when a stored file could not be read: a replay that cannot
	// read its input must fail, not look like a failed request.
	Err error
}

// Send returns the stored result.
func (r *ReplaySender) Send(_ context.Context, _ Backend, _ BuiltRequest) SendResult {
	if len(r.Attempts) == 0 {
		r.Err = errors.New("replay: classification has no attempts")
		return SendResult{Fail: &SendFailure{Class: "replay_no_attempts"}}
	}
	last := r.Attempts[len(r.Attempts)-1]
	s, err := readStored(r.Dir, last)
	if err != nil {
		r.Err = err
		return SendResult{Fail: &SendFailure{Class: "replay_missing_raw"}}
	}
	switch last.AttemptState {
	case AttemptHTTPOK:
		return SendResult{Status: 200, Body: s.body, Header: s.header}
	case AttemptHTTPError:
		f := &SendFailure{Class: last.ErrorClass}
		if last.HTTPStatus == 401 || last.HTTPStatus == 402 || last.HTTPStatus == 403 {
			f.StopArm = "auth_" + strconv.Itoa(last.HTTPStatus)
		}
		return SendResult{Status: last.HTTPStatus, Body: s.body, Header: s.header, Fail: f}
	default:
		f := &SendFailure{Class: last.ErrorClass}
		if last.AttemptState == AttemptBudgetRefused {
			f.StopArm = "budget"
		}
		return SendResult{Fail: f}
	}
}

// replayTransport serves stored responses in attempt order to the unchanged
// incumbent provider, so extraction of the completion text runs the production
// code.
type replayTransport struct {
	dir  string
	list []AttemptRecord
	i    int
}

func (t *replayTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.i >= len(t.list) {
		return nil, errors.New("replay: the provider sent more requests than were recorded")
	}
	a := t.list[t.i]
	t.i++
	s, err := readStored(t.dir, a)
	if err != nil {
		return nil, err
	}
	if s.errText != "" || (a.AttemptState != AttemptHTTPOK && a.AttemptState != AttemptHTTPError) {
		text := s.errText
		if text == "" {
			text = a.ErrorClass
		}
		return nil, errors.New(text)
	}
	return &http.Response{StatusCode: s.status, Header: s.header, Body: io.NopCloser(bytes.NewReader(s.body)), Request: req}, nil
}

// ReplayClassification recomputes the outcome of a recorded classification from
// the stored raw responses. No network is used. weights selects the support
// map for a candidate arm.
func ReplayClassification(ctx context.Context, r *Rubric, weights []float64, rec ClassificationRecord, data *LedgerData, bundle units.TextBundle) (Replayed, error) {
	if rec.Gate != "" {
		o := categorize.FallbackOutcome(rec.Gate)
		return Replayed{State: rec.Gate, Status: rec.Gate, Outcome: o}, nil
	}
	attempts := data.AttemptsOf(rec)
	switch rec.Arm {
	case ArmJev, ArmDecisions:
		var b Backend
		if rec.Arm == ArmJev {
			b = NewJevBackend(rec.endpointOf(data), secrets.Hidden{}, rec.ModelRequested)
		} else {
			b = NewDecisionsBackend(rec.endpointOf(data), secrets.Hidden{}, rec.ModelRequested)
		}
		b.AcceptedModels = rec.AcceptedModels
		sender := &ReplaySender{Dir: data.Dir, Attempts: attempts}
		c, err := DecisionCategorize(ctx, bundle, DecisionDeps{Rubric: r, Weights: weights, Backend: b, Sender: sender})
		if err != nil {
			return Replayed{}, err
		}
		if sender.Err != nil {
			return Replayed{}, sender.Err
		}
		return Replayed{State: c.State, Status: c.Outcome.Status, Outcome: c.Outcome, Interp: c.Interp, Terminal: c.Terminal}, nil
	default:
		list := append([]AttemptRecord(nil), attempts...)
		sort.SliceStable(list, func(i, j int) bool { return list[i].Attempt < list[j].Attempt })
		client := &http.Client{Transport: &replayTransport{dir: data.Dir, list: list}}
		cfg := IncumbentConfig{APIKey: secrets.NewHidden("replay"), Model: rec.ModelRequested}
		outcome, err := IncumbentCategorize(ctx, bundle, cfg, client, nil)
		if err != nil {
			if strings.Contains(err.Error(), "more requests than were recorded") {
				return Replayed{}, err
			}
			return Replayed{State: categorize.StatusLLMTaskFailed, Status: categorize.StatusLLMTaskFailed,
				Outcome: categorize.FallbackOutcome(categorize.StatusLLMTaskFailed)}, nil
		}
		return Replayed{State: outcome.Status, Status: outcome.Status, Outcome: outcome}, nil
	}
}

func (c ClassificationRecord) endpointOf(data *LedgerData) string {
	for _, a := range data.Attempts {
		for _, id := range c.AttemptIDs {
			if a.AttemptID == id && a.Endpoint != "" {
				return a.Endpoint
			}
		}
	}
	return ""
}
