package syncreconciler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func validRecorderObservation() Observation {
	return Observation{
		Kinds: []KindObservation{
			{Kind: frozenKinds[0], Route: "celery", DuePending: 1, ExpiredClaims: 1},
			{Kind: frozenKinds[1], Route: "celery"},
			{Kind: frozenKinds[2], Route: "river", DuePending: 1},
			{Kind: frozenKinds[3], Route: "celery"},
		},
		CeleryDuePending:  1,
		RiverDuePending:   1,
		SampledCandidates: 2,
		ObservedAt:        time.Date(2026, time.July, 22, 12, 0, 0, 123, time.UTC),
		Limit:             3,
		PredicateVersion:  PredicateVersion,
		DigestVersion:     DigestVersion,
		CandidateDigest:   "sha256:" + strings.Repeat("0", 64),
	}
}

func TestSlogRecorderEmitsExactRedactedJSON(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, attribute slog.Attr) slog.Attr {
			if len(groups) == 0 && (attribute.Key == slog.TimeKey || attribute.Key == slog.LevelKey) {
				return slog.Attr{}
			}
			return attribute
		},
	}))
	recorder, err := NewSlogObservationRecorder(logger)
	if err != nil {
		t.Fatal(err)
	}
	if !recorder.TryRecord(validRecorderObservation()) {
		t.Fatal("initial observation was dropped")
	}
	if err := recorder.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	var record map[string]any
	if err := json.Unmarshal(output.Bytes(), &record); err != nil {
		t.Fatalf("decode JSON: %v\n%s", err, output.String())
	}
	expectedKeys := []string{
		"candidate_digest", "celery_due_pending", "digest_version", "event",
		"kinds", "limit", "msg", "observed_at", "predicate_version",
		"river_due_pending", "runtime", "sampled_candidates", "truncated",
		"unknown_kind_count",
	}
	actualKeys := make([]string, 0, len(record))
	for key := range record {
		actualKeys = append(actualKeys, key)
	}
	sortStrings(actualKeys)
	if !reflect.DeepEqual(actualKeys, expectedKeys) {
		t.Fatalf("JSON keys = %v, want %v\n%s", actualKeys, expectedKeys, output.String())
	}
	for key, want := range map[string]any{
		"msg":                parityObservationEvent,
		"event":              parityObservationEvent,
		"runtime":            parityRuntime,
		"observed_at":        "2026-07-22T12:00:00.000000123Z",
		"limit":              float64(3),
		"predicate_version":  PredicateVersion,
		"digest_version":     DigestVersion,
		"candidate_digest":   "sha256:" + strings.Repeat("0", 64),
		"sampled_candidates": float64(2),
		"truncated":          false,
		"unknown_kind_count": float64(0),
		"celery_due_pending": float64(1),
		"river_due_pending":  float64(1),
	} {
		if !reflect.DeepEqual(record[key], want) {
			t.Fatalf("%s = %#v, want %#v", key, record[key], want)
		}
	}
	kinds, ok := record["kinds"].([]any)
	if !ok || len(kinds) != len(frozenKinds) {
		t.Fatalf("kinds = %#v", record["kinds"])
	}
	for index, value := range kinds {
		kind, ok := value.(map[string]any)
		if !ok || kind["kind"] != frozenKinds[index] ||
			len(kind) != 4 || kind["route"] != validRecorderObservation().Kinds[index].Route {
			t.Fatalf("kind[%d] = %#v", index, value)
		}
	}
	for _, forbidden := range []string{
		candidateID1,
		"tenant_id",
		"org_id",
		"sync_run_id",
	} {
		if strings.Contains(output.String(), forbidden) {
			t.Fatalf("redacted JSON contains %q:\n%s", forbidden, output.String())
		}
	}
}

func TestSlogRecorderCadenceIsInitialThenAtMostOncePerMinute(t *testing.T) {
	handler := &notificationHandler{handled: make(chan struct{}, 2)}
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	recorder, err := newSlogObservationRecorder(slog.New(handler), defaultRecorderCadence, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	observation := validRecorderObservation()
	if !recorder.TryRecord(observation) {
		t.Fatal("initial observation was dropped")
	}
	<-handler.handled
	if recorder.TryRecord(observation) {
		t.Fatal("same-time observation bypassed cadence")
	}
	now = now.Add(defaultRecorderCadence - time.Nanosecond)
	if recorder.TryRecord(observation) {
		t.Fatal("early observation bypassed cadence")
	}
	now = now.Add(time.Nanosecond)
	if !recorder.TryRecord(observation) {
		t.Fatal("observation at cadence boundary was dropped")
	}
	<-handler.handled
	if err := recorder.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSlogRecorderDropsMalformedObservationAndSurvivesHandlerPanic(t *testing.T) {
	var malformedOutput bytes.Buffer
	malformedRecorder, err := NewSlogObservationRecorder(slog.New(slog.NewJSONHandler(&malformedOutput, nil)))
	if err != nil {
		t.Fatal(err)
	}
	malformed := validRecorderObservation()
	malformed.Kinds = malformed.Kinds[:3]
	if malformedRecorder.TryRecord(malformed) {
		t.Fatal("malformed observation was accepted")
	}
	if err := malformedRecorder.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if malformedOutput.Len() != 0 {
		t.Fatalf("malformed observation was logged: %s", malformedOutput.String())
	}

	handler := &panicOnceHandler{
		panicked: make(chan struct{}),
		handled:  make(chan struct{}),
	}
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	recorder, err := newSlogObservationRecorder(slog.New(handler), minRecorderCadence, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if !recorder.TryRecord(validRecorderObservation()) {
		t.Fatal("panic record was dropped before handler")
	}
	<-handler.panicked
	now = now.Add(minRecorderCadence)
	if !recorder.TryRecord(validRecorderObservation()) {
		t.Fatal("worker did not survive handler panic")
	}
	<-handler.handled
	if err := recorder.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSlogRecorderRejectsInvalidConstructionAndFixedShapes(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	for _, construct := range []func() error{
		func() error {
			_, err := newSlogObservationRecorder(nil, defaultRecorderCadence, time.Now)
			return err
		},
		func() error {
			_, err := newSlogObservationRecorder(logger, minRecorderCadence-time.Nanosecond, time.Now)
			return err
		},
		func() error {
			_, err := newSlogObservationRecorder(logger, maxRecorderCadence+time.Nanosecond, time.Now)
			return err
		},
	} {
		if err := construct(); err == nil {
			t.Fatal("invalid recorder construction succeeded")
		}
	}

	for name, mutate := range map[string]func(*Observation){
		"unsorted kinds": func(observation *Observation) {
			observation.Kinds[0], observation.Kinds[1] = observation.Kinds[1], observation.Kinds[0]
		},
		"route totals": func(observation *Observation) {
			observation.CeleryDuePending++
		},
		"expired exceeds due": func(observation *Observation) {
			observation.Kinds[0].ExpiredClaims = observation.Kinds[0].DuePending + 1
		},
		"wrong version": func(observation *Observation) {
			observation.DigestVersion = "future"
		},
	} {
		t.Run(name, func(t *testing.T) {
			observation := validRecorderObservation()
			mutate(&observation)
			if validRecordedObservation(observation) {
				t.Fatalf("malformed observation was valid: %#v", observation)
			}
		})
	}
}

func TestSlogRecorderConcurrentOffersAreRaceSafeAndBounded(t *testing.T) {
	handler := &notificationHandler{handled: make(chan struct{}, 1)}
	recorder, err := newSlogObservationRecorder(
		slog.New(handler),
		defaultRecorderCadence,
		func() time.Time { return time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC) },
	)
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int64
	var wait sync.WaitGroup
	for range 100 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if recorder.TryRecord(validRecorderObservation()) {
				accepted.Add(1)
			}
		}()
	}
	wait.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("accepted offers = %d, want 1", accepted.Load())
	}
	<-handler.handled
	if err := recorder.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if recorder.TryRecord(validRecorderObservation()) {
		t.Fatal("post-shutdown observation was accepted")
	}
}

func TestSlogRecorderBusyHandlerUsesBoundedQueueAndDrops(t *testing.T) {
	handler := &blockingOnceHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		handled: make(chan struct{}, 2),
	}
	now := time.Date(2026, time.July, 22, 12, 0, 0, 0, time.UTC)
	recorder, err := newSlogObservationRecorder(slog.New(handler), minRecorderCadence, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	observation := validRecorderObservation()
	if !recorder.TryRecord(observation) {
		t.Fatal("initial observation was dropped")
	}
	<-handler.entered
	now = now.Add(minRecorderCadence)
	if !recorder.TryRecord(observation) {
		t.Fatal("single queued observation was dropped")
	}
	now = now.Add(minRecorderCadence)
	if recorder.TryRecord(observation) {
		t.Fatal("full bounded queue accepted another observation")
	}
	close(handler.release)
	<-handler.handled
	<-handler.handled
	if err := recorder.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSlogRecorderShutdownIsContextBoundedWhenHandlerBlocks(t *testing.T) {
	handler := &blockingOnceHandler{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		handled: make(chan struct{}, 1),
	}
	recorder, err := NewSlogObservationRecorder(slog.New(handler))
	if err != nil {
		t.Fatal(err)
	}
	if !recorder.TryRecord(validRecorderObservation()) {
		t.Fatal("initial observation was dropped")
	}
	<-handler.entered

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := recorder.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown() error = %v, want cancellation", err)
	}
	close(handler.release)
	<-handler.handled
	if err := recorder.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type notificationHandler struct {
	handled chan struct{}
}

func (*notificationHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler *notificationHandler) Handle(context.Context, slog.Record) error {
	handler.handled <- struct{}{}
	return nil
}
func (handler *notificationHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *notificationHandler) WithGroup(string) slog.Handler      { return handler }

type panicOnceHandler struct {
	once     sync.Once
	panicked chan struct{}
	handled  chan struct{}
}

func (*panicOnceHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler *panicOnceHandler) Handle(context.Context, slog.Record) error {
	didPanic := false
	handler.once.Do(func() {
		didPanic = true
		close(handler.panicked)
	})
	if didPanic {
		panic("handler panic")
	}
	close(handler.handled)
	return nil
}
func (handler *panicOnceHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *panicOnceHandler) WithGroup(string) slog.Handler      { return handler }

type blockingOnceHandler struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
	handled chan struct{}
}

func (*blockingOnceHandler) Enabled(context.Context, slog.Level) bool { return true }
func (handler *blockingOnceHandler) Handle(context.Context, slog.Record) error {
	handler.once.Do(func() {
		close(handler.entered)
		<-handler.release
	})
	handler.handled <- struct{}{}
	return nil
}
func (handler *blockingOnceHandler) WithAttrs([]slog.Attr) slog.Handler { return handler }
func (handler *blockingOnceHandler) WithGroup(string) slog.Handler      { return handler }

func sortStrings(values []string) {
	for index := 1; index < len(values); index++ {
		for current := index; current > 0 && values[current] < values[current-1]; current-- {
			values[current], values[current-1] = values[current-1], values[current]
		}
	}
}
