package syncdispatchruntime

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
	"github.com/full-chaos/dev-health-ops/internal/syncroute"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

var (
	_ Args = DispatchSyncRunArgs{}
	_ Args = FinalizeSyncRunArgs{}
	_ Args = PostSyncArgs{}
	_ Args = ReferenceDiscoveryArgs{}
)

const (
	testOutbox = "10000000-0000-4000-8000-000000000001"
	testOrg    = "20000000-0000-4000-8000-000000000002"
	testRun    = "30000000-0000-4000-8000-000000000003"
)

func TestConvertProducesExactVersionedTypedArgsWithoutClaimToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		kind string
		want any
	}{
		{syncdispatchcontract.KindDispatchSyncRun, DispatchSyncRunArgs{}},
		{syncdispatchcontract.KindFinalizeSyncRun, FinalizeSyncRunArgs{}},
		{syncdispatchcontract.KindPostSync, PostSyncArgs{}},
		{syncdispatchcontract.KindReferenceDiscovery, ReferenceDiscoveryArgs{}},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			args, err := Convert(Claim{OutboxID: testOutbox, Kind: test.kind, RouteGeneration: 9}, testReference())
			if err != nil {
				t.Fatal(err)
			}
			if reflect.TypeOf(args) != reflect.TypeOf(test.want) || args.Kind() != test.kind ||
				args.ContractVersion() != ContractVersionV1 || args.OutboxID() != testOutbox ||
				args.OrganizationID() != testOrg || args.SyncRunID() != testRun || args.RouteGeneration() != 9 {
				t.Fatalf("converted args = %#v", args)
			}
			encoded, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 5 || fields["contract_version"] != float64(1) || fields["organization_id"] != testOrg ||
				fields["sync_run_id"] != testRun || fields["outbox_id"] != testOutbox ||
				fields["route_generation"] != float64(9) {
				t.Fatalf("encoded fields = %#v", fields)
			}
			if _, leaked := fields["claim_token"]; leaked {
				t.Fatal("claim token leaked into River arguments")
			}
		})
	}
}

func TestConvertFailsClosedForInvalidClaimAndDomainReference(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		claim     Claim
		reference DomainReference
		want      error
	}{
		{"unknown kind", Claim{OutboxID: testOutbox, Kind: "not_a_kind", RouteGeneration: 1}, testReference(), ErrInvalidClaim},
		{"non uuid outbox", Claim{OutboxID: "unsafe", Kind: syncdispatchcontract.KindPostSync, RouteGeneration: 1}, testReference(), ErrInvalidClaim},
		{"zero generation", Claim{OutboxID: testOutbox, Kind: syncdispatchcontract.KindPostSync}, testReference(), ErrInvalidClaim},
		{"non uuid org", Claim{OutboxID: testOutbox, Kind: syncdispatchcontract.KindPostSync, RouteGeneration: 1}, DomainReference{OrganizationID: "unsafe", SyncRunID: testRun}, ErrInvalidReference},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Convert(test.claim, test.reference)
			if !errors.Is(err, test.want) {
				t.Fatalf("Convert() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestPublisherUsesCallerTransactionAndVerifiesExactArgs(t *testing.T) {
	t.Parallel()
	client := &recordingInsertClient{}
	publisher, err := NewPublisher(client, PublisherOptions{Queue: "sync", MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	jobID, err := publisher.Publish(context.Background(), &inertTx{}, Claim{
		OutboxID: testOutbox, Kind: syncdispatchcontract.KindDispatchSyncRun, RouteGeneration: 3,
	}, testReference())
	if err != nil {
		t.Fatal(err)
	}
	if jobID != "42" || client.tx == nil || client.args == nil || client.args.Kind() != syncdispatchcontract.KindDispatchSyncRun ||
		client.options.Queue != "sync" || client.options.MaxAttempts != 5 || !client.options.UniqueOpts.ByArgs {
		t.Fatalf("publisher call = job:%s tx:%T args:%#v opts:%#v", jobID, client.tx, client.args, client.options)
	}
}

func TestPublisherRejectsMismatchedRiverResultWithoutLeakingArguments(t *testing.T) {
	t.Parallel()
	client := &recordingInsertClient{mutate: func(job *rivertype.JobRow) { job.Queue = "other" }}
	publisher, err := NewPublisher(client, PublisherOptions{Queue: "sync", MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	_, err = publisher.Publish(context.Background(), &inertTx{}, Claim{
		OutboxID: testOutbox, Kind: syncdispatchcontract.KindFinalizeSyncRun, RouteGeneration: 3,
	}, testReference())
	if !errors.Is(err, ErrInsertRejected) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrInsertRejected)
	}
}

func TestPublisherRejectsTypedNilClientAndMissingReturnedArgs(t *testing.T) {
	t.Parallel()
	var typedNil *recordingInsertClient
	if _, err := NewPublisher(typedNil, PublisherOptions{Queue: "sync", MaxAttempts: 5}); !errors.Is(err, ErrInvalidPublisher) {
		t.Fatalf("NewPublisher(typed nil) error = %v, want %v", err, ErrInvalidPublisher)
	}
	validPublisher := mustPublisher(t)
	var typedNilTx *inertTx
	if _, err := validPublisher.Publish(context.Background(), typedNilTx, Claim{
		OutboxID: testOutbox, Kind: syncdispatchcontract.KindFinalizeSyncRun, RouteGeneration: 3,
	}, testReference()); !errors.Is(err, ErrInvalidPublisher) {
		t.Fatalf("Publish(typed nil transaction) error = %v, want %v", err, ErrInvalidPublisher)
	}
	publisher, err := NewPublisher(&recordingInsertClient{mutate: func(job *rivertype.JobRow) { job.EncodedArgs = nil }}, PublisherOptions{Queue: "sync", MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	_, err = publisher.Publish(context.Background(), &inertTx{}, Claim{
		OutboxID: testOutbox, Kind: syncdispatchcontract.KindFinalizeSyncRun, RouteGeneration: 3,
	}, testReference())
	if !errors.Is(err, ErrInsertRejected) {
		t.Fatalf("Publish() error = %v, want %v", err, ErrInsertRejected)
	}
}

func TestCapabilitiesCannotAdvertiseWithoutEveryConcreteHandler(t *testing.T) {
	t.Parallel()
	publisher := mustPublisher(t)
	if _, err := NewCapabilities(&Publisher{}, completeHandlers(), NewGenerationTracker()); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("NewCapabilities(zero publisher) error = %v, want %v", err, ErrCapabilityUnavailable)
	}
	if _, err := NewCapabilities(publisher, completeHandlers(), &GenerationTracker{}); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("NewCapabilities(zero tracker) error = %v, want %v", err, ErrCapabilityUnavailable)
	}
	if _, err := NewCapabilities(publisher, Handlers{}, NewGenerationTracker()); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("NewCapabilities() error = %v, want %v", err, ErrCapabilityUnavailable)
	}
	var nilHandler DispatchSyncRunHandlerFunc
	if _, err := NewCapabilities(publisher, Handlers{DispatchSyncRun: nilHandler}, NewGenerationTracker()); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("NewCapabilities(nil typed handler) error = %v, want %v", err, ErrCapabilityUnavailable)
	}

	capabilities, err := NewCapabilities(publisher, completeHandlers(), NewGenerationTracker())
	if err != nil {
		t.Fatal(err)
	}
	descriptors := capabilities.Descriptors()
	if len(descriptors) != 4 || descriptors[0].Kind != syncdispatchcontract.KindDispatchSyncRun ||
		!descriptors[0].HandlerRegistered || !descriptors[0].PublisherBound {
		t.Fatalf("descriptors = %#v", descriptors)
	}
	withoutLegacyHandoff := completeHandlers()
	withoutLegacyHandoff.PostSyncHandoff = nil
	capabilities, err = NewCapabilities(publisher, withoutLegacyHandoff, NewGenerationTracker())
	if err != nil || !capabilities.Descriptors()[2].PublisherBound {
		t.Fatalf("post_sync must be publishable without legacy handoff: %#v / %v", capabilities, err)
	}
}

func TestRouteCapabilitiesMatchTheRegisteredAtLeastOnceWorkers(t *testing.T) {
	t.Parallel()
	capabilities, err := syncroute.NewCapabilities(RouteCapabilities())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{
		syncdispatchcontract.KindDispatchSyncRun,
		syncdispatchcontract.KindFinalizeSyncRun,
		syncdispatchcontract.KindReferenceDiscovery,
	} {
		if _, ok := capabilities.Lookup(kind, syncdispatchcontract.RouteRiver); !ok {
			t.Fatalf("missing River capability for %s", kind)
		}
	}
	if _, ok := capabilities.Lookup(syncdispatchcontract.KindPostSync, syncdispatchcontract.RouteRiver); ok {
		t.Fatal("post_sync capability advertised before its worker is registered")
	}
}

func TestCapabilitiesRetainAndDispatchEveryTypedHandler(t *testing.T) {
	t.Parallel()
	called := make(map[string]bool)
	handlers := completeHandlers()
	handlers.DispatchSyncRun = DispatchSyncRunHandlerFunc(func(context.Context, DispatchSyncRunArgs) error { called["dispatch"] = true; return nil })
	handlers.FinalizeSyncRun = FinalizeSyncRunHandlerFunc(func(context.Context, FinalizeSyncRunArgs) error { called["finalize"] = true; return nil })
	handlers.PostSync = PostSyncHandlerFunc(func(context.Context, PostSyncArgs) error { called["post"] = true; return nil })
	handlers.ReferenceDiscovery = ReferenceDiscoveryHandlerFunc(func(context.Context, ReferenceDiscoveryArgs) error { called["reference"] = true; return nil })
	capabilities, err := NewCapabilities(mustPublisher(t), handlers, NewGenerationTracker())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		kind string
		run  func(Args) error
	}{
		{syncdispatchcontract.KindDispatchSyncRun, func(args Args) error {
			return capabilities.DispatchSyncRun(context.Background(), args.(DispatchSyncRunArgs))
		}},
		{syncdispatchcontract.KindFinalizeSyncRun, func(args Args) error {
			return capabilities.FinalizeSyncRun(context.Background(), args.(FinalizeSyncRunArgs))
		}},
		{syncdispatchcontract.KindPostSync, func(args Args) error { return capabilities.PostSync(context.Background(), args.(PostSyncArgs)) }},
		{syncdispatchcontract.KindReferenceDiscovery, func(args Args) error {
			return capabilities.ReferenceDiscovery(context.Background(), args.(ReferenceDiscoveryArgs))
		}},
	} {
		args, err := Convert(Claim{OutboxID: testOutbox, Kind: test.kind, RouteGeneration: 1}, testReference())
		if err != nil || test.run(args) != nil {
			t.Fatalf("dispatch %s failed: conversion=%v", test.kind, err)
		}
	}
	if !called["dispatch"] || !called["finalize"] || !called["post"] || !called["reference"] {
		t.Fatalf("typed handler calls = %#v", called)
	}
}

func TestGenerationTrackerIsLocalOnlyAndWaitsForLocalHandoffs(t *testing.T) {
	t.Parallel()
	tracker := NewGenerationTracker()
	if _, isCrossProcessBarrier := any(tracker).(syncroute.Quiescer); isCrossProcessBarrier {
		t.Fatal("GenerationTracker must not implement syncroute.Quiescer")
	}
	leave, err := tracker.EnterLocalHandoff(7)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err = tracker.WaitForLocalHandoffs(cancelled, 7)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled WaitForLocalHandoffs() error = %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- tracker.WaitForLocalHandoffs(context.Background(), 7)
	}()
	select {
	case err := <-done:
		t.Fatalf("WaitForLocalHandoffs returned before leave: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if _, err := tracker.EnterLocalHandoff(7); !errors.Is(err, ErrGenerationClosed) {
		t.Fatalf("EnterLocalHandoff old generation error = %v, want %v", err, ErrGenerationClosed)
	}
	leave()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForLocalHandoffs() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForLocalHandoffs did not release after old handoff left")
	}
	if leaveNew, err := tracker.EnterLocalHandoff(8); err != nil {
		t.Fatalf("EnterLocalHandoff new generation error = %v", err)
	} else {
		leaveNew()
	}
}

func TestPostSyncHandoffIsPairedWithGenerationTracker(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	handlers := completeHandlers()
	handlers.PostSyncHandoff = postSyncHandoffFunc(func(context.Context, PostSyncArgs) error {
		close(entered)
		<-release
		return nil
	})
	capabilities, err := NewCapabilities(mustPublisher(t), handlers, NewGenerationTracker())
	if err != nil {
		t.Fatal(err)
	}
	args, err := Convert(Claim{OutboxID: testOutbox, Kind: syncdispatchcontract.KindPostSync, RouteGeneration: 4}, testReference())
	if err != nil {
		t.Fatal(err)
	}
	postSyncArgs := args.(PostSyncArgs)
	done := make(chan error, 1)
	go func() { done <- capabilities.HandoffPostSync(context.Background(), postSyncArgs) }()
	<-entered
	quiesced := make(chan error, 1)
	go func() {
		quiesced <- capabilities.quiescer.WaitForLocalHandoffs(context.Background(), 4)
	}()
	select {
	case err := <-quiesced:
		t.Fatalf("WaitForLocalHandoffs returned before handoff left: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("HandoffPostSync() error = %v", err)
	}
	if err := <-quiesced; err != nil {
		t.Fatalf("WaitForLocalHandoffs() error = %v", err)
	}
}

func testReference() DomainReference {
	return DomainReference{OrganizationID: testOrg, SyncRunID: testRun}
}

type inertTx struct{ pgx.Tx }

type recordingInsertClient struct {
	tx      pgx.Tx
	args    river.JobArgs
	options *river.InsertOpts
	mutate  func(*rivertype.JobRow)
}

func (client *recordingInsertClient) InsertTx(_ context.Context, tx pgx.Tx, args river.JobArgs, options *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	client.tx = tx
	client.args = args
	copyOptions := *options
	client.options = &copyOptions
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	job := &rivertype.JobRow{ID: 42, Kind: args.Kind(), Queue: options.Queue, MaxAttempts: options.MaxAttempts, EncodedArgs: encoded}
	if client.mutate != nil {
		client.mutate(job)
	}
	return &rivertype.JobInsertResult{Job: job}, nil
}

func mustPublisher(t *testing.T) *Publisher {
	t.Helper()
	publisher, err := NewPublisher(&recordingInsertClient{}, PublisherOptions{Queue: "sync", MaxAttempts: 5})
	if err != nil {
		t.Fatal(err)
	}
	return publisher
}

func completeHandlers() Handlers {
	return Handlers{
		DispatchSyncRun:    DispatchSyncRunHandlerFunc(func(context.Context, DispatchSyncRunArgs) error { return nil }),
		FinalizeSyncRun:    FinalizeSyncRunHandlerFunc(func(context.Context, FinalizeSyncRunArgs) error { return nil }),
		PostSync:           PostSyncHandlerFunc(func(context.Context, PostSyncArgs) error { return nil }),
		ReferenceDiscovery: ReferenceDiscoveryHandlerFunc(func(context.Context, ReferenceDiscoveryArgs) error { return nil }),
		PostSyncHandoff:    postSyncHandoffFunc(func(context.Context, PostSyncArgs) error { return nil }),
	}
}

type postSyncHandoffFunc func(context.Context, PostSyncArgs) error

func (function postSyncHandoffFunc) Handoff(ctx context.Context, args PostSyncArgs) error {
	return function(ctx, args)
}
