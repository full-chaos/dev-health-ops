package report

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

func TestExecuteStoresOneArtifactAndNotification(t *testing.T) {
	store := &fakeRunStore{claim: true, complete: true, notificationClaim: true}
	dependencies := Dependencies{
		Runs:  store,
		Query: queryFunc(func(context.Context, QueryInput) (QueryResult, error) { return QueryResult{}, nil }),
		Renderer: rendererFunc(func(context.Context, QueryResult) (Artifact, error) {
			return Artifact{Markdown: "# report", Fingerprint: "sha256:abc"}, nil
		}),
		Artifacts:     artifactFunc(func(_ context.Context, _ string, artifact Artifact) (Artifact, error) { return artifact, nil }),
		Notifications: notificationFunc(func(context.Context, string, string) error { return nil }),
	}
	err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", dependencies)
	if err != nil || store.completed != 1 || store.notificationsCompleted != 1 {
		t.Fatalf("execute err=%v completed=%d notifications=%d", err, store.completed, store.notificationsCompleted)
	}
}

func TestExecuteDuplicateOrCancelledClaimDoesNothing(t *testing.T) {
	store := &fakeRunStore{}
	err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", Dependencies{
		Runs: store, Query: queryFunc(nil), Renderer: rendererFunc(nil), Artifacts: artifactFunc(nil), Notifications: notificationFunc(nil),
	})
	if err != nil || store.completed != 0 {
		t.Fatalf("duplicate execution err=%v completed=%d", err, store.completed)
	}
}

func TestExecuteNotificationFailureReleasesClaim(t *testing.T) {
	store := &fakeRunStore{claim: true, complete: true, notificationClaim: true}
	err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", Dependencies{
		Runs:          store,
		Query:         queryFunc(func(context.Context, QueryInput) (QueryResult, error) { return QueryResult{}, nil }),
		Renderer:      rendererFunc(func(context.Context, QueryResult) (Artifact, error) { return Artifact{Fingerprint: "sha256:abc"}, nil }),
		Artifacts:     artifactFunc(func(_ context.Context, _ string, artifact Artifact) (Artifact, error) { return artifact, nil }),
		Notifications: notificationFunc(func(context.Context, string, string) error { return errors.New("offline") }),
	})
	if err == nil || store.notificationsReleased != 1 {
		t.Fatalf("err=%v released=%d", err, store.notificationsReleased)
	}
}

func reportEnvelope() jobcontract.Envelope {
	return jobcontract.Envelope{Domain: jobcontract.DomainLink{Type: "report_run", ID: "00000000-0000-4000-8000-000000000001"}}
}

type fakeRunStore struct {
	claim, complete, notificationClaim                       bool
	completed, notificationsCompleted, notificationsReleased int
}

func (store *fakeRunStore) Claim(context.Context, string, string) (bool, error) {
	return store.claim, nil
}
func (store *fakeRunStore) Complete(context.Context, string, Artifact) (bool, error) {
	store.completed++
	return store.complete, nil
}
func (store *fakeRunStore) Fail(context.Context, string, string) error { return nil }
func (store *fakeRunStore) ClaimNotification(context.Context, string) (string, bool, error) {
	return "report.ready:1", store.notificationClaim, nil
}
func (store *fakeRunStore) CompleteNotification(context.Context, string) error {
	store.notificationsCompleted++
	return nil
}
func (store *fakeRunStore) ReleaseNotification(context.Context, string) error {
	store.notificationsReleased++
	return nil
}

type queryFunc func(context.Context, QueryInput) (QueryResult, error)

func (fn queryFunc) Query(ctx context.Context, input QueryInput) (QueryResult, error) {
	return fn(ctx, input)
}

type rendererFunc func(context.Context, QueryResult) (Artifact, error)

func (fn rendererFunc) Render(ctx context.Context, input QueryResult) (Artifact, error) {
	return fn(ctx, input)
}

type artifactFunc func(context.Context, string, Artifact) (Artifact, error)

func (fn artifactFunc) Store(ctx context.Context, id string, artifact Artifact) (Artifact, error) {
	return fn(ctx, id, artifact)
}

type notificationFunc func(context.Context, string, string) error

func (fn notificationFunc) Notify(ctx context.Context, reportID, key string) error {
	return fn(ctx, reportID, key)
}
