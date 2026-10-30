package local

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/callseq"
)

type recordingLocker struct {
	Locker
	rec *flowRecorder
}

func (l recordingLocker) WithPrincipalLock(ctx context.Context, id uuid.UUID, fn func(context.Context) error) error {
	l.rec.add("Lock")
	return l.Locker.WithPrincipalLock(ctx, id, fn)
}

// lateRig records the principal lock and the password hash next to the store calls.
func lateRig(t *testing.T) *flowRig {
	t.Helper()
	rig := newFlowRig(t, true)
	rig.f.locker = recordingLocker{Locker: rig.locker, rec: rig.rec}
	rig.f.hash = func(p string) (string, error) {
		rig.rec.add("Hash")
		return "hash-of:" + p, nil
	}
	return rig
}

// A reset link that a concurrent write uses or replaces after the first read
// answers exactly as an unknown link; its only extra steps are the lock, the
// hash and the one refused write, all inside the lock.
func TestResetLateRefusalIsTheUnknownAnswerAfterOneWriteAttempt(t *testing.T) {
	const newPassword = "A-new-pass-77"
	unknownRig := lateRig(t)
	unknownOut, err := unknownRig.f.Reset(context.Background(), "no-such-link", newPassword)
	if err != nil || unknownOut.Kind != FlowInvalidToken {
		t.Fatalf("unknown: Reset = %+v, %v", unknownOut, err)
	}
	unknown := unknownRig.rec.names()
	if want := []string{"PeekResetLink"}; !reflect.DeepEqual(unknown, want) {
		t.Fatalf("unknown link calls = %v, want %v", unknown, want)
	}
	unknownBytes, err := json.Marshal(unknownOut)
	if err != nil {
		t.Fatal(err)
	}

	digest := sha256.Sum256([]byte("late-link"))
	late := map[string][]string{}
	for name, window := range map[string]func(r *flowRig, id uuid.UUID){
		"used-first": func(r *flowRig, _ uuid.UUID) { delete(r.links.redeem, digest) },
		"reissued": func(r *flowRig, id uuid.UUID) {
			delete(r.links.redeem, digest)
			r.grant("late-link-reissued", id)
		},
	} {
		rig := lateRig(t)
		u := rig.user("late@example.com", true)
		rig.grant("late-link", u.ID)
		rig.accounts.afterPeek = func() { window(rig, u.ID) }
		out, err := rig.f.Reset(context.Background(), "late-link", newPassword)
		if err != nil {
			t.Fatalf("%s: Reset error %v, want the invalid-token answer", name, err)
		}
		outBytes, err := json.Marshal(out)
		if err != nil {
			t.Fatal(err)
		}
		if string(outBytes) != string(unknownBytes) || !reflect.DeepEqual(out, unknownOut) {
			t.Errorf("%s: answer %s, want the unknown link's %s", name, outBytes, unknownBytes)
		}
		if len(rig.accounts.hashes) != 0 || rig.accounts.outside != 0 {
			t.Errorf("%s: %d password writes, %d writes outside the lock; want none", name, len(rig.accounts.hashes), rig.accounts.outside)
		}
		late[name] = rig.rec.names()
		if want := []string{"PeekResetLink", "Lock", "Hash", "SetPasswordEndingSessions"}; !reflect.DeepEqual(late[name], want) {
			t.Errorf("%s: calls = %v, want %v", name, late[name], want)
		}
		callseq.Delta(t, name+" vs unknown", unknown, late[name], []string{"Lock", "Hash", "SetPasswordEndingSessions"})

		rig.accounts.afterPeek = nil
		before := len(rig.rec.names())
		again, err := rig.f.Reset(context.Background(), "late-link", newPassword)
		if err != nil || !reflect.DeepEqual(again, unknownOut) {
			t.Errorf("%s: replay = %+v, %v", name, again, err)
		}
		if got := rig.rec.names()[before:]; !reflect.DeepEqual(got, unknown) {
			t.Errorf("%s: replay calls = %v, want the unknown link's %v", name, got, unknown)
		}
	}
	callseq.Same(t, "late reset refusals", late)
}
