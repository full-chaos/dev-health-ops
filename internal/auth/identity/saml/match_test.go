package saml

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

type flowsOnly struct {
	consumed int
	result   error
}

func (f *flowsOnly) SaveFlow(context.Context, Flow) error { return nil }

func (f *flowsOnly) ConsumeFlow(context.Context, uuid.UUID, string, [32]byte, time.Time) error {
	f.consumed++
	return f.result
}

func TestSAMLMatchRequestBindsEveryResponseThatNamesARequest(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	noCookie := func(string) string { return "" }
	withCookie := func(string) string { return "nonce" }
	for _, allow := range []bool{false, true} {
		p := Provider{ID: uuid.New(), AllowIdPInitiated: allow}
		for name, tc := range map[string]struct {
			cookie func(string) string
			flows  *flowsOnly
			want   error
		}{
			"no cookie":                  {cookie: noCookie, flows: &flowsOnly{}, want: ErrFlowNotFound},
			"cookie, no matching flow":   {cookie: withCookie, flows: &flowsOnly{result: ErrFlowNotFound}, want: ErrFlowNotFound},
			"cookie, consumed flow":      {cookie: withCookie, flows: &flowsOnly{result: ErrFlowConsumed}, want: ErrFlowConsumed},
			"cookie, open matching flow": {cookie: withCookie, flows: &flowsOnly{}, want: nil},
		} {
			s := &Service{Flows: tc.flows}
			err := s.matchRequest(ctx, p, Verified{InResponseTo: "_id1"}, tc.cookie, now)
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Errorf("allow_idp_initiated=%v %s: err = %v, want %v", allow, name, err, tc.want)
			}
		}
		s := &Service{Flows: &flowsOnly{}}
		err := s.matchRequest(ctx, p, Verified{}, noCookie, now)
		if allow && err != nil {
			t.Errorf("allow_idp_initiated=true, no InResponseTo: err = %v, want accepted", err)
		}
		if !allow && !errors.Is(err, ErrUnsolicited) {
			t.Errorf("allow_idp_initiated=false, no InResponseTo: err = %v, want ErrUnsolicited", err)
		}
	}
}
