package analytics

import (
	"context"
	"strconv"
	"strings"
	"sync/atomic"

	"golang.org/x/sync/singleflight"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// inflight lets concurrent callers that ask the same question share one run of
// it. Nothing is kept after the run ends: it is not a cache.
//
// Every caller gets its own copy of the result (clone). A waiter whose own
// context ends leaves alone. A waiter whose shared run failed, for any reason
// including the leader's own cancel, runs its own call once without sharing, so
// it never returns another caller's error.
type inflight[T any] struct {
	group singleflight.Group
}

func (f *inflight[T]) do(ctx context.Context, key string, clone func(T) T, run func(context.Context) (T, error)) (T, error) {
	var zero T
	if ctx.Err() != nil {
		return run(ctx)
	}
	var led atomic.Bool
	started := make(chan struct{})
	ch := f.group.DoChan(key, func() (any, error) {
		led.Store(true)
		close(started)
		return run(ctx)
	})
	var res singleflight.Result
	select {
	case res = <-ch:
	case <-ctx.Done():
		select {
		case <-started:
			res = <-ch
		default:
			return zero, ctx.Err()
		}
		if !led.Load() {
			return zero, ctx.Err()
		}
	}
	if res.Err == nil {
		return clone(res.Val.(T)), nil
	}
	if led.Load() {
		return zero, res.Err
	}
	if ctx.Err() != nil {
		return zero, ctx.Err()
	}
	return run(ctx)
}

// flightKey builds a key from typed fields, each length-prefixed so two
// different field lists never read as the same string.
type flightKey struct{ b strings.Builder }

func (k *flightKey) str(s string) *flightKey {
	k.b.WriteString(strconv.Itoa(len(s)))
	k.b.WriteByte(':')
	k.b.WriteString(s)
	return k
}

func (k *flightKey) num(n int64) *flightKey { return k.str(strconv.FormatInt(n, 10)) }

func (k *flightKey) flag(v bool) *flightKey {
	if v {
		return k.str("1")
	}
	return k.str("0")
}

func (k *flightKey) strs(list []string) *flightKey {
	k.num(int64(len(list)))
	for _, s := range list {
		k.str(s)
	}
	return k
}

func (k *flightKey) String() string { return k.b.String() }

// filter writes every field of the filter input. nil and the empty filter
// differ, so a request never shares with one that filters differently.
func (k *flightKey) filter(f *model.FilterInput) *flightKey {
	if f == nil {
		return k.flag(false)
	}
	k.flag(true)
	if f.Scope == nil {
		k.flag(false)
	} else {
		k.flag(true).str(string(f.Scope.Level)).strs(f.Scope.Ids)
	}
	if f.Who == nil {
		k.flag(false)
	} else {
		k.flag(true).strs(f.Who.Developers).strs(f.Who.Roles)
	}
	if f.What == nil {
		k.flag(false)
	} else {
		k.flag(true).strs(f.What.Repos).strs(f.What.Services)
	}
	if f.Why == nil {
		k.flag(false)
	} else {
		k.flag(true).strs(f.Why.WorkCategory).strs(f.Why.IssueType)
	}
	if f.How == nil {
		k.flag(false)
	} else {
		k.flag(true).strs(f.How.FlowStage)
	}
	return k
}
