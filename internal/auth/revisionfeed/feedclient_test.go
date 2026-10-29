package revisionfeed

import (
	"context"
	"sync"
)

type evalCall struct {
	script string
	keys   []string
	args   []string
}

// fakeClient is the narrow Valkey surface the feed uses. It cannot run Lua (the module set has no interpreter): the
// script's semantics are pinned by its text in sink_test.go and run on a real Valkey by the integration cells.
type fakeClient struct {
	mu sync.Mutex

	evalCalls  []evalCall
	evalResult int64
	evalErr    error

	hash       map[string]string
	hgetallErr error

	config    map[string]string
	configErr error

	ttl    int64
	ttlErr error

	subscribe func(ctx context.Context, channel string, hooks SubscribeHooks) error
}

func (f *fakeClient) EvalInt(_ context.Context, script string, keys, args []string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evalCalls = append(f.evalCalls, evalCall{script: script, keys: keys, args: args})
	return f.evalResult, f.evalErr
}

func (f *fakeClient) HGetAll(context.Context, string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hash, f.hgetallErr
}

func (f *fakeClient) ConfigGet(_ context.Context, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.configErr != nil {
		return "", f.configErr
	}
	return f.config[name], nil
}

func (f *fakeClient) TTL(context.Context, string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ttl, f.ttlErr
}

func (f *fakeClient) Subscribe(ctx context.Context, channel string, hooks SubscribeHooks) error {
	if f.subscribe == nil {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.subscribe(ctx, channel, hooks)
}

func (f *fakeClient) calls() []evalCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]evalCall(nil), f.evalCalls...)
}
