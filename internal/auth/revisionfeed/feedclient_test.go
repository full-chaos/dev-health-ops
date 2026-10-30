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

	hash         map[string]string
	hgetallErr   error
	hgetallCalls int

	info     map[string]string
	infoErr  error
	infoLog  [][]string
	cfgCalls int

	config    map[string]string
	configErr error

	ttl     int64
	ttlZero bool // a TTL of exactly 0 s (the key is about to expire); ttl 0 alone means no expiry
	ttlErr  error

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
	f.hgetallCalls++
	return f.hash, f.hgetallErr
}

func (f *fakeClient) Info(_ context.Context, sections ...string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.infoLog = append(f.infoLog, sections)
	if f.infoErr != nil {
		return nil, f.infoErr
	}
	return f.info, nil
}

func (f *fakeClient) hgetalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hgetallCalls
}

func (f *fakeClient) setHGetAll(hash map[string]string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hash, f.hgetallErr = hash, err
}

func (f *fakeClient) infoCalls() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.infoLog...)
}

func (f *fakeClient) configGets() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cfgCalls
}

// infoFields is what INFO persistence and INFO memory give for a healthy AOF and the policy; a modification with an empty
// value removes the field (a server that does not report it).
func infoFields(policy string, mods ...map[string]string) map[string]string {
	m := map[string]string{
		"aof_enabled": "1", "aof_last_write_status": "ok", "aof_last_bgrewrite_status": "ok", "maxmemory_policy": policy,
	}
	for _, mod := range mods {
		for k, v := range mod {
			if v == "" {
				delete(m, k)
			} else {
				m[k] = v
			}
		}
	}
	if policy == "" {
		delete(m, "maxmemory_policy")
	}
	return m
}

func (f *fakeClient) ConfigGet(_ context.Context, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cfgCalls++
	if f.configErr != nil {
		return "", f.configErr
	}
	return f.config[name], nil
}

// TTL answers the field ttl; the zero value means "no expiry" (-1), the state of a hash nobody set a TTL on.
func (f *fakeClient) TTL(context.Context, string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ttl == 0 && !f.ttlZero {
		return -1, f.ttlErr
	}
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
