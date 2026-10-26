package server

import (
	"testing"
)

func TestQueryAPISessionReadFollowsTheSessionsFlag(t *testing.T) {
	t.Parallel()
	env := func(values map[string]string) getenvFunc { return func(k string) string { return values[k] } }
	for _, off := range []map[string]string{nil, {"AUTH_PROXY_SESSIONS": ""}, {"AUTH_PROXY_SESSIONS": "false"}} {
		if read, err := queryAPISessionRead(env(off)); read != nil || err != nil {
			t.Errorf("%v: %v %v, want no read", off, read, err)
		}
	}
	read, err := queryAPISessionRead(env(map[string]string{"AUTH_PROXY_SESSIONS": "true", "AUTH_DATABASE_SCHEMA": "acp"}))
	if err != nil || read == nil || read.Schema != "acp" || read.Table != "sessions" || len(read.Columns) != 4 {
		t.Fatalf("flag on: %+v %v", read, err)
	}
	if _, err := queryAPISessionRead(env(map[string]string{"AUTH_PROXY_SESSIONS": "maybe"})); err == nil {
		t.Error("an unreadable flag must be an error, not off")
	}
	if _, err := queryAPISessionRead(env(map[string]string{"AUTH_PROXY_SESSIONS": "true", "AUTH_DATABASE_SCHEMA": "a;b"})); err == nil {
		t.Error("an unusable schema name must be an error")
	}
}

func TestQueryAPIPostureCheckFailsClosedOnAnUnreadableSessionsFlag(t *testing.T) {
	t.Parallel()
	getenv := func(k string) string {
		return map[string]string{"QUERY_API_DATABASE_ROLE": "devhealth_query_api", "AUTH_PROXY_SESSIONS": "maybe"}[k]
	}
	check := queryAPIPostureCheck(getenv, nil)
	if check == nil {
		t.Fatal("a named role must be checked")
	}
	if err := check(t.Context()); err == nil {
		t.Fatal("an unreadable flag must not pass")
	}
}
