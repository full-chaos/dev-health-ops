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

// The platform role read follows AUTH_PLATFORM_ROLES_OWNED (parsed by
// platformrole.OwnedFromLookup) and rides on the session read.
func TestQueryAPIPlatformReadFollowsTheSwitch(t *testing.T) {
	t.Parallel()
	env := func(values map[string]string) getenvFunc { return func(k string) string { return values[k] } }
	read, err := queryAPISessionRead(env(map[string]string{"AUTH_PROXY_SESSIONS": "true", "AUTH_PLATFORM_ROLES_OWNED": " TRUE "}))
	if err != nil || read == nil || len(read.Tables()) != 3 || read.Tables()[1].Table != "platform_role_assignments" {
		t.Fatalf("owned: %+v %v", read, err)
	}
	read, err = queryAPISessionRead(env(map[string]string{"AUTH_PROXY_SESSIONS": "true", "AUTH_PLATFORM_ROLES_OWNED": "false"}))
	if err != nil || read == nil || len(read.Tables()) != 1 {
		t.Fatalf("not owned: %+v %v", read, err)
	}
	if _, err := queryAPISessionRead(env(map[string]string{"AUTH_PROXY_SESSIONS": "true", "AUTH_PLATFORM_ROLES_OWNED": "yes"})); err == nil {
		t.Error("a malformed switch must be an error")
	}
	if _, err := queryAPISessionRead(env(map[string]string{"AUTH_PLATFORM_ROLES_OWNED": "true"})); err == nil {
		t.Error("platform roles owned with the session read off must be an error")
	}
}
