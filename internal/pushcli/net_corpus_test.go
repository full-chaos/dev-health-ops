//go:build integration

package pushcli

import (
	"fmt"
	"strings"
	"testing"
)

const (
	netToken = "fcpush_test_token_abc-123"
	netOrg   = "org-1"
)

func netDefaultEnv() map[string]string {
	return map[string]string{"FULLCHAOS_API_URL": "{url}", "FULLCHAOS_INGEST_TOKEN": netToken, "FULLCHAOS_ORG_ID": netOrg}
}

func jsonStep(status int, body string, headers ...string) netStep {
	step := netStep{Status: status, Body: body}
	for index := 0; index+1 < len(headers); index += 2 {
		if step.Headers == nil {
			step.Headers = map[string]string{}
		}
		step.Headers[headers[index]] = headers[index+1]
	}
	return step
}

func accepted(id string) netStep {
	return jsonStep(202, fmt.Sprintf(`{"ingestionId":%q,"status":"queued","itemsReceived":1}`, id))
}

func statusStep(id, status string, accepted, rejected int) netStep {
	return jsonStep(200, fmt.Sprintf(`{"ingestionId":%q,"status":%q,"itemsReceived":%d,"itemsAccepted":%d,"itemsRejected":%d}`, id, status, accepted+rejected, accepted, rejected))
}

func apiErr(status int, code, message string) netStep {
	return jsonStep(status, fmt.Sprintf(`{"error":{"code":%q,"message":%q}}`, code, message))
}

// netCorpus builds the cases; the order is fixed.
func netCorpus(t *testing.T) []netCase {
	t.Helper()
	valid := mustJSON(t, envelope(sampleRecordMap(t, "commit.v1")))
	two := mustJSON(t, envelope(sampleRecordMap(t, "commit.v1"), sampleRecordMap(t, "team.v1")))
	var cases []netCase
	add := func(name string, args []string, file string, steps ...netStep) *netCase {
		c := netCase{Name: name, Args: args, Env: netDefaultEnv(), Steps: steps}
		if strings.Contains(strings.Join(args, "\x00"), "{file}") {
			c.File = &file
		}
		cases = append(cases, c)
		return &cases[len(cases)-1]
	}
	batch := func(extra ...string) []string { return append([]string{"batch", "{file}"}, extra...) }

	// batch: the request and the report.
	add("batch accepted", batch(), valid, accepted("ing-1"))
	add("batch accepted json", batch("--json"), valid, accepted("ing-1"))
	add("batch flags over env", []string{"batch", "{file}", "--api-url", "{url}/", "--token", "fcpush_flag", "--org", "org-flag"}, valid, accepted("ing-2"))
	add("batch trailing slashes", batch(), valid, accepted("ing-3")).Env["FULLCHAOS_API_URL"] = "{url}///"
	add("batch payload first flags after", []string{"batch", "--json", "{file}", "--skip-limits-check"}, valid, accepted("ing-4"))
	add("batch stdin", []string{"batch", "-"}, "", accepted("ing-5")).Stdin = valid
	add("batch skip limits", batch("--skip-limits-check"), valid, accepted("ing-6"))
	cases[len(cases)-1].Schema = &netStep{Status: 200, Body: `{"limits":{"maxRecordsPerBatch":1,"maxBodyBytes":10}}`}
	add("batch two records", batch(), two, accepted("ing-7"))
	replay := add("batch replay 200", batch(), valid, jsonStep(200, `{"ingestionId":"ing-8","status":"completed","itemsReceived":1,"itemsAccepted":1,"itemsRejected":0}`))
	_ = replay
	add("batch ingestion_id snake", batch(), valid, jsonStep(202, `{"ingestion_id":"ing-9","status":"queued"}`))
	add("batch no ids", batch(), valid, jsonStep(202, `{"status":"queued"}`))
	add("batch falsy ingestionId", batch(), valid, jsonStep(202, `{"ingestionId":"","ingestion_id":"fallback","status":null,"itemsReceived":null,"itemsAccepted":1.5,"itemsRejected":[1,"a"]}`))
	add("batch body list", batch(), valid, jsonStep(202, `[1,2]`))
	add("batch body list json", batch("--json"), valid, jsonStep(202, `[1,{"b":1,"a":2}]`))
	add("batch body string json", batch("--json"), valid, jsonStep(202, `"ok"`))
	add("batch body null", batch(), valid, jsonStep(202, `null`))
	add("batch body not json", batch(), valid, jsonStep(202, `accepted`))
	add("batch body empty 204", batch(), valid, jsonStep(204, ``))
	add("batch redirect", batch(), valid, jsonStep(302, ``, "Location", "/elsewhere"))
	add("batch nan and unicode json", batch("--json"), valid, jsonStep(202, `{"ingestionId":"é-1","status":"q","x":NaN,"big":123456789012345678901234567890,"f":1e400,"s":"é😀"}`))

	// batch: errors the server reports.
	for _, status := range []int{400, 401, 403, 404, 409, 413, 422, 500, 502} {
		add(fmt.Sprintf("batch error %d", status), batch(), valid, apiErr(status, "some_code", "the server said no"))
		add(fmt.Sprintf("batch error %d json", status), batch("--json"), valid, apiErr(status, "some_code", "the server said no"))
	}
	withErrors := jsonStep(400, `{"error":{"code":"invalid_batch","message":"batch rejected","errors":[{"index":0,"kind":"commit.v1","code":"invalid_record","message":"bad field","path":"records[0].payload.sha"},{"index":-1,"code":"invalid_envelope","message":"envelope"},{"index":true,"kind":"x","code":5,"message":null,"path":7},"skipme",{"index":1.5,"message":"m"}]}}`)
	add("batch error with errors", batch(), valid, withErrors)
	add("batch error with errors json", batch("--json"), valid, withErrors)
	leak := jsonStep(400, fmt.Sprintf(`{"error":{"code":"echo %s","message":"Authorization: Bearer %s and BEARER  other-secret\tx and fcpush_zzz_9","errors":[{"message":"Bearer keyed","path":"fcpush_pathtoken","Authorization: Bearer key1":"Bearer v1","Authorization: Bearer key2":"v2","nested":[{"fcpush_k-1":"bearer q"}]}]}}`, netToken, netToken))
	add("batch error redaction", batch(), valid, leak)
	add("batch error redaction json", batch("--json"), valid, leak)
	add("batch error html", batch(), valid, jsonStep(500, `<html><body>Bad gateway `+strings.Repeat("x", 700)+`</body></html>`, "Content-Type", "text/html"))
	add("batch error html json", batch("--json"), valid, jsonStep(502, `<html>oops</html>`, "Content-Type", "text/html"))
	add("batch error empty body", batch(), valid, jsonStep(500, ``))
	add("batch error no message", batch(), valid, jsonStep(400, `{"error":{"code":"c","message":"","errors":"nope"}}`))
	add("batch error code missing", batch("--json"), valid, jsonStep(400, `{"error":{"message":"m","errors":[1,{"a":"Bearer x"}]}}`))
	add("batch error not an object", batch(), valid, jsonStep(400, `{"error":"plain"}`))
	add("batch error list body", batch(), valid, jsonStep(400, `[1]`))
	add("batch error unicode", batch(), valid, jsonStep(400, `{"error":{"code":"é","message":"ünï 😀 text"}}`))

	// batch: retries.
	add("batch 503 then accepted", batch(), valid, apiErr(503, "stream_unavailable", "later"), accepted("ing-r1"))
	add("batch 429 retry-after then accepted", batch("--json"), valid, jsonStep(429, `{"error":{"code":"rate_limited","message":"slow"}}`, "Retry-After", "1"), accepted("ing-r2"))
	add("batch 429 bad retry-after", batch(), valid, jsonStep(429, `{"error":{"code":"rate_limited","message":"slow"}}`, "Retry-After", "abc"), accepted("ing-r3"))
	add("batch 503 retry-after 0 always", batch(), valid, jsonStep(503, `{"error":{"code":"stream_unavailable","message":"down"}}`, "Retry-After", "0"))
	add("batch 503 retry-after 0 always json", batch("--json"), valid, jsonStep(503, `{"error":{"code":"auth_not_configured","message":"down"}}`, "Retry-After", "0"))
	add("batch 429 negative retry-after then accepted", batch(), valid, jsonStep(429, `{"error":{"code":"rate_limited","message":"slow"}}`, "Retry-After", "-1"), accepted("ing-r4"))
	add("batch 503 html retry-after 0 always", batch(), valid, jsonStep(503, `<html>`, "Content-Type", "text/html", "Retry-After", "0"))
	add("batch 503 always backoff", batch(), valid, apiErr(503, "ingest_temporarily_unavailable", "down"))
	add("batch 400 not retried after 503", batch(), valid, jsonStep(503, `{"error":{"code":"x","message":"y"}}`, "Retry-After", "0"), apiErr(400, "bad", "no"))
	c := add("batch connection refused", batch(), valid)
	c.BaseURL, c.MaskTransport = "http://127.0.0.1:1", true
	c = add("batch connection refused json", batch("--json"), valid)
	c.BaseURL, c.MaskTransport = "http://127.0.0.1:1", true
	c = add("batch url without protocol", batch(), valid)
	c.BaseURL, c.MaskTransport = "example.invalid", true

	// batch: the payload.
	add("batch missing file", []string{"batch", "{dir}/no-such-file.json"}, "", accepted("x"))
	add("batch bad json", batch(), `{"a":`, accepted("x"))
	add("batch bad json json", batch("--json"), `{"a":`, accepted("x"))
	add("batch empty file", batch(), ``, accepted("x"))
	add("batch not an object", batch(), `[1,2]`, accepted("x"))
	for index, mutate := range []func(map[string]any){
		func(m map[string]any) { m["schemaVersion"] = "external-ingest.v2" },
		func(m map[string]any) { delete(m, "idempotencyKey") },
		func(m map[string]any) { m["idempotencyKey"] = "" },
		func(m map[string]any) { m["idempotencyKey"] = strings.Repeat("k", 256) },
		func(m map[string]any) { m["records"] = []any{} },
		func(m map[string]any) { m["surprise"] = 1 },
		func(m map[string]any) {
			m["records"] = []any{map[string]any{"kind": "nope.v1", "externalId": "x", "payload": map[string]any{}}, map[string]any{"kind": "commit", "externalId": "y", "payload": map[string]any{}}}
		},
		func(m map[string]any) { m["idempotencyKey"] = strings.Repeat("k", 255) },
		func(m map[string]any) { m["idempotencyKey"] = "  spaced key  " },
		func(m map[string]any) { m["idempotencyKey"] = "clé-é-\U0001F600" },
		func(m map[string]any) { m["idempotencyKey"] = "tab\tkey" },
		func(m map[string]any) { m["idempotencyKey"] = "line\nbreak" },
		func(m map[string]any) {
			m["records"].([]any)[0].(map[string]any)["payload"] = map[string]any{"unexpected": 1}
		},
	} {
		envelopeMap := clonePlain(envelope(sampleRecordMap(t, "commit.v1")))
		mutate(envelopeMap)
		body := mustJSON(t, envelopeMap)
		add(fmt.Sprintf("batch envelope mutation %d", index), batch(), body, accepted("ing-m"))
		add(fmt.Sprintf("batch envelope mutation %d json", index), batch("--json"), body, accepted("ing-m"))
	}
	// Server-reported limits.
	limitsOf := func(text string) *netStep { return &netStep{Status: 200, Body: text} }
	for index, schema := range []*netStep{
		limitsOf(`{"limits":{"maxRecordsPerBatch":1,"maxBodyBytes":100000}}`),
		limitsOf(`{"limits":{"maxRecordsPerBatch":5,"maxBodyBytes":200}}`),
		limitsOf(`{"limits":{"maxRecordsPerBatch":true,"maxBodyBytes":true}}`),
		limitsOf(`{"limits":{"maxRecordsPerBatch":false,"maxBodyBytes":0}}`),
		limitsOf(`{"limits":{"maxRecordsPerBatch":2.5,"maxBodyBytes":"9"}}`),
		limitsOf(`{"limits":{"maxRecordsPerBatch":-3,"maxBodyBytes":-3}}`),
		limitsOf(`{"limits":{"maxRecordsPerBatch":123456789012345678901234567890,"maxBodyBytes":123456789012345678901234567890}}`),
		limitsOf(`{"limits":[1]}`),
		limitsOf(`{"limits":null}`),
		limitsOf(`{"other":1}`),
		limitsOf(`[1]`),
		limitsOf(`not json`),
		{Status: 500, Body: `{"limits":{"maxRecordsPerBatch":1}}`},
		{Status: 302, Body: `{"limits":{"maxRecordsPerBatch":1}}`, Headers: map[string]string{"Location": "/x"}},
		{Status: 201, Body: `{"limits":{"maxRecordsPerBatch":1}}`},
	} {
		c := add(fmt.Sprintf("batch schema limits %d", index), batch(), two, accepted("ing-l"))
		c.Schema = schema
		c = add(fmt.Sprintf("batch schema limits %d json", index), batch("--json"), two, accepted("ing-l"))
		c.Schema = schema
	}
	oversize := add("batch oversized by schema", batch(), valid, accepted("x"))
	oversize.Schema = limitsOf(`{"limits":{"maxBodyBytes":50}}`)
	oversizeStdin := add("batch oversized stdin by schema json", []string{"batch", "-", "--json"}, "", accepted("x"))
	oversizeStdin.Schema, oversizeStdin.Stdin = limitsOf(`{"limits":{"maxBodyBytes":50}}`), valid
	exact := add("batch exactly the schema limit", batch(), valid, accepted("ing-e"))
	exact.Schema = limitsOf(fmt.Sprintf(`{"limits":{"maxBodyBytes":%d}}`, len(valid)))
	over := add("batch one over the schema limit", batch(), valid, accepted("ing-e"))
	over.Schema = limitsOf(fmt.Sprintf(`{"limits":{"maxBodyBytes":%d}}`, len(valid)-1))

	recordsOnly := add("batch schema max records true only", batch(), two, accepted("ing-l"))
	recordsOnly.Schema = limitsOf(`{"limits":{"maxRecordsPerBatch":true}}`)
	recordsOnly = add("batch schema max records true only json", batch("--json"), two, accepted("ing-l"))
	recordsOnly.Schema = limitsOf(`{"limits":{"maxRecordsPerBatch":true}}`)
	recordsOnly = add("batch schema max records equals the batch", batch(), two, accepted("ing-l"))
	recordsOnly.Schema = limitsOf(`{"limits":{"maxRecordsPerBatch":2}}`)
	recordsOnly = add("batch schema max records equals one record", batch(), valid, accepted("ing-l"))
	recordsOnly.Schema = limitsOf(`{"limits":{"maxRecordsPerBatch":1}}`)

	// A URL with userinfo: httpx replaces the Authorization header with Basic.
	userinfo := add("batch url with userinfo", batch(), valid, accepted("ing-u"))
	userinfo.Env["FULLCHAOS_API_URL"] = "{userurl}"
	userinfo = add("batch url with userinfo json flag", []string{"batch", "{file}", "--api-url", "{userurl}", "--json"}, valid, accepted("ing-u"))
	userinfo.Schema = limitsOf(`{"limits":{"maxRecordsPerBatch":5}}`)
	add("batch url with login only", []string{"batch", "{file}", "--api-url", "{loginurl}"}, valid, accepted("ing-u"))
	add("batch url with password only", []string{"batch", "{file}", "--api-url", "{passurl}"}, valid, accepted("ing-u"))
	add("batch url with empty userinfo", []string{"batch", "{file}", "--api-url", "{emptyurl}"}, valid, accepted("ing-u"))
	add("status url with userinfo", []string{"status", "ing-1", "--api-url", "{userurl}"}, "", statusStep("ing-1", "queued", 0, 0))
	add("status url with userinfo retried", []string{"status", "ing-1", "--api-url", "{userurl}"}, "", jsonStep(503, `{"error":{"code":"x","message":"y"}}`, "Retry-After", "0"), statusStep("ing-1", "queued", 0, 0))

	// A response that goes silent: httpx's 30 s read timeout is per read, and a
	// timeout is a retryable network error whose text is empty.
	stall := add("status body stalls then answers", []string{"status", "ing-1"}, "", statusStep("ing-1", "queued", 0, 0))
	stall.Steps = []netStep{{Status: 200, Body: `{"ingestionId":"ing-1","status":"queued"}`, BodyDelayMs: 30500}, statusStep("ing-1", "queued", 0, 0)}
	stall = add("batch body stalls then accepted", batch("--json"), valid, accepted("ing-s"))
	stall.Steps = []netStep{{Status: 202, Body: `{"ingestionId":"ing-s","status":"queued"}`, BodyDelayMs: 30500}, accepted("ing-s")}

	// batch: configuration and arguments.
	missing := func(name string, env map[string]string, args ...string) {
		c := add(name, args, valid, accepted("x"))
		c.Env = env
	}
	missing("batch no config", map[string]string{}, "batch", "{file}")
	missing("batch no config json", map[string]string{}, "batch", "{file}", "--json")
	missing("batch only url", map[string]string{"FULLCHAOS_API_URL": "{url}"}, "batch", "{file}")
	missing("batch only token", map[string]string{"FULLCHAOS_INGEST_TOKEN": netToken}, "batch", "{file}")
	missing("batch only org", map[string]string{"FULLCHAOS_ORG_ID": netOrg}, "batch", "{file}")
	missing("batch url and token", map[string]string{"FULLCHAOS_API_URL": "{url}", "FULLCHAOS_INGEST_TOKEN": netToken}, "batch", "{file}")
	missing("batch slashes only url", map[string]string{"FULLCHAOS_API_URL": "///", "FULLCHAOS_INGEST_TOKEN": netToken, "FULLCHAOS_ORG_ID": netOrg}, "batch", "{file}")
	missing("batch empty env values", map[string]string{"FULLCHAOS_API_URL": "", "FULLCHAOS_INGEST_TOKEN": "", "FULLCHAOS_ORG_ID": ""}, "batch", "{file}")
	missing("batch legacy token", map[string]string{"FULLCHAOS_API_URL": "{url}", "FULLCHAOS_API_TOKEN": "fcpush_legacy", "FULLCHAOS_ORG_ID": netOrg}, "batch", "{file}")
	missing("batch both tokens", map[string]string{"FULLCHAOS_API_URL": "{url}", "FULLCHAOS_API_TOKEN": "fcpush_legacy", "FULLCHAOS_INGEST_TOKEN": "fcpush_primary", "FULLCHAOS_ORG_ID": netOrg}, "batch", "{file}")
	missing("batch empty flags fall to env", netDefaultEnv(), "batch", "{file}", "--api-url", "", "--token", "", "--org", "")
	missing("batch config from flags only", map[string]string{}, "batch", "{file}", "--api-url", "{url}", "--token", "fcpush_f", "--org", "o-f")
	add("batch no payload", []string{"batch"}, "", accepted("x"))
	add("batch two payloads", []string{"batch", "{file}", "{file}"}, valid, accepted("x"))
	add("batch unknown flag", batch("--nope"), valid, accepted("x"))
	add("batch poll-interval equals", batch("--poll", "--poll-interval=0.5"), valid, accepted("ing-p"), statusStep("ing-p", "completed", 1, 0))
	for index, value := range []string{"0.4", "0", "-1", "inf", "nan", "abc", "", "1_0", " 1 ", "0x1p-1", "1e-300", "١"} {
		add(fmt.Sprintf("batch poll-interval %d", index), batch("--poll-interval", value), valid, accepted("ing-p"))
		add(fmt.Sprintf("batch poll-timeout %d", index), batch("--poll-timeout", value), valid, accepted("ing-p"))
	}

	// batch --poll.
	poll := func(name string, extra []string, steps ...netStep) {
		add(name, batch(append([]string{"--poll", "--poll-interval", "0.5"}, extra...)...), valid, steps...)
	}
	poll("poll completed", nil, accepted("ing-1"), statusStep("ing-1", "queued", 0, 0), statusStep("ing-1", "processing", 0, 0), statusStep("ing-1", "completed", 1, 0))
	poll("poll completed json", []string{"--json"}, accepted("ing-1"), statusStep("ing-1", "processing", 0, 0), statusStep("ing-1", "completed", 1, 0))
	poll("poll completed with rejections", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsReceived":2,"itemsAccepted":1,"itemsRejected":1,"errors":[{"index":1,"kind":"commit.v1","code":"invalid_record","message":"bad","path":"records[1].payload"}]}`))
	poll("poll partial", nil, accepted("ing-1"), statusStep("ing-1", "partial", 1, 1))
	poll("poll partial no rejections", nil, accepted("ing-1"), statusStep("ing-1", "partial", 1, 0))
	poll("poll failed json", []string{"--json"}, accepted("ing-1"), statusStep("ing-1", "failed", 0, 1))
	poll("poll completed null rejected", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":null}`))
	poll("poll completed snake rejected", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","items_rejected":2}`))
	poll("poll completed zero float rejected", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":0.0}`))
	poll("poll completed string rejected", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":"0"}`))
	poll("poll stream unavailable", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"stream_unavailable","itemsReceived":1}`))
	poll("poll stream unavailable json", []string{"--json"}, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"stream_unavailable"}`))
	poll("poll accepted body is stream unavailable", nil, jsonStep(202, `{"ingestionId":"ing-1","status":"stream_unavailable"}`), jsonStep(200, `{"ingestionId":"ing-1","status":"stream_unavailable"}`))
	poll("poll timeout", []string{"--poll-timeout", "1"}, accepted("ing-1"), statusStep("ing-1", "processing", 0, 0))
	poll("poll timeout json", []string{"--poll-timeout", "0.6", "--json"}, accepted("ing-1"), statusStep("ing-1", "queued", 0, 0))
	poll("poll replay terminal", nil, jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsReceived":1,"itemsAccepted":1,"itemsRejected":0}`))
	poll("poll replay failed", nil, jsonStep(200, `{"ingestionId":"ing-1","status":"failed","itemsRejected":1}`))
	poll("poll replay not terminal", nil, jsonStep(200, `{"ingestionId":"ing-1","status":"queued"}`), statusStep("ing-1", "completed", 1, 0))
	poll("poll accepted terminal 202 still polls", nil, jsonStep(202, `{"ingestionId":"ing-1","status":"completed"}`), statusStep("ing-1", "completed", 1, 0))
	poll("poll missing ingestionId", nil, jsonStep(202, `{"status":"queued"}`))
	poll("poll missing ingestionId json", []string{"--json"}, jsonStep(202, `{"ingestionId":7,"status":"queued"}`))
	poll("poll ingestion_id snake", nil, jsonStep(202, `{"ingestion_id":"ing-s","status":"queued"}`), statusStep("ing-s", "completed", 1, 0))
	poll("poll empty ingestionId falls to snake", nil, jsonStep(202, `{"ingestionId":"","ingestion_id":"ing-t"}`), statusStep("ing-t", "completed", 1, 0))
	poll("poll status 404 mid poll", nil, accepted("ing-1"), apiErr(404, "not_found", "no such batch"))
	poll("poll status 404 mid poll json", []string{"--json"}, accepted("ing-1"), statusStep("ing-1", "queued", 0, 0), apiErr(404, "not_found", "no such batch"))
	poll("poll status 503 retried mid poll", nil, accepted("ing-1"), jsonStep(503, `{"error":{"code":"x","message":"y"}}`, "Retry-After", "0"), statusStep("ing-1", "completed", 1, 0))
	poll("poll status body not json", nil, accepted("ing-1"), jsonStep(200, `nope`))
	poll("poll status body list", nil, accepted("ing-1"), jsonStep(200, `[1]`))
	poll("poll status unhashable", nil, accepted("ing-1"), jsonStep(200, `{"status":["completed"]}`))
	poll("poll status numeric", []string{"--poll-timeout", "0.6"}, accepted("ing-1"), jsonStep(200, `{"status":5}`))
	poll("poll status missing", []string{"--poll-timeout", "0.6"}, accepted("ing-1"), jsonStep(200, `{}`))
	poll("poll accepted status unhashable 202", nil, jsonStep(202, `{"ingestionId":"ing-u","status":["completed"]}`), statusStep("ing-u", "completed", 1, 0))
	poll("poll accepted status unhashable 200", nil, jsonStep(200, `{"ingestionId":"ing-u","status":{"a":1}}`))
	poll("poll accepted body list", nil, jsonStep(202, `[1]`))
	poll("poll completed errors string", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":1,"errors":"boom"}`))
	poll("poll completed errors mixed", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":1,"errors":[{"index":0,"code":"a","message":"b"},5]}`))
	poll("poll completed errors number", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":1,"errors":5}`))
	poll("poll completed errors object", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":1,"errors":{"a":1}}`))
	poll("poll completed errors empty", nil, accepted("ing-1"), jsonStep(200, `{"ingestionId":"ing-1","status":"completed","itemsRejected":0,"errors":[]}`))
	add("batch accepted errors table", batch(), valid, jsonStep(202, `{"ingestionId":"i","status":"queued","errors":[{"index":0,"kind":"","code":"c","message":"m","path":""},{"index":"3","kind":null,"code":"d","message":"n","path":0},{"code":"e"},{"index":false,"kind":["k"],"code":{"a":1},"message":[1,2],"path":{"p":1}}]}`))

	// status.
	statusArgs := func(extra ...string) []string { return append([]string{"status", "ing-1"}, extra...) }
	add("status basic", statusArgs(), "", statusStep("ing-1", "processing", 0, 0))
	add("status basic json", statusArgs("--json"), "", statusStep("ing-1", "completed", 1, 0))
	add("status failed exits 0", statusArgs(), "", statusStep("ing-1", "failed", 0, 2))
	add("status with errors", statusArgs(), "", jsonStep(200, `{"ingestionId":"ing-1","status":"partial","itemsReceived":2,"itemsAccepted":1,"itemsRejected":1,"errors":[{"index":1,"kind":"commit.v1","code":"invalid_record","message":"bad","path":"records[1]"}]}`))
	add("status 404", statusArgs(), "", apiErr(404, "not_found", "no such batch"))
	add("status 404 json", statusArgs("--json"), "", apiErr(404, "not_found", "no such batch"))
	add("status 401", statusArgs(), "", apiErr(401, "unauthorized", "bad token"))
	add("status 503 then ok", statusArgs(), "", jsonStep(503, `{"error":{"code":"x","message":"y"}}`, "Retry-After", "0"), statusStep("ing-1", "queued", 0, 0))
	add("status 503 always", statusArgs("--json"), "", jsonStep(503, `{"error":{"code":"x","message":"y"}}`, "Retry-After", "0"))
	add("status body list", statusArgs(), "", jsonStep(200, `[1]`))
	add("status body list json", statusArgs("--json"), "", jsonStep(200, `[1]`))
	add("status body not json", statusArgs(), "", jsonStep(200, `up`))
	add("status no config", statusArgs(), "").Env = map[string]string{}
	add("status legacy token", statusArgs(), "", statusStep("ing-1", "queued", 0, 0)).Env = map[string]string{"FULLCHAOS_API_URL": "{url}", "FULLCHAOS_API_TOKEN": "fcpush_legacy", "FULLCHAOS_ORG_ID": netOrg}
	add("status no id", []string{"status"}, "")
	add("status two ids", []string{"status", "a", "b"}, "")
	add("status skip-limits flag refused", statusArgs("--skip-limits-check"), "", statusStep("ing-1", "queued", 0, 0))
	for index, id := range []string{"ing 1", "a/b", "../x", "a?b=1", "é", "a#b", "%41", "a%20b", "", "x y/../z", "a\\b", "a;b", "a'b", "a\"b", "a{b}", "a|b", "/", "//x", "./a", "a/.", "a b?c d", "😀"} {
		add(fmt.Sprintf("status id %d", index), []string{"status", id}, "", statusStep("ing-1", "queued", 0, 0))
	}
	pollStatus := func(name string, extra []string, steps ...netStep) {
		add(name, statusArgs(append([]string{"--poll", "--poll-interval", "0.5"}, extra...)...), "", steps...)
	}
	pollStatus("status poll completed", nil, statusStep("ing-1", "queued", 0, 0), statusStep("ing-1", "processing", 0, 0), statusStep("ing-1", "completed", 1, 0))
	pollStatus("status poll completed json", []string{"--json"}, statusStep("ing-1", "completed", 1, 0))
	pollStatus("status poll failed", nil, statusStep("ing-1", "failed", 0, 1))
	pollStatus("status poll partial json", []string{"--json"}, statusStep("ing-1", "processing", 0, 0), statusStep("ing-1", "partial", 1, 1))
	pollStatus("status poll stream unavailable", nil, jsonStep(200, `{"ingestionId":"ing-1","status":"stream_unavailable"}`))
	pollStatus("status poll stream unavailable json", []string{"--json"}, jsonStep(200, `{"ingestionId":"ing-1","status":"stream_unavailable"}`))
	pollStatus("status poll timeout", []string{"--poll-timeout", "1"}, statusStep("ing-1", "processing", 0, 0))
	pollStatus("status poll timeout json", []string{"--poll-timeout", "0.6", "--json"}, statusStep("ing-1", "processing", 0, 0))
	pollStatus("status poll 404", nil, apiErr(404, "not_found", "no such batch"))
	pollStatus("status poll 500 mid poll", nil, statusStep("ing-1", "queued", 0, 0), apiErr(500, "server_error", "boom"))
	pollStatus("status poll status missing timeout", []string{"--poll-timeout", "0.6"}, jsonStep(200, `{}`))
	pollStatus("status poll bad interval", []string{"--poll-interval", "0.1"}, statusStep("ing-1", "completed", 1, 0))
	return cases
}
