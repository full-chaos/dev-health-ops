package streamrunner

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
)

// ValkeyTransport is the production Redis Streams adapter. It deliberately
// uses the long-lived client supplied by storage/valkey rather than creating a
// client per poll, and all blocking reads inherit their lifecycle context.
type ValkeyTransport struct{ client valkeygo.Client }

func NewValkeyTransport(client valkeygo.Client) (*ValkeyTransport, error) {
	if client == nil {
		return nil, ErrInvalidConfig
	}
	return &ValkeyTransport{client: client}, nil
}

func (t *ValkeyTransport) EnsureGroup(ctx context.Context, stream, group string) error {
	err := t.client.Do(ctx, t.client.B().XgroupCreate().Key(stream).Group(group).Id("0").Mkstream().Build()).Error()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (t *ValkeyTransport) ReadNew(ctx context.Context, stream, group, consumer string, count int, block time.Duration) ([]Message, error) {
	result := t.client.Do(ctx, t.client.B().Xreadgroup().Group(group, consumer).Count(int64(count)).Block(block.Milliseconds()).Streams().Key(stream).Id(">").Build())
	read, err := result.AsXRead()
	if err != nil {
		return nil, err
	}
	return messagesFromRead(read), nil
}

func (t *ValkeyTransport) Pending(ctx context.Context, stream, group string, count int, idle time.Duration) ([]Pending, error) {
	result := t.client.Do(ctx, t.client.B().Xpending().Key(stream).Group(group).Idle(idle.Milliseconds()).Start("-").End("+").Count(int64(count)).Build())
	value, err := result.ToAny()
	if err != nil {
		return nil, err
	}
	rows, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected XPENDING reply")
	}
	pending := make([]Pending, 0, len(rows))
	for _, raw := range rows {
		row, ok := raw.([]any)
		if !ok || len(row) < 4 {
			return nil, fmt.Errorf("unexpected XPENDING entry")
		}
		id, ok := asString(row[0])
		if !ok {
			return nil, fmt.Errorf("unexpected XPENDING id")
		}
		idleMS, ok := asInt64(row[2])
		if !ok {
			return nil, fmt.Errorf("unexpected XPENDING idle")
		}
		deliveries, ok := asInt64(row[3])
		if !ok {
			return nil, fmt.Errorf("unexpected XPENDING deliveries")
		}
		pending = append(pending, Pending{MessageID: id, Idle: time.Duration(idleMS) * time.Millisecond, TimesDelivered: int(deliveries)})
	}
	return pending, nil
}

func (t *ValkeyTransport) Claim(ctx context.Context, stream, group, consumer string, ids []string, idle time.Duration) ([]Message, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	result := t.client.Do(ctx, t.client.B().Xclaim().Key(stream).Group(group).Consumer(consumer).MinIdleTime(strconv.FormatInt(idle.Milliseconds(), 10)).Id(ids...).Build())
	entries, err := result.AsXRange()
	if err != nil {
		return nil, err
	}
	messages := make([]Message, 0, len(entries))
	for _, entry := range entries {
		messages = append(messages, Message{Stream: stream, ID: entry.ID, Fields: entry.FieldValues})
	}
	return messages, nil
}

func (t *ValkeyTransport) Ack(ctx context.Context, stream, group, id string) error {
	return t.client.Do(ctx, t.client.B().Xack().Key(stream).Group(group).Id(id).Build()).Error()
}

func (t *ValkeyTransport) Quarantine(ctx context.Context, message Message, reason string) error {
	fields := map[string]string{"original_stream": message.Stream, "entry_id": message.ID, "reason": reason, "moved_at": time.Now().UTC().Format(time.RFC3339Nano)}
	for key, value := range message.Fields {
		if key == "ingestion_id" || key == "org_id" {
			fields[key] = value
		}
	}
	command := t.client.B().Xadd().Key(quarantineStream(message.Stream)).Maxlen().Almost().Threshold("100000").Id("*").FieldValue()
	for key, value := range fields {
		command = command.FieldValue(key, value)
	}
	return t.client.Do(ctx, command.Build()).Error()
}

func (t *ValkeyTransport) Stats(ctx context.Context, stream, group string) (StreamStats, error) {
	length, err := t.client.Do(ctx, t.client.B().Xlen().Key(stream).Build()).AsInt64()
	if err != nil {
		return StreamStats{}, err
	}
	summary, err := t.client.Do(ctx, t.client.B().Xpending().Key(stream).Group(group).Build()).ToAny()
	if err != nil {
		return StreamStats{}, err
	}
	pending, oldest, err := pendingSummary(summary)
	if err != nil {
		return StreamStats{}, err
	}
	if pending > 0 {
		// Match the existing Python health contract: the lowest-ID PEL entry
		// is the operational oldest-pending proxy. This keeps the metric cheap
		// and does not expose consumer or tenant identities.
		entries, pendingErr := t.Pending(ctx, stream, group, 1, 0)
		if pendingErr != nil {
			return StreamStats{}, pendingErr
		}
		if len(entries) > 0 {
			oldest = entries[0].Idle
		}
	}
	lag := length - pending
	// XINFO GROUPS exposes precise group lag when Redis/Valkey supports it.
	if groups, err := t.client.Do(ctx, t.client.B().XinfoGroups().Key(stream).Build()).ToAny(); err == nil {
		if exact, found := groupLag(groups, group); found {
			lag = exact
		}
	}
	return StreamStats{Lag: max(lag, 0), Pending: pending, OldestPending: oldest}, nil
}

func (t *ValkeyTransport) Close() {
	if t != nil && t.client != nil {
		t.client.Close()
	}
}

func messagesFromRead(read map[string][]valkeygo.XRangeEntry) []Message {
	var messages []Message
	for stream, entries := range read {
		for _, entry := range entries {
			messages = append(messages, Message{Stream: stream, ID: entry.ID, Fields: entry.FieldValues})
		}
	}
	return messages
}

func quarantineStream(stream string) string {
	parts := strings.Split(stream, ":")
	if len(parts) == 3 && parts[0] == "external-ingest" {
		return "external-ingest:" + parts[1] + ":dlq"
	}
	if len(parts) == 3 && parts[0] == "ingest" {
		return "ingest:dlq:" + parts[2]
	}
	return "product-telemetry:dlq"
}

func pendingSummary(value any) (int64, time.Duration, error) {
	row, ok := value.([]any)
	if !ok || len(row) < 1 {
		return 0, 0, fmt.Errorf("unexpected XPENDING summary")
	}
	pending, ok := asInt64(row[0])
	if !ok {
		return 0, 0, fmt.Errorf("unexpected XPENDING count")
	}
	return pending, 0, nil
}

func groupLag(value any, group string) (int64, bool) {
	rows, ok := value.([]any)
	if !ok {
		return 0, false
	}
	for _, raw := range rows {
		row, ok := raw.([]any)
		if !ok {
			continue
		}
		values := make(map[string]any, len(row)/2)
		for index := 0; index+1 < len(row); index += 2 {
			key, keyOK := asString(row[index])
			if keyOK {
				values[key] = row[index+1]
			}
		}
		name, nameOK := asString(values["name"])
		lag, lagOK := asInt64(values["lag"])
		if nameOK && lagOK && name == group {
			return lag, true
		}
	}
	return 0, false
}

func asString(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return "", false
	}
}
func asInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case string:
		parsed, err := strconv.ParseInt(typed, 10, 64)
		return parsed, err == nil
	case []byte:
		parsed, err := strconv.ParseInt(string(typed), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}
