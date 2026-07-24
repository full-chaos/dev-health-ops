package externalrecompute

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/google/uuid"
	valkeygo "github.com/valkey-io/valkey-go"
)

const (
	recomputeKeyTag       = "{external-ingest-recompute-go}"
	recomputeDueKey       = "external-ingest:recompute:" + recomputeKeyTag + ":due"
	recomputeInflightKey  = "external-ingest:recompute:" + recomputeKeyTag + ":inflight"
	recomputeSequenceKey  = "external-ingest:recompute:" + recomputeKeyTag + ":sequence"
	recomputeBlobTTLFloor = 5 * time.Minute
)

const coalesceLua = `
local raw = redis.call("GET", KEYS[1])
local incoming = cjson.decode(ARGV[1])
local function union(left, right)
  local seen = {}
  local result = {}
  for _, values in ipairs({left or {}, right or {}}) do
    for _, value in ipairs(values) do
      if not seen[value] then
        seen[value] = true
        table.insert(result, value)
      end
    end
  end
  table.sort(result)
  return result
end
if raw then
  local current = cjson.decode(raw)
  if current.org_id ~= incoming.org_id or current.source_system ~= incoming.source_system or current.source_instance ~= incoming.source_instance then
    return redis.error_reply("scope identity mismatch")
  end
  current.repo_ids = union(current.repo_ids, incoming.repo_ids)
  current.team_ids = union(current.team_ids, incoming.team_ids)
  current.record_kinds = union(current.record_kinds, incoming.record_kinds)
  current.ingestion_ids = union(current.ingestion_ids, incoming.ingestion_ids)
  if incoming.window_start and (not current.window_start or incoming.window_start < current.window_start) then
    current.window_start = incoming.window_start
  end
  if incoming.window_end and (not current.window_end or incoming.window_end > current.window_end) then
    current.window_end = incoming.window_end
  end
  local encoded = cjson.encode(current)
  redis.call("SET", KEYS[1], encoded, "XX", "PX", ARGV[4])
  return encoded
end
local generation = redis.call("INCR", KEYS[3])
incoming.generation = generation
local encoded = cjson.encode(incoming)
redis.call("SET", KEYS[1], encoded, "NX", "PX", ARGV[4])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[3] .. "|" .. tostring(generation))
return encoded
`

const claimPendingLua = `
local raw = redis.call("GET", KEYS[1])
if not raw then
  redis.call("ZREM", KEYS[2], ARGV[1])
  return nil
end
local value = cjson.decode(raw)
if tostring(value.generation) ~= ARGV[2] then
  redis.call("ZREM", KEYS[2], ARGV[1])
  return nil
end
redis.call("SET", KEYS[3], raw, "PX", ARGV[4])
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("ZADD", KEYS[4], ARGV[3], ARGV[1])
return raw
`

const leaseInflightLua = `
local raw = redis.call("GET", KEYS[1])
if not raw then
  redis.call("ZREM", KEYS[2], ARGV[1])
  return nil
end
redis.call("PEXPIRE", KEYS[1], ARGV[3])
redis.call("ZADD", KEYS[2], ARGV[2], ARGV[1])
return raw
`

const completeInflightLua = `
redis.call("DEL", KEYS[1])
redis.call("ZREM", KEYS[2], ARGV[1])
return 1
`

type wireScope struct {
	Generation     int64    `json:"generation,omitempty"`
	OrgID          string   `json:"org_id"`
	SourceSystem   string   `json:"source_system"`
	SourceInstance string   `json:"source_instance"`
	IngestionIDs   []string `json:"ingestion_ids"`
	RepoIDs        []string `json:"repo_ids"`
	TeamIDs        []string `json:"team_ids"`
	RecordKinds    []string `json:"record_kinds"`
	WindowStart    *string  `json:"window_start"`
	WindowEnd      *string  `json:"window_end"`
}

type ValkeyStore struct{ client valkeygo.Client }

func NewValkeyStore(client valkeygo.Client) (*ValkeyStore, error) {
	if client == nil {
		return nil, ErrInvalidConfig
	}
	return &ValkeyStore{client: client}, nil
}

func (store *ValkeyStore) Coalesce(
	ctx context.Context,
	scope streamhandlers.ExternalRecomputeScope,
	now time.Time,
	debounce time.Duration,
) error {
	if store == nil || store.client == nil || validateScope(scope) != nil || debounce <= 0 {
		return ErrInvalidConfig
	}
	wire := wireFromScope(scope)
	raw, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("marshal external recompute scope: %w", err)
	}
	pendingKey := pendingScopeKey(scope.OrgID, scope.SourceSystem, scope.SourceInstance)
	ticketPrefix := pendingKey
	ttl := max(4*debounce, recomputeBlobTTLFloor)
	result := valkeygo.NewLuaScriptNoSha(coalesceLua).Exec(
		ctx,
		store.client,
		[]string{pendingKey, recomputeDueKey, recomputeSequenceKey},
		[]string{
			string(raw),
			strconv.FormatInt(now.Add(debounce).UnixMilli(), 10),
			ticketPrefix,
			strconv.FormatInt(ttl.Milliseconds(), 10),
		},
	)
	if result.Error() != nil {
		return fmt.Errorf("coalesce external recompute scope: %w", result.Error())
	}
	return nil
}

func (store *ValkeyStore) ClaimDue(
	ctx context.Context,
	now time.Time,
	limit int,
	retryAfter time.Duration,
) ([]Claim, error) {
	if store == nil || store.client == nil || limit < 1 || retryAfter <= 0 {
		return nil, ErrInvalidConfig
	}
	pending, err := store.dueTickets(ctx, recomputeDueKey, now, limit)
	if err != nil {
		return nil, err
	}
	inflight, err := store.dueTickets(ctx, recomputeInflightKey, now, limit)
	if err != nil {
		return nil, err
	}
	tickets := append(pending, inflight...)
	slices.Sort(tickets)
	tickets = slices.Compact(tickets)
	if len(tickets) > limit {
		tickets = tickets[:limit]
	}
	claims := make([]Claim, 0, len(tickets))
	for _, ticket := range tickets {
		var (
			raw string
			err error
		)
		if slices.Contains(pending, ticket) {
			raw, err = store.claimPending(ctx, ticket, now, retryAfter)
		} else {
			raw, err = store.leaseInflight(ctx, ticket, now, retryAfter)
		}
		if err != nil {
			return nil, err
		}
		if raw == "" {
			continue
		}
		claim, err := claimFromWire(ticket, []byte(raw))
		if err != nil {
			return nil, err
		}
		claims = append(claims, claim)
	}
	return claims, nil
}

func (store *ValkeyStore) Complete(ctx context.Context, claim Claim) error {
	if store == nil || store.client == nil || claim.ID == "" {
		return ErrInvalidConfig
	}
	result := valkeygo.NewLuaScriptNoSha(completeInflightLua).Exec(
		ctx,
		store.client,
		[]string{inflightScopeKey(claim.ID), recomputeInflightKey},
		[]string{claim.ID},
	)
	if err := result.Error(); err != nil {
		return fmt.Errorf("complete external recompute claim: %w", err)
	}
	return nil
}

func (store *ValkeyStore) dueTickets(ctx context.Context, key string, now time.Time, limit int) ([]string, error) {
	result := store.client.Do(
		ctx,
		store.client.B().Zrangebyscore().Key(key).Min("-inf").
			Max(strconv.FormatInt(now.UnixMilli(), 10)).Limit(0, int64(limit)).Build(),
	)
	tickets, err := result.AsStrSlice()
	if valkeygo.IsValkeyNil(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load due external recompute claims: %w", err)
	}
	return tickets, nil
}

func (store *ValkeyStore) claimPending(ctx context.Context, ticket string, now time.Time, retryAfter time.Duration) (string, error) {
	pendingKey, generation, ok := parseTicket(ticket)
	if !ok {
		return "", fmt.Errorf("invalid external recompute ticket")
	}
	ttl := max(4*retryAfter, recomputeBlobTTLFloor)
	result := valkeygo.NewLuaScriptNoSha(claimPendingLua).Exec(
		ctx,
		store.client,
		[]string{pendingKey, recomputeDueKey, inflightScopeKey(ticket), recomputeInflightKey},
		[]string{
			ticket,
			generation,
			strconv.FormatInt(now.Add(retryAfter).UnixMilli(), 10),
			strconv.FormatInt(ttl.Milliseconds(), 10),
		},
	)
	raw, err := result.ToString()
	if valkeygo.IsValkeyNil(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("claim pending external recompute scope: %w", err)
	}
	return raw, nil
}

func (store *ValkeyStore) leaseInflight(ctx context.Context, ticket string, now time.Time, retryAfter time.Duration) (string, error) {
	ttl := max(4*retryAfter, recomputeBlobTTLFloor)
	result := valkeygo.NewLuaScriptNoSha(leaseInflightLua).Exec(
		ctx,
		store.client,
		[]string{inflightScopeKey(ticket), recomputeInflightKey},
		[]string{
			ticket,
			strconv.FormatInt(now.Add(retryAfter).UnixMilli(), 10),
			strconv.FormatInt(ttl.Milliseconds(), 10),
		},
	)
	raw, err := result.ToString()
	if valkeygo.IsValkeyNil(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("lease inflight external recompute scope: %w", err)
	}
	return raw, nil
}

func pendingScopeKey(orgID, system, instance string) string {
	hash := sha256.Sum256([]byte(orgID + "\x00" + system + "\x00" + instance))
	return "external-ingest:recompute:" + recomputeKeyTag + ":pending:" + hex.EncodeToString(hash[:])
}

func inflightScopeKey(ticket string) string {
	hash := sha256.Sum256([]byte(ticket))
	return "external-ingest:recompute:" + recomputeKeyTag + ":inflight:" + hex.EncodeToString(hash[:])
}

func parseTicket(ticket string) (string, string, bool) {
	index := strings.LastIndex(ticket, "|")
	if index < 1 || index == len(ticket)-1 {
		return "", "", false
	}
	if _, err := strconv.ParseInt(ticket[index+1:], 10, 64); err != nil {
		return "", "", false
	}
	return ticket[:index], ticket[index+1:], true
}

func wireFromScope(scope streamhandlers.ExternalRecomputeScope) wireScope {
	wire := wireScope{
		OrgID: scope.OrgID, SourceSystem: scope.SourceSystem,
		SourceInstance: scope.SourceInstance,
		IngestionIDs:   []string{scope.IngestionID.String()},
		RepoIDs:        sortedUnique(scope.RepoIDs),
		TeamIDs:        sortedUnique(scope.TeamIDs),
		RecordKinds:    sortedUnique(scope.RecordKinds),
	}
	if scope.WindowStart != nil {
		value := scope.WindowStart.UTC().Format(time.RFC3339Nano)
		wire.WindowStart = &value
	}
	if scope.WindowEnd != nil {
		value := scope.WindowEnd.UTC().Format(time.RFC3339Nano)
		wire.WindowEnd = &value
	}
	return wire
}

func claimFromWire(ticket string, raw []byte) (Claim, error) {
	var wire wireScope
	if err := json.Unmarshal(raw, &wire); err != nil {
		return Claim{}, fmt.Errorf("decode external recompute claim: %w", err)
	}
	if wire.Generation <= 0 || len(wire.IngestionIDs) == 0 {
		return Claim{}, fmt.Errorf("invalid external recompute claim")
	}
	first, err := uuid.Parse(wire.IngestionIDs[0])
	if err != nil {
		return Claim{}, fmt.Errorf("parse external recompute ingestion id: %w", err)
	}
	scope := streamhandlers.ExternalRecomputeScope{
		OrgID: wire.OrgID, SourceSystem: wire.SourceSystem,
		SourceInstance: wire.SourceInstance, IngestionID: first,
		RepoIDs: wire.RepoIDs, TeamIDs: wire.TeamIDs, RecordKinds: wire.RecordKinds,
	}
	if wire.WindowStart != nil {
		value, err := time.Parse(time.RFC3339Nano, *wire.WindowStart)
		if err != nil {
			return Claim{}, fmt.Errorf("parse external recompute window start: %w", err)
		}
		scope.WindowStart = &value
	}
	if wire.WindowEnd != nil {
		value, err := time.Parse(time.RFC3339Nano, *wire.WindowEnd)
		if err != nil {
			return Claim{}, fmt.Errorf("parse external recompute window end: %w", err)
		}
		scope.WindowEnd = &value
	}
	// The bridge needs every coalesced ingestion id; keep them on the claim
	// rather than overloading the single-batch scheduler seam.
	return Claim{ID: ticket, Scope: scope, ingestionIDs: wire.IngestionIDs}, nil
}
