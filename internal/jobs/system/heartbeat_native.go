package system

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// defaultHeartbeatTier is what an organization with no org_licenses row (or a
// row with a blank tier) reports -- the same fallback system_ops.py's
// phone_home_heartbeat used.
const defaultHeartbeatTier = "community"

// heartbeatAuditSource replaces the Python HTTP bridge's "http_bridge" value:
// the compute now runs inside this process, not behind a bridge call, so the
// audit trail should say so.
const heartbeatAuditSource = "go_worker"

// HeartbeatSnapshot is the phone-home payload. Field NAMES are unchanged from
// system_ops.py's phone_home_heartbeat -- an external telemetry receiver
// keys on them -- but three fields now carry THIS process's own identity
// rather than a value translated from the interpreter that used to compute
// them: InstanceID/Version come from Go worker configuration and build
// metadata, and UptimeSeconds is measured from this worker's own start
// (never from a Python clock reading).
type HeartbeatSnapshot struct {
	InstanceID    string  `json:"instance_id"`
	Version       string  `json:"version"`
	OrgCount      int64   `json:"org_count"`
	UserCount     int64   `json:"user_count"`
	Tier          string  `json:"tier"`
	LicenseHash   *string `json:"license_hash"`
	UptimeSeconds float64 `json:"uptime_seconds"`
	Timestamp     string  `json:"timestamp"`
}

// NativeHeartbeatDispatcher computes and reports the phone-home heartbeat
// entirely inside the Go worker: no HTTP call into the Python API remains on
// this path. It implements the same HeartbeatDispatcher interface the HTTP
// bridge used to, so HeartbeatHandler and its tests are unchanged.
type NativeHeartbeatDispatcher struct {
	pool       *pgxpool.Pool
	httpClient *http.Client
	endpoint   string
	instanceID string
	version    string
	// startedAt anchors UptimeSeconds to this dispatcher's own construction
	// (once per worker process), not to an arbitrary monotonic-clock origin
	// the way the Python implementation's time.monotonic() read did.
	startedAt time.Time
	now       func() time.Time
}

func NewNativeHeartbeatDispatcher(
	pool *pgxpool.Pool, httpClient *http.Client, endpoint, instanceID, version string,
) (*NativeHeartbeatDispatcher, error) {
	if pool == nil {
		return nil, errors.New("heartbeat dispatcher requires a database pool")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if strings.TrimSpace(instanceID) == "" {
		instanceID = "unknown"
	}
	return &NativeHeartbeatDispatcher{
		pool:       pool,
		httpClient: httpClient,
		endpoint:   strings.TrimSpace(endpoint),
		instanceID: instanceID,
		version:    version,
		startedAt:  time.Now(),
		now:        time.Now,
	}, nil
}

func (dispatcher *NativeHeartbeatDispatcher) DispatchHeartbeat(
	ctx context.Context, _ time.Time,
) error {
	if dispatcher == nil || dispatcher.pool == nil {
		return errors.New("native heartbeat dispatcher is not configured")
	}
	facts, err := queryHeartbeatFacts(ctx, dispatcher.pool)
	if err != nil {
		return fmt.Errorf("heartbeat facts query failed: %w", err)
	}
	now := dispatcher.now()
	snapshot := HeartbeatSnapshot{
		InstanceID:    dispatcher.instanceID,
		Version:       dispatcher.version,
		OrgCount:      facts.orgCount,
		UserCount:     facts.userCount,
		Tier:          facts.tier,
		LicenseHash:   facts.licenseHash,
		UptimeSeconds: now.Sub(dispatcher.startedAt).Seconds(),
		Timestamp:     now.UTC().Format("2006-01-02T15:04:05.000000-07:00"),
	}

	// Parity with phone_home_heartbeat: the audit row is written in the same
	// unit of work as the counts read, so a write failure here fails the
	// heartbeat rather than being swallowed -- only the OUTBOUND POST below
	// is best-effort.
	if facts.firstOrgID != "" {
		if auditErr := writeHeartbeatAuditLog(
			ctx, dispatcher.pool, facts.firstOrgID, snapshot, dispatcher.endpoint,
		); auditErr != nil {
			return fmt.Errorf("heartbeat audit log write failed: %w", auditErr)
		}
	}

	if dispatcher.endpoint == "" {
		slog.DebugContext(ctx, "heartbeat: no telemetry endpoint configured, recorded locally")
		return nil
	}
	if postErr := dispatcher.postTelemetry(ctx, snapshot); postErr != nil {
		// Matches phone_home_heartbeat: a failed outbound POST is logged and
		// swallowed, never turned into a job retry -- the heartbeat has
		// already been recorded locally by the audit write above.
		slog.WarnContext(ctx, "heartbeat: phone-home POST failed", "error", postErr)
	}
	return nil
}

func (dispatcher *NativeHeartbeatDispatcher) postTelemetry(
	ctx context.Context, snapshot HeartbeatSnapshot,
) error {
	body, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode telemetry payload: %w", err)
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, dispatcher.endpoint, bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf("construct telemetry request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := dispatcher.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("telemetry endpoint unreachable: %w", logging.TransportFailure(err))
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4*1024))
	slog.InfoContext(ctx, "heartbeat: phone-home sent", "status_code", response.StatusCode)
	return nil
}

// heartbeatFacts is what phone_home_heartbeat's Postgres session block used
// to read in one unit of work: the two fleet counts, the license tier and
// digest, and the organization the audit row gets attached to.
type heartbeatFacts struct {
	orgCount    int64
	userCount   int64
	tier        string
	licenseHash *string
	firstOrgID  string
}

// queryHeartbeatFacts ports system_ops.py's session block verbatim,
// including its two intentionally unordered `LIMIT 1` reads: the Python
// code never applied an ORDER BY to "the first organization" or "the org
// license row", so this does not add one either -- both pick whichever row
// the database returns first, exactly as before.
func queryHeartbeatFacts(ctx context.Context, pool *pgxpool.Pool) (heartbeatFacts, error) {
	facts := heartbeatFacts{tier: defaultHeartbeatTier}

	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.organizations`).
		Scan(&facts.orgCount); err != nil {
		return heartbeatFacts{}, fmt.Errorf("count organizations: %w", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM public.users`).
		Scan(&facts.userCount); err != nil {
		return heartbeatFacts{}, fmt.Errorf("count users: %w", err)
	}

	switch err := pool.QueryRow(ctx, `SELECT id::text FROM public.organizations LIMIT 1`).
		Scan(&facts.firstOrgID); {
	case err == nil, errors.Is(err, pgx.ErrNoRows):
		// No rows leaves firstOrgID empty, meaning "no audit row" downstream
		// -- matching Python's org_id_for_audit staying None.
	default:
		return heartbeatFacts{}, fmt.Errorf("load an organization for the audit row: %w", err)
	}

	var tier *string
	var licenseKey *string
	switch err := pool.QueryRow(ctx, `SELECT tier, license_key FROM public.org_licenses LIMIT 1`).
		Scan(&tier, &licenseKey); {
	case err == nil:
		if tier != nil && strings.TrimSpace(*tier) != "" {
			facts.tier = *tier
		}
		if licenseKey != nil && *licenseKey != "" {
			sum := sha256.Sum256([]byte(*licenseKey))
			hash := hex.EncodeToString(sum[:])[:16]
			facts.licenseHash = &hash
		}
	case errors.Is(err, pgx.ErrNoRows):
		// No license row: stays at defaultHeartbeatTier, no hash.
	default:
		return heartbeatFacts{}, fmt.Errorf("load the org license: %w", err)
	}

	return facts, nil
}

// writeHeartbeatAuditLog ports the AuditLog row system_ops.py wrote inline.
// audit_logs.status is NOT NULL with no database-level default (the Python
// model default is applied by the ORM, not the schema), so it is set
// explicitly here.
func writeHeartbeatAuditLog(
	ctx context.Context, pool *pgxpool.Pool, orgID string, snapshot HeartbeatSnapshot, endpoint string,
) error {
	changes, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode heartbeat payload: %w", err)
	}
	var endpointValue any
	if endpoint != "" {
		endpointValue = endpoint
	}
	metadata, err := json.Marshal(map[string]any{
		"source":   heartbeatAuditSource,
		"endpoint": endpointValue,
	})
	if err != nil {
		return fmt.Errorf("encode heartbeat audit metadata: %w", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO public.audit_logs
			(id, org_id, action, resource_type, resource_id, description,
				changes, request_metadata, status, created_at)
		VALUES (
			gen_random_uuid(), $1, 'other', 'other', 'phone_home_heartbeat',
			'Background phone-home heartbeat recorded', $2::jsonb, $3::jsonb,
			'success', now()
		)`, orgID, changes, metadata,
	); err != nil {
		return fmt.Errorf("insert heartbeat audit log: %w", err)
	}
	return nil
}
