package remaining

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

// sqlStateShape is the only SQLSTATE form that may reach a log: five characters of [0-9A-Z]. A server error code
// from any other source (a proxy, a wrapped driver, a test double) is dropped, not truncated or escaped.
var sqlStateShape = regexp.MustCompile(`^[0-9A-Z]{5}$`)

// stepFailure returns err with its message unchanged (fmt.Errorf("<step>: %w")) and a safe log cause that names
// the step and, when the error carries them, the ClickHouse exception code and the Postgres SQLSTATE.
//
// CHAOS-8020: a retryable failure of the partition handler leaves River only the bounded text
// `dev-health job failed [retryable]` (jobruntime/errors.go safeError). The runtime logs a cause only when the
// handler opts in (WithSafeCauseText), and the ClickHouse/Postgres step errors never did, so a daily failure of
// work_item_attribution (three attempts, then a discard) was unrecoverable. The cause text built here is made
// ONLY of the closed step label written at the call site, the integer ClickHouse code and a validated SQLSTATE:
// never the driver message, a query, a table value or an identifier of the data.
func stepFailure(step string, err error) error {
	if err == nil {
		return nil
	}
	return jobruntime.WithSafeCauseText(fmt.Errorf("%s: %w", step, err), safeStepCause(step, err))
}

// safeStepCause is stepFailure's cause text: `step=<label>` plus ` ch_code=<n>` and ` sqlstate=<xxxxx>` when present.
func safeStepCause(step string, err error) string {
	cause := "step=" + step
	var exception *clickhouse.Exception
	if errors.As(err, &exception) && exception != nil {
		cause += " ch_code=" + strconv.FormatInt(int64(exception.Code), 10)
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) && pgError != nil && sqlStateShape.MatchString(pgError.Code) {
		cause += " sqlstate=" + pgError.Code
	}
	return cause
}
