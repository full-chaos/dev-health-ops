package main

import (
	"context"
	"io"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
)

// undeliveredClass is one row of `workgraph list-undelivered`: every outbox
// row of one job kind that cannot reach River on its own, grouped by the
// reason the reconciler's undelivered sweep assigns (or will assign) it.
type undeliveredClass struct {
	JobKind          string `json:"job_kind"`
	Reason           string `json:"reason"`
	Count            int64  `json:"count"`
	OldestAgeSeconds int64  `json:"oldest_age_seconds"`
}

type undeliveredReport struct {
	CeilingSeconds int64              `json:"ceiling_seconds"`
	Actionable     int64              `json:"actionable"`
	Blocked        int64              `json:"blocked"`
	Classes        []undeliveredClass `json:"classes"`
}

// dispatchWorkgraphListUndelivered is read-only. It counts the rows the
// reconciler's undelivered sweep (internal/joboutbox/undelivered_repair.go)
// acts on, with the SAME classification SQL, so an operator can see the
// stuck class -- pending behind a completion fence that cannot or did not
// arrive, or dead while its work-graph request stayed pending -- before and
// after the sweep runs. `blocked` rows are inside the ceiling and are left
// alone; every other reason is terminalized on the next reconciler pass.
func dispatchWorkgraphListUndelivered(ctx context.Context, runtime *operatorRuntime, args []string, stdout, stderr io.Writer) int {
	flags := quietFlags("workgraph list-undelivered")
	ceilingHours := flags.Int("ceiling-hours", int(joboutbox.DefaultUndeliveredCeiling/time.Hour),
		"age after which a fenced row counts as prerequisite_expired; defaults to the reconciler's ceiling")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *ceilingHours < 1 || *ceilingHours > 24*90 {
		return writeError(stderr, "invalid_request")
	}
	if runtime == nil || runtime.pools == nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	now := time.Now().UTC()
	ceiling := time.Duration(*ceilingHours) * time.Hour
	rows, err := runtime.pools.Domain.Query(ctx, joboutbox.UndeliveredReportSQL, now.Add(-ceiling))
	if err != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	defer rows.Close()
	report := undeliveredReport{CeilingSeconds: int64(ceiling.Seconds()), Classes: []undeliveredClass{}}
	for rows.Next() {
		var class undeliveredClass
		var oldest time.Time
		if err := rows.Scan(&class.JobKind, &class.Reason, &class.Count, &oldest); err != nil {
			return writeError(stderr, "operator_backend_unavailable")
		}
		class.OldestAgeSeconds = int64(now.Sub(oldest).Seconds())
		if class.Reason == "blocked" {
			report.Blocked += class.Count
		} else {
			report.Actionable += class.Count
		}
		report.Classes = append(report.Classes, class)
	}
	if rows.Err() != nil {
		return writeError(stderr, "operator_backend_unavailable")
	}
	return writeResult(stdout, stderr, report)
}
