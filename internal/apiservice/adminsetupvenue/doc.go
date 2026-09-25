// Package adminsetupvenue holds the venue differential oracle for
// GET /api/v1/admin/setup/status (TestAdminSetupStatusVenueOracle): the real
// Python api and the real dho api on two copies of one seeded Postgres,
// compared as raw response text across every first-run state. It is its own
// package so the venue run has its own time budget, apart from the other
// admin venues.
package adminsetupvenue
