// Package adminllmvenue holds the venue differential oracles for the admin
// LLM settings reads that are not plain settings rows: the BYO budget
// (TestAdminLLMBudgetVenueOracle) compared as raw response text between the
// real Python api and the real dho api. It is its own package so the venue run
// has its own time budget, apart from the other admin venues.
package adminllmvenue
