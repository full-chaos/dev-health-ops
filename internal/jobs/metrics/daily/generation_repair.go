package daily

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const legacyRedriveGenerationPrefix = "redrive:"

type GenerationRepairRow struct {
	RunID         string `json:"run_id"`
	TargetDay     string `json:"target_day"`
	OldGeneration string `json:"old_generation"`
	NewGeneration string `json:"new_generation,omitempty"`
	FullOrgFlip   bool   `json:"full_org_flip,omitempty"`
	Outcome       string `json:"outcome"`
}

type GenerationRepairOutcome struct {
	Examined       int                   `json:"examined"`
	Repaired       int                   `json:"repaired"`
	WouldRepair    int                   `json:"would_repair"`
	NotProvable    int                   `json:"not_provable"`
	NotSettled     int                   `json:"not_settled"`
	Raced          int                   `json:"raced"`
	FullOrgFlipped int                   `json:"full_org_flipped"`
	Rows           []GenerationRepairRow `json:"rows"`
}

func provenFanoutGeneration(orgID, day string, createdAt time.Time, runID uuid.UUID) (string, bool) {
	return "", false
}

func (store *PostgresStore) RepairLegacyRedriveGenerations(
	ctx context.Context, orgID string, from, to time.Time, dryRun bool, reason string,
) (GenerationRepairOutcome, error) {
	return GenerationRepairOutcome{Rows: []GenerationRepairRow{}}, nil
}
