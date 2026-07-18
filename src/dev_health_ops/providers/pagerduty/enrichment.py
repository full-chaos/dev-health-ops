from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class PagerDutyEnrichmentToggles:
    alerts: bool = True
    log_entries: bool = True
    notes: bool = True

    def enabled(self, dataset_key: str) -> bool:
        match dataset_key:
            case "incident-alerts":
                return self.alerts
            case "incident-log-entries":
                return self.log_entries
            case "incident-notes":
                return self.notes
            case _:
                return True
