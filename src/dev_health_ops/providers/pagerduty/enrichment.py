from __future__ import annotations

from collections.abc import Mapping
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

    @classmethod
    def from_dataset_options(
        cls, dataset_key: str, options: Mapping[str, object]
    ) -> PagerDutyEnrichmentToggles:
        enabled = options.get("enabled")
        if not isinstance(enabled, bool):
            return cls()
        match dataset_key:
            case "incident-alerts":
                return cls(alerts=enabled)
            case "incident-log-entries":
                return cls(log_entries=enabled)
            case "incident-notes":
                return cls(notes=enabled)
            case _:
                return cls()
