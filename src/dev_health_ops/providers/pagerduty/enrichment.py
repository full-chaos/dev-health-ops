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
                enabled = self.alerts
            case "incident-log-entries":
                enabled = self.log_entries
            case "incident-notes":
                enabled = self.notes
            case _:
                enabled = True
        return enabled

    @classmethod
    def from_dataset_options(
        cls, dataset_key: str, options: Mapping[str, object]
    ) -> PagerDutyEnrichmentToggles:
        enabled = options.get("enabled")
        match enabled:
            case bool() as is_enabled:
                match dataset_key:
                    case "incident-alerts":
                        toggles = cls(alerts=is_enabled)
                    case "incident-log-entries":
                        toggles = cls(log_entries=is_enabled)
                    case "incident-notes":
                        toggles = cls(notes=is_enabled)
                    case _:
                        toggles = cls()
            case _:
                toggles = cls()
        return toggles
