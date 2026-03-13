from __future__ import annotations

from fastapi import APIRouter

from .audit_logs import router as audit_logs_router
from .ip_allowlist import router as ip_allowlist_router
from .retention import router as retention_router

router = APIRouter()
router.include_router(audit_logs_router)
router.include_router(ip_allowlist_router)
router.include_router(retention_router)
