from __future__ import annotations

from fastapi import APIRouter

from .audit_logs import router as audit_logs_router

router = APIRouter()
router.include_router(audit_logs_router)
