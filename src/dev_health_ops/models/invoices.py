from __future__ import annotations

import uuid

import sqlalchemy as sa
from sqlalchemy import JSON, Column, DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import relationship

from .git import GUID, Base


class Invoice(Base):
    __tablename__ = "invoices"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    org_id = Column(GUID(), ForeignKey("organizations.id"), nullable=False, index=True)
    subscription_id = Column(
        GUID(),
        ForeignKey("subscriptions.id"),
        nullable=True,
        index=True,
    )
    stripe_invoice_id = Column(Text, unique=True, nullable=False, index=True)
    stripe_customer_id = Column(Text, nullable=False, index=True)
    status = Column(Text, nullable=False)
    amount_due = Column(Integer, nullable=False)
    amount_paid = Column(Integer, server_default="0", nullable=False)
    amount_remaining = Column(Integer, server_default="0", nullable=False)
    currency = Column(Text, server_default="usd", nullable=False)
    period_start = Column(DateTime(timezone=True), nullable=True)
    period_end = Column(DateTime(timezone=True), nullable=True)
    hosted_invoice_url = Column(Text, nullable=True)
    pdf_url = Column(Text, nullable=True)
    payment_intent_id = Column(Text, nullable=True)
    finalized_at = Column(DateTime(timezone=True), nullable=True)
    paid_at = Column(DateTime(timezone=True), nullable=True)
    voided_at = Column(DateTime(timezone=True), nullable=True)
    attempt_count = Column(Integer, server_default="0", nullable=False)
    metadata_ = Column("metadata", JSON, server_default="{}", nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))
    updated_at = Column(DateTime(timezone=True), server_default=sa.text("now()"))

    line_items = relationship(
        "InvoiceLineItem",
        back_populates="invoice",
        cascade="all, delete-orphan",
    )


class InvoiceLineItem(Base):
    __tablename__ = "invoice_line_items"

    id = Column(GUID(), primary_key=True, default=uuid.uuid4)
    invoice_id = Column(
        GUID(),
        ForeignKey("invoices.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    stripe_line_item_id = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    amount = Column(Integer, nullable=False)
    quantity = Column(Integer, server_default="1", nullable=False)
    period_start = Column(DateTime(timezone=True), nullable=True)
    period_end = Column(DateTime(timezone=True), nullable=True)
    stripe_price_id = Column(Text, nullable=True)

    invoice = relationship("Invoice", back_populates="line_items")
