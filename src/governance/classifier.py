"""Data classification and PII detection for hydra-ingest."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import structlog
from pydantic import BaseModel, Field

logger = structlog.get_logger(__name__)

# --- Regex patterns (compiled once) ---
_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
)
# US phone: optional +1, separators flexible
_PHONE_RE = re.compile(
    r"(?:\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})\b",
)
_SSN_RE = re.compile(
    r"\b(?!000|666|9\d{2})\d{3}[-\s]?(?!00)\d{2}[-\s]?(?!0000)\d{4}\b",
)
# Luhn-friendly credit card (13–19 digits, optional separators)
_CC_RE = re.compile(
    r"\b(?:\d[ -]*?){13,19}\b",
)
_IP_RE = re.compile(
    r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}"
    r"(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b",
)


class DataClassification(str, Enum):
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class PIIType(str, Enum):
    EMAIL = "email"
    PHONE = "phone"
    SSN = "ssn"
    NAME = "name"
    ADDRESS = "address"
    DOB = "dob"
    CREDIT_CARD = "credit_card"
    IP_ADDRESS = "ip_address"


class ClassificationResult(BaseModel):
    source_id: str
    classification: DataClassification
    pii_fields: dict[str, PIIType] = Field(default_factory=dict)
    scanned_columns: int = 0
    scanned_rows: int = 0
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DataClassifier:
    """Heuristic + regex PII scanner over column names and sampled values."""

    _COLUMN_HINTS: dict[str, PIIType] = {
        "email": PIIType.EMAIL,
        "e_mail": PIIType.EMAIL,
        "mail": PIIType.EMAIL,
        "phone": PIIType.PHONE,
        "mobile": PIIType.PHONE,
        "tel": PIIType.PHONE,
        "telephone": PIIType.PHONE,
        "ssn": PIIType.SSN,
        "social_security": PIIType.SSN,
        "socialsecurity": PIIType.SSN,
        "name": PIIType.NAME,
        "first_name": PIIType.NAME,
        "last_name": PIIType.NAME,
        "fullname": PIIType.NAME,
        "full_name": PIIType.NAME,
        "address": PIIType.ADDRESS,
        "street": PIIType.ADDRESS,
        "zip": PIIType.ADDRESS,
        "postal": PIIType.ADDRESS,
        "dob": PIIType.DOB,
        "date_of_birth": PIIType.DOB,
        "birthdate": PIIType.DOB,
        "birth_date": PIIType.DOB,
        "credit_card": PIIType.CREDIT_CARD,
        "cc_number": PIIType.CREDIT_CARD,
        "card_number": PIIType.CREDIT_CARD,
        "ip": PIIType.IP_ADDRESS,
        "ip_address": PIIType.IP_ADDRESS,
        "client_ip": PIIType.IP_ADDRESS,
    }

    def classify_records(
        self,
        source_id: str,
        records: list[dict[str, Any]],
        governance_config: dict[str, Any],
    ) -> ClassificationResult:
        if not records:
            config_raw = str(governance_config.get("classification", "internal")).lower()
            classification = self._parse_config_classification(config_raw)
            return ClassificationResult(
                source_id=source_id,
                classification=classification,
                pii_fields={},
                scanned_columns=0,
                scanned_rows=0,
            )

        columns = sorted({k for row in records for k in row.keys()})
        pii_found: dict[str, PIIType] = {}
        for col in columns:
            values = [row.get(col) for row in records if col in row]
            detected = self._detect_pii(col, values)
            if detected is not None:
                pii_found[col] = detected

        config_raw = str(governance_config.get("classification", "internal")).lower()
        classification = self._determine_classification(pii_found, config_raw)

        return ClassificationResult(
            source_id=source_id,
            classification=classification,
            pii_fields=pii_found,
            scanned_columns=len(columns),
            scanned_rows=len(records),
        )

    def _parse_config_classification(self, raw: str) -> DataClassification:
        try:
            return DataClassification(raw)
        except ValueError:
            logger.warning("classifier.unknown_config_classification", configured=raw)
            return DataClassification.INTERNAL

    def _detect_pii(self, column_name: str, values: list[Any]) -> PIIType | None:
        key = column_name.lower().replace(" ", "_").replace("-", "_")
        for hint, pii in self._COLUMN_HINTS.items():
            if hint in key or key == hint:
                return pii

        sample = [v for v in values if v is not None and str(v).strip()][:500]
        if not sample:
            return None

        for v in sample:
            s = str(v).strip()
            if _EMAIL_RE.search(s):
                return PIIType.EMAIL
            if _PHONE_RE.search(s) and len(re.sub(r"\D", "", s)) >= 10:
                return PIIType.PHONE
            if _SSN_RE.search(s):
                return PIIType.SSN
            if _IP_RE.search(s):
                return PIIType.IP_ADDRESS
            if self._looks_like_credit_card(s):
                return PIIType.CREDIT_CARD
        return None

    @staticmethod
    def _looks_like_credit_card(s: str) -> bool:
        digits = re.sub(r"\D", "", s)
        if not (13 <= len(digits) <= 19):
            return False
        # Luhn check
        total = 0
        reverse = digits[::-1]
        for i, ch in enumerate(reverse):
            n = int(ch)
            if i % 2 == 1:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        return total % 10 == 0

    def _determine_classification(
        self,
        pii_found: dict[str, PIIType],
        config_classification: str,
    ) -> DataClassification:
        raw = config_classification.lower().strip()
        if pii_found and raw == DataClassification.PUBLIC.value:
            logger.warning(
                "classifier.auto_escalate_public_with_pii",
                pii_columns=list(pii_found.keys()),
                new_classification=DataClassification.CONFIDENTIAL.value,
            )
            return DataClassification.CONFIDENTIAL
        return self._parse_config_classification(raw)
