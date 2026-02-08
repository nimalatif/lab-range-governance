from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class CsvRow:
    # Minimal v1 CSV fields (you can extend later)
    tenant_id: str
    patient_id: str
    result_id: str
    test_code: str
    test_name: str
    value: Any
    unit: str
    timestamp_iso: str  # ISO 8601
    source_system_id: str
    performing_lab_id: Optional[str] = None
    ref_low: Optional[float] = None
    ref_high: Optional[float] = None
    ref_text: Optional[str] = None
    reported_flag: Optional[str] = None


def _as_iso_z(ts: str) -> str:
    # Accepts ISO strings; ensures Zulu formatting
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def to_canonical(row: CsvRow, environment: str, rules_version: str = "v1", mapping_version: str = "v1") -> Dict[str, Any]:
    ref_range = None
    if row.ref_text or (row.ref_low is not None) or (row.ref_high is not None):
        ref_range = {
            "low": row.ref_low,
            "high": row.ref_high,
            "text": row.ref_text,
            "unit": row.unit
        }
        # remove None keys for cleanliness
        ref_range = {k: v for k, v in ref_range.items() if v is not None}

    reported_flag = (row.reported_flag or "unknown").strip()
    if reported_flag not in {"H", "L", "N", "A", "U", "unknown"}:
        reported_flag = "unknown"

    record: Dict[str, Any] = {
        "tenant": {"tenant_id": row.tenant_id, "environment": environment},
        "result": {
            "result_id": row.result_id,
            "patient_id": row.patient_id,
            "effective_time": {"timestamp": _as_iso_z(row.timestamp_iso), "time_type": "result_reported"},
            "value": row.value,
            "unit": row.unit
        },
        "test": {"local_code": row.test_code, "local_name": row.test_name},
        "source": {"source_type": "CSV", "source_system_id": row.source_system_id},
        "provenance": {
            "raw_pointer": f"tenants/{row.tenant_id}/raw/CSV/{row.source_system_id}/{row.result_id}.json",
            "field_lineage": {
                "value": "RECEIVED",
                "unit": "RECEIVED",
                "reference_range": "RECEIVED" if ref_range else "MISSING",
                "reported_flag": "RECEIVED" if row.reported_flag else "MISSING"
            },
            "mapping_version": mapping_version,
            "rules_version": rules_version
        }
    }

    if row.performing_lab_id:
        record["source"]["performing_lab_id"] = row.performing_lab_id
    if ref_range:
        record["result"]["reference_range"] = ref_range
    if row.reported_flag:
        record["result"]["reported_flag"] = reported_flag

    return record
