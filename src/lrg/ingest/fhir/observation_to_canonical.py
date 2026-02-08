from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class FhirObservation:
    tenant_id: str
    source_system_id: str
    observation_json: Dict[str, Any]  # raw FHIR Observation resource


def _as_iso_z(ts: str) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _get_obs_id(o: Dict[str, Any]) -> str:
    # Prefer resource id; fallback to identifier.value
    if isinstance(o.get("id"), str) and o["id"].strip():
        return o["id"].strip()
    for ident in o.get("identifier", []) or []:
        v = ident.get("value")
        if isinstance(v, str) and v.strip():
            return v.strip()
    return "TBD"


def _get_patient_id(o: Dict[str, Any]) -> str:
    subj = o.get("subject") or {}
    ref = subj.get("reference")
    if isinstance(ref, str) and ref.strip():
        # e.g., "Patient/123" -> "123"
        return ref.strip().split("/")[-1]
    ident = subj.get("identifier") or {}
    v = ident.get("value")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return "TBD"


def _get_effective_time(o: Dict[str, Any]) -> tuple[str, str]:
    # Choose the best available time in order
    if isinstance(o.get("effectiveDateTime"), str):
        return _as_iso_z(o["effectiveDateTime"]), "specimen_collected"
    if isinstance(o.get("effectiveInstant"), str):
        return _as_iso_z(o["effectiveInstant"]), "specimen_collected"
    eff = o.get("effectivePeriod") or {}
    if isinstance(eff.get("start"), str):
        return _as_iso_z(eff["start"]), "specimen_collected"
    if isinstance(o.get("issued"), str):
        return _as_iso_z(o["issued"]), "result_reported"
    return "1970-01-01T00:00:00Z", "unknown"


def _get_test_identity(o: Dict[str, Any]) -> tuple[str, str, Optional[str]]:
    # local_code/local_name from code; loinc if present
    code = o.get("code") or {}
    text = code.get("text")
    local_name = text.strip() if isinstance(text, str) and text.strip() else "TBD"

    local_code = "TBD"
    loinc = None

    for c in (code.get("coding") or []):
        system = (c.get("system") or "").strip()
        ccode = (c.get("code") or "").strip()
        disp = (c.get("display") or "").strip()

        if ccode and local_code == "TBD":
            local_code = ccode
            if disp and local_name == "TBD":
                local_name = disp

        # LOINC system
        if system == "http://loinc.org" and ccode:
            loinc = ccode

    return local_code, local_name, loinc


def _get_value_unit(o: Dict[str, Any]) -> tuple[Any, str]:
    vq = o.get("valueQuantity")
    if isinstance(vq, dict):
        val = vq.get("value", "TBD")
        # Prefer UCUM code if present, else human unit
        unit = (vq.get("code") or vq.get("unit") or "TBD")
        return val, str(unit)

    # Other FHIR value[x] shapes
    if "valueString" in o:
        return o.get("valueString"), "TBD"
    if "valueInteger" in o:
        return o.get("valueInteger"), "TBD"
    if "valueBoolean" in o:
        return o.get("valueBoolean"), "TBD"

    vcc = o.get("valueCodeableConcept")
    if isinstance(vcc, dict):
        if isinstance(vcc.get("text"), str) and vcc["text"].strip():
            return vcc["text"].strip(), "TBD"
        codings = vcc.get("coding") or []
        if codings:
            c0 = codings[0] or {}
            return c0.get("code") or c0.get("display") or "TBD", "TBD"

    return "TBD", "TBD"


def _get_reference_range(o: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rr_list = o.get("referenceRange") or []
    if not rr_list:
        return None
    rr0 = rr_list[0] or {}

    out: Dict[str, Any] = {}

    low = rr0.get("low")
    if isinstance(low, dict) and low.get("value") is not None:
        out["low"] = low["value"]

    high = rr0.get("high")
    if isinstance(high, dict) and high.get("value") is not None:
        out["high"] = high["value"]

    text = rr0.get("text")
    if isinstance(text, str) and text.strip():
        out["text"] = text.strip()

    # Prefer explicit range unit if present; otherwise leave it out
    unit = None
    if isinstance(low, dict):
        unit = low.get("code") or low.get("unit")
    if unit is None and isinstance(high, dict):
        unit = high.get("code") or high.get("unit")
    if unit:
        out["unit"] = str(unit)

    return out or None


def _map_interpretation_flag(o: Dict[str, Any]) -> Optional[str]:
    interp = o.get("interpretation") or []
    if not interp:
        return None
    cc = interp[0] or {}
    for c in (cc.get("coding") or []):
        code = (c.get("code") or "").strip().upper()
        if code in {"H", "L", "N", "A", "U"}:
            return code
        if code == "HH":
            return "H"
        if code == "LL":
            return "L"
    return None


def to_canonical(obs: FhirObservation, environment: str, rules_version: str = "v1", mapping_version: str = "v1") -> Dict[str, Any]:
    o = obs.observation_json
    obs_id = _get_obs_id(o)
    patient_id = _get_patient_id(o)
    ts, ts_type = _get_effective_time(o)
    value, unit = _get_value_unit(o)
    local_code, local_name, loinc = _get_test_identity(o)
    ref_range = _get_reference_range(o)
    flag = _map_interpretation_flag(o)

    record: Dict[str, Any] = {
        "tenant": {"tenant_id": obs.tenant_id, "environment": environment},
        "result": {
            "result_id": obs_id,
            "patient_id": patient_id,
            "effective_time": {"timestamp": ts, "time_type": ts_type},
            "value": value,
            "unit": unit
        },
        "test": {"local_code": local_code, "local_name": local_name},
        "source": {"source_type": "FHIR", "source_system_id": obs.source_system_id},
        "provenance": {
            "raw_pointer": f"tenants/{obs.tenant_id}/raw/FHIR/{obs.source_system_id}/{obs_id}.json",
            "field_lineage": {
                "value": "RECEIVED",
                "unit": "RECEIVED",
                "reference_range": "RECEIVED" if ref_range else "MISSING",
                "reported_flag": "RECEIVED" if flag else "MISSING"
            },
            "mapping_version": mapping_version,
            "rules_version": rules_version
        }
    }

    if loinc:
        record["test"]["loinc"] = loinc
    if ref_range:
        record["result"]["reference_range"] = ref_range
    if flag:
        record["result"]["reported_flag"] = flag

    return record
