import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


@dataclass(frozen=True)
class ReferenceInterval:
    interval_id: str
    analyte_code: str
    unit: str
    specimen_type: str
    age_bucket: str
    sex: str
    performing_lab_id: str
    policy_version: str
    valid_from_utc: str
    valid_to_utc: Optional[str]
    range: Dict[str, Any]
    metadata: Dict[str, Any]


def parse_jsonl(text: str) -> List[ReferenceInterval]:
    items: List[ReferenceInterval] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        items.append(
            ReferenceInterval(
                interval_id=obj["interval_id"],
                analyte_code=obj["analyte_code"],
                unit=obj["unit"],
                specimen_type=obj["specimen_type"],
                age_bucket=obj["age_bucket"],
                sex=obj["sex"],
                performing_lab_id=obj["performing_lab_id"],
                policy_version=obj["policy_version"],
                valid_from_utc=obj["valid_from_utc"],
                valid_to_utc=obj.get("valid_to_utc"),
                range=obj["range"],
                metadata=obj.get("metadata", {}),
            )
        )
    return items


def _parse_dt(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s).astimezone(timezone.utc)


def select_interval(
    intervals: Iterable[ReferenceInterval],
    *,
    analyte_code: str,
    unit: str,
    specimen_type: str,
    age_bucket: str,
    sex: str,
    performing_lab_id: str,
    policy_version: str,
    observation_time_utc: str,
) -> Optional[ReferenceInterval]:
    t = _parse_dt(observation_time_utc)

    candidates: List[ReferenceInterval] = []
    for it in intervals:
        if it.analyte_code != analyte_code:
            continue
        if it.unit != unit:
            continue
        if it.specimen_type != specimen_type:
            continue
        if it.age_bucket != age_bucket:
            continue
        if it.sex != sex:
            continue
        if it.performing_lab_id != performing_lab_id:
            continue
        if it.policy_version != policy_version:
            continue

        vf = _parse_dt(it.valid_from_utc)
        vt = _parse_dt(it.valid_to_utc) if it.valid_to_utc else None

        if t < vf:
            continue
        if vt and t >= vt:
            continue

        candidates.append(it)

    if not candidates:
        return None

    candidates.sort(key=lambda x: _parse_dt(x.valid_from_utc), reverse=True)
    return candidates[0]
