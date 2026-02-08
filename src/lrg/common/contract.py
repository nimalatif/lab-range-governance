from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from jsonschema import Draft202012Validator


def load_contract(repo_root: Path) -> Draft202012Validator:
    contract_path = repo_root / "contracts" / "canonical" / "lab_result_record.v1.json"
    schema = json.loads(contract_path.read_text(encoding="utf-8"))
    return Draft202012Validator(schema)


def validate_record(validator: Draft202012Validator, record: Dict[str, Any]) -> None:
    errors = sorted(validator.iter_errors(record), key=lambda e: e.path)
    if errors:
        msg_lines = ["Canonical record failed validation:"]
        for e in errors[:10]:
            loc = ".".join([str(p) for p in e.path]) or "<root>"
            msg_lines.append(f"- {loc}: {e.message}")
        raise ValueError("\n".join(msg_lines))
