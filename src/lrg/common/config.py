from __future__ import annotations
import os
import yaml
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict


@dataclass(frozen=True)
class RuntimeScope:
    tenant_id: str
    workspace_id: str | None
    environment: str  # dev/test/prod


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_base_config(repo_root: Path) -> Dict[str, Any]:
    return load_yaml(repo_root / "configs" / "base.yaml")


def read_scope_from_env(base_cfg: Dict[str, Any]) -> RuntimeScope:
    tenancy = base_cfg["tenancy"]
    tenant_id = os.getenv(tenancy["tenant_id_env"], "").strip()
    env = os.getenv(tenancy["environment_env"], "").strip()

    if not tenant_id:
        raise ValueError(f"Missing tenant id env var: {tenancy['tenant_id_env']}")
    if env not in {"dev", "test", "prod"}:
        raise ValueError(f"ENV must be one of dev/test/prod (got '{env}')")

    workspace_env = tenancy.get("workspace_id_env")
    workspace_id = os.getenv(workspace_env, "").strip() if workspace_env else ""
    return RuntimeScope(tenant_id=tenant_id, workspace_id=workspace_id or None, environment=env)
