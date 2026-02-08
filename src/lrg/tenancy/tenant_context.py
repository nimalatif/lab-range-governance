from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any


@dataclass(frozen=True)
class TenantPaths:
    raw_root: str
    canonical_root: str
    derived_root: str


def build_tenant_paths(base_cfg: Dict[str, Any], tenant_id: str) -> TenantPaths:
    storage = base_cfg["storage"]
    raw_tpl = storage["raw"]["path_template"]
    can_tpl = storage["canonical"]["path_template"]
    der_tpl = storage["derived"]["path_template"]

    return TenantPaths(
        raw_root=raw_tpl.format(tenant_id=tenant_id, source_type="{source_type}"),
        canonical_root=can_tpl.format(tenant_id=tenant_id),
        derived_root=der_tpl.format(tenant_id=tenant_id),
    )
