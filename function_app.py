import json
import os
import sys
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Make src/ importable on Azure (repo uses src/ layout)
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import azure.functions as func

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


def _utc_path_date() -> str:
    now = datetime.now(timezone.utc)
    return f"{now.year:04d}/{now.month:02d}/{now.day:02d}"


def _write_raw_event(
    dl,
    *,
    container: str,
    tenant_id: str,
    event_id: str,
    payload: dict,
    event_type: str,
) -> str:
    fs = dl.get_file_system_client(container)
    date_path = _utc_path_date()
    file_path = f"tenants/{tenant_id}/ingest_events/{event_type}/{date_path}/{event_id}.json"
    body = json.dumps(
        {"event_type": event_type, "payload": payload},
        ensure_ascii=False,
    ).encode("utf-8")
    fs.get_file_client(file_path).upload_data(body, overwrite=True)
    return file_path


def _datalake_client(account: str):
    from azure.identity import DefaultAzureCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    url = f"https://{account}.dfs.core.windows.net"
    return DataLakeServiceClient(account_url=url, credential=DefaultAzureCredential())


def _request_id(req: func.HttpRequest) -> str:
    rid = (req.headers.get("x-request-id") or "").strip()
    return rid if rid else uuid.uuid4().hex


def _audit_write(dl, *, container: str, tenant_id: str, request_id: str, event: dict) -> str:
    fs = dl.get_file_system_client(container)
    date_path = _utc_path_date()
    path = f"tenants/{tenant_id}/audit/{date_path}/{request_id}.json"
    body = json.dumps(event, ensure_ascii=False).encode("utf-8")
    fs.get_file_client(path).upload_data(body, overwrite=True)
    return path


def _audit_best_effort(
    *,
    dl,
    audit_container: str,
    tenant_id: str,
    request_id: str,
    event: dict,
) -> Optional[str]:
    try:
        if dl and audit_container and tenant_id and request_id:
            return _audit_write(
                dl,
                container=audit_container,
                tenant_id=tenant_id,
                request_id=request_id,
                event=event,
            )
    except Exception:
        pass
    return None


@app.route(route="healthz", methods=["GET"])
def healthz(req: func.HttpRequest) -> func.HttpResponse:
    rid = _request_id(req)
    return func.HttpResponse(
        json.dumps(
            {
                "status": "ok",
                "request_id": rid,
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            }
        ),
        status_code=200,
        mimetype="application/json",
    )


@app.route(route="ingest/csv", methods=["POST"])
def ingest_csv(req: func.HttpRequest) -> func.HttpResponse:
    rid = _request_id(req)
    t0 = time.time()

    dl = None
    tenant_id = ""
    audit_container = os.getenv("STORAGE_AUDIT_CONTAINER", "audit")

    try:
        from lrg.common.contract import load_contract, validate_record
        from lrg.ingest.csv.row_to_canonical import CsvRow, to_canonical
        from lrg.governance.runbook_loader import load_reference_intervals_from_adls
        from lrg.governance.reference_intervals import select_interval

        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Body must be a JSON object")

        env = os.getenv("ENV", "dev")
        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        row = CsvRow(
            tenant_id=tenant_id,
            patient_id=str(payload["patient_id"]),
            result_id=str(payload["result_id"]),
            test_code=str(payload["test_code"]),
            test_name=str(payload["test_name"]),
            value=payload["value"],
            unit=str(payload["unit"]),
            timestamp_iso=str(payload["timestamp_iso"]),
            source_system_id=str(payload.get("source_system_id", "api")),
            performing_lab_id=payload.get("performing_lab_id"),
            ref_low=payload.get("ref_low"),
            ref_high=payload.get("ref_high"),
            ref_text=payload.get("ref_text"),
            reported_flag=payload.get("reported_flag"),
        )

        record: Dict[str, Any] = to_canonical(
            row=row, environment=env, rules_version="v1", mapping_version="v1"
        )

        validator = load_contract(Path(__file__).resolve().parent)
        validate_record(validator, record)

        account = os.environ["DATALAKE_ACCOUNT"]
        canonical_container = os.getenv("STORAGE_CANONICAL_CONTAINER", "canonical")

        dl = _datalake_client(account)

        # Bronze (raw ingest event)
        raw_container = os.getenv("STORAGE_RAW_CONTAINER", "raw")
        raw_path = _write_raw_event(
            dl,
            container=raw_container,
            tenant_id=tenant_id,
            event_id=record["result"]["result_id"],
            payload=payload,
            event_type="ingest_csv",
        )

        # Silver (canonical)
        fs_canon = dl.get_file_system_client(canonical_container)
        date_path = _utc_path_date()
        tenant_prefix = f"tenants/{tenant_id}/lab_results/{date_path}"
        canonical_file_path = f"{tenant_prefix}/{record['result']['result_id']}.json"

        canon_file_client = fs_canon.get_file_client(canonical_file_path)
        canon_body = json.dumps(record, ensure_ascii=False).encode("utf-8")
        canon_file_client.upload_data(canon_body, overwrite=True)

        # --- Derived interpretation (Gold / AI-ready) ---
        ctx = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        specimen_type = str(ctx.get("specimen_type", "serum"))
        age_bucket = str(ctx.get("age_bucket", "adult"))
        sex = str(ctx.get("sex", "male"))
        performing_lab_id = str(
            ctx.get("performing_lab_id", payload.get("performing_lab_id", "LAB_A"))
        )
        policy_version = str(ctx.get("policy_version", "v1"))

        # analyte_code: allow direct override; else map common local codes -> LOINC (v1 minimal)
        analyte_code = None
        if isinstance(payload.get("analyte_code"), str) and payload["analyte_code"].strip():
            analyte_code = payload["analyte_code"].strip()
        elif isinstance(payload.get("loinc_code"), str) and payload["loinc_code"].strip():
            analyte_code = f"loinc:{payload['loinc_code'].strip()}"
        else:
            tc = str(payload.get("test_code", "")).strip().upper()
            local_to_loinc_v1 = {"GLU": "2345-7"}
            if tc in local_to_loinc_v1:
                analyte_code = f"loinc:{local_to_loinc_v1[tc]}"

        observation_time_utc = str(payload.get("timestamp_iso", "")).strip()
        if not observation_time_utc:
            observation_time_utc = (
                datetime.now(timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )

        value = record.get("result", {}).get("value", None)
        unit = record.get("result", {}).get("unit", None)

        derived_container = os.getenv("STORAGE_DERIVED_CONTAINER", "derived")
        fs_derived = dl.get_file_system_client(derived_container)

        interpretation = {
            "computed_flag": "U",
            "interval_id": None,
            "computed_by": "lrg.governance.v1",
            "computed_at_utc": datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
        }

        if analyte_code and unit is not None and value is not None:
            runbooks_container = os.getenv("STORAGE_RUNBOOKS_CONTAINER", "runbooks")
            intervals = load_reference_intervals_from_adls(
                account=account,
                container=runbooks_container,
                tenant_id=tenant_id,
            )

            it = select_interval(
                intervals,
                analyte_code=str(analyte_code),
                unit=str(unit),
                specimen_type=str(specimen_type),
                age_bucket=str(age_bucket),
                sex=str(sex),
                performing_lab_id=str(performing_lab_id),
                policy_version=str(policy_version),
                observation_time_utc=str(observation_time_utc),
            )

            if it and isinstance(it.range, dict) and it.range.get("type") == "numeric":
                low = it.range.get("low")
                high = it.range.get("high")
                if (
                    isinstance(low, (int, float))
                    and isinstance(high, (int, float))
                    and isinstance(value, (int, float))
                ):
                    if value < low:
                        interpretation["computed_flag"] = "L"
                    elif value > high:
                        interpretation["computed_flag"] = "H"
                    else:
                        interpretation["computed_flag"] = "N"
                    interpretation["interval_id"] = it.interval_id

        derived_record = {
            "tenant_id": tenant_id,
            "result_id": record["result"]["result_id"],
            "analyte_code": analyte_code,
            "unit": unit,
            "value": value,
            "observation_time_utc": observation_time_utc,
            "context": {
                "specimen_type": specimen_type,
                "age_bucket": age_bucket,
                "sex": sex,
                "performing_lab_id": performing_lab_id,
                "policy_version": policy_version,
            },
            "interpretation": interpretation,
            "source": {
                "raw_path": f"{raw_container}/{raw_path}",
                "canonical_path": f"{canonical_container}/{canonical_file_path}",
            },
        }

        derived_prefix = f"tenants/{tenant_id}/lab_results_interpreted/{date_path}"
        derived_file_path = f"{derived_prefix}/{record['result']['result_id']}.json"
        der_file_client = fs_derived.get_file_client(derived_file_path)
        der_body = json.dumps(derived_record, ensure_ascii=False).encode("utf-8")
        der_file_client.upload_data(der_body, overwrite=True)

        audit_path = _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "ingest/csv",
                "status": "ok",
                "duration_ms": int((time.time() - t0) * 1000),
                "raw_written_to": f"{raw_container}/{raw_path}",
                "canonical_written_to": f"{canonical_container}/{canonical_file_path}",
                "derived_written_to": f"{derived_container}/{derived_file_path}",
                "computed_flag": interpretation.get("computed_flag"),
                "interval_id": interpretation.get("interval_id"),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "request_id": rid,
                    "audit_written_to": f"{audit_container}/{audit_path}" if audit_path else None,
                    "raw_written_to": f"{raw_container}/{raw_path}",
                    "written_to": f"{canonical_container}/{canonical_file_path}",
                    "derived_written_to": f"{derived_container}/{derived_file_path}",
                    "computed_flag": interpretation["computed_flag"],
                    "interval_id": interpretation["interval_id"],
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "ingest/csv",
                "status": "error",
                "duration_ms": int((time.time() - t0) * 1000),
                "message": str(e),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )
        return func.HttpResponse(
            json.dumps({"status": "error", "request_id": rid, "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )


@app.route(route="ingest/fhir", methods=["POST"])
def ingest_fhir(req: func.HttpRequest) -> func.HttpResponse:
    rid = _request_id(req)
    t0 = time.time()

    dl = None
    tenant_id = ""
    audit_container = os.getenv("STORAGE_AUDIT_CONTAINER", "audit")

    try:
        from lrg.common.contract import load_contract, validate_record
        from lrg.ingest.fhir.observation_to_canonical import FhirObservation, to_canonical
        from lrg.governance.runbook_loader import load_reference_intervals_from_adls
        from lrg.governance.reference_intervals import select_interval

        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Body must be a JSON object")

        env = os.getenv("ENV", "dev")
        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        source_system_id = str(payload.get("source_system_id", "api"))
        obs_json = payload.get("observation")
        if not isinstance(obs_json, dict):
            raise ValueError("Missing 'observation' object (FHIR Observation JSON)")

        obs = FhirObservation(
            tenant_id=tenant_id,
            source_system_id=source_system_id,
            observation_json=obs_json,
        )

        record: Dict[str, Any] = to_canonical(
            obs, environment=env, rules_version="v1", mapping_version="v1"
        )

        validator = load_contract(Path(__file__).resolve().parent)
        validate_record(validator, record)

        account = os.environ["DATALAKE_ACCOUNT"]
        canonical_container = os.getenv("STORAGE_CANONICAL_CONTAINER", "canonical")

        dl = _datalake_client(account)

        raw_container = os.getenv("STORAGE_RAW_CONTAINER", "raw")
        raw_path = _write_raw_event(
            dl,
            container=raw_container,
            tenant_id=tenant_id,
            event_id=record["result"]["result_id"],
            payload=payload,
            event_type="ingest_fhir",
        )

        fs_canon = dl.get_file_system_client(canonical_container)

        date_path = _utc_path_date()
        tenant_prefix = f"tenants/{tenant_id}/lab_results/{date_path}"
        canonical_file_path = f"{tenant_prefix}/{record['result']['result_id']}.json"

        canon_file_client = fs_canon.get_file_client(canonical_file_path)
        canon_body = json.dumps(record, ensure_ascii=False).encode("utf-8")
        canon_file_client.upload_data(canon_body, overwrite=True)

        ctx = payload.get("context") if isinstance(payload.get("context"), dict) else {}
        specimen_type = str(ctx.get("specimen_type", "serum"))
        age_bucket = str(ctx.get("age_bucket", "adult"))
        sex = str(ctx.get("sex", "male"))
        performing_lab_id = str(ctx.get("performing_lab_id", "LAB_A"))
        policy_version = str(ctx.get("policy_version", "v1"))

        loinc_code = None
        code_obj = obs_json.get("code") if isinstance(obs_json.get("code"), dict) else {}
        codings = code_obj.get("coding") if isinstance(code_obj.get("coding"), list) else []
        for c in codings:
            if not isinstance(c, dict):
                continue
            system = str(c.get("system", "")).strip()
            code = str(c.get("code", "")).strip()
            if code and (system == "http://loinc.org" or system.endswith("/loinc")):
                loinc_code = code
                break
        if not loinc_code:
            for c in codings:
                if isinstance(c, dict) and str(c.get("code", "")).strip():
                    loinc_code = str(c.get("code")).strip()
                    break

        analyte_code = f"loinc:{loinc_code}" if loinc_code else None

        observation_time_utc = (
            str(obs_json.get("effectiveDateTime")).strip()
            if str(obs_json.get("effectiveDateTime", "")).strip()
            else str(record.get("result", {}).get("timestamp_iso", "")).strip()
        )
        if not observation_time_utc:
            observation_time_utc = (
                datetime.now(timezone.utc)
                .isoformat(timespec="seconds")
                .replace("+00:00", "Z")
            )

        value = record.get("result", {}).get("value", None)
        unit = record.get("result", {}).get("unit", None)

        derived_container = os.getenv("STORAGE_DERIVED_CONTAINER", "derived")
        fs_derived = dl.get_file_system_client(derived_container)

        interpretation = {
            "computed_flag": "U",
            "interval_id": None,
            "computed_by": "lrg.governance.v1",
            "computed_at_utc": datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
        }

        if analyte_code and unit is not None and value is not None:
            runbooks_container = os.getenv("STORAGE_RUNBOOKS_CONTAINER", "runbooks")
            intervals = load_reference_intervals_from_adls(
                account=account,
                container=runbooks_container,
                tenant_id=tenant_id,
            )

            it = select_interval(
                intervals,
                analyte_code=str(analyte_code),
                unit=str(unit),
                specimen_type=str(specimen_type),
                age_bucket=str(age_bucket),
                sex=str(sex),
                performing_lab_id=str(performing_lab_id),
                policy_version=str(policy_version),
                observation_time_utc=str(observation_time_utc),
            )

            if it and isinstance(it.range, dict) and it.range.get("type") == "numeric":
                low = it.range.get("low")
                high = it.range.get("high")
                if (
                    isinstance(low, (int, float))
                    and isinstance(high, (int, float))
                    and isinstance(value, (int, float))
                ):
                    if value < low:
                        interpretation["computed_flag"] = "L"
                    elif value > high:
                        interpretation["computed_flag"] = "H"
                    else:
                        interpretation["computed_flag"] = "N"
                    interpretation["interval_id"] = it.interval_id

        derived_record = {
            "tenant_id": tenant_id,
            "result_id": record["result"]["result_id"],
            "analyte_code": analyte_code,
            "unit": unit,
            "value": value,
            "observation_time_utc": observation_time_utc,
            "context": {
                "specimen_type": specimen_type,
                "age_bucket": age_bucket,
                "sex": sex,
                "performing_lab_id": performing_lab_id,
                "policy_version": policy_version,
            },
            "interpretation": interpretation,
            "source": {
                "raw_path": f"{raw_container}/{raw_path}",
                "canonical_path": f"{canonical_container}/{canonical_file_path}",
            },
        }

        derived_prefix = f"tenants/{tenant_id}/lab_results_interpreted/{date_path}"
        derived_file_path = f"{derived_prefix}/{record['result']['result_id']}.json"
        der_file_client = fs_derived.get_file_client(derived_file_path)
        der_body = json.dumps(derived_record, ensure_ascii=False).encode("utf-8")
        der_file_client.upload_data(der_body, overwrite=True)

        audit_path = _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "ingest/fhir",
                "status": "ok",
                "duration_ms": int((time.time() - t0) * 1000),
                "raw_written_to": f"{raw_container}/{raw_path}",
                "canonical_written_to": f"{canonical_container}/{canonical_file_path}",
                "derived_written_to": f"{derived_container}/{derived_file_path}",
                "computed_flag": interpretation.get("computed_flag"),
                "interval_id": interpretation.get("interval_id"),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "request_id": rid,
                    "audit_written_to": f"{audit_container}/{audit_path}" if audit_path else None,
                    "raw_written_to": f"{raw_container}/{raw_path}",
                    "written_to": f"{canonical_container}/{canonical_file_path}",
                    "derived_written_to": f"{derived_container}/{derived_file_path}",
                    "computed_flag": interpretation["computed_flag"],
                    "interval_id": interpretation["interval_id"],
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "ingest/fhir",
                "status": "error",
                "duration_ms": int((time.time() - t0) * 1000),
                "message": str(e),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )
        return func.HttpResponse(
            json.dumps({"status": "error", "request_id": rid, "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )


@app.route(route="governance/interval/resolve", methods=["POST"])
def resolve_interval(req: func.HttpRequest) -> func.HttpResponse:
    rid = _request_id(req)
    t0 = time.time()

    dl = None
    tenant_id = ""
    audit_container = os.getenv("STORAGE_AUDIT_CONTAINER", "audit")

    try:
        from lrg.governance.runbook_loader import load_reference_intervals_from_adls
        from lrg.governance.reference_intervals import select_interval

        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Body must be a JSON object")

        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        account = os.environ["DATALAKE_ACCOUNT"]
        dl = _datalake_client(account)

        runbooks_container = os.getenv("STORAGE_RUNBOOKS_CONTAINER", "runbooks")
        intervals = load_reference_intervals_from_adls(
            account=account,
            container=runbooks_container,
            tenant_id=tenant_id,
        )

        it = select_interval(
            intervals,
            analyte_code=str(payload["analyte_code"]),
            unit=str(payload["unit"]),
            specimen_type=str(payload["specimen_type"]),
            age_bucket=str(payload["age_bucket"]),
            sex=str(payload["sex"]),
            performing_lab_id=str(payload["performing_lab_id"]),
            policy_version=str(payload["policy_version"]),
            observation_time_utc=str(payload["observation_time_utc"]),
        )

        if not it:
            _audit_best_effort(
                dl=dl,
                audit_container=audit_container,
                tenant_id=tenant_id,
                request_id=rid,
                event={
                    "request_id": rid,
                    "route": "governance/interval/resolve",
                    "status": "not_found",
                    "duration_ms": int((time.time() - t0) * 1000),
                    "payload": payload,
                    "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                },
            )
            return func.HttpResponse(
                json.dumps({"status": "not_found", "request_id": rid}),
                status_code=404,
                mimetype="application/json",
            )

        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "governance/interval/resolve",
                "status": "ok",
                "duration_ms": int((time.time() - t0) * 1000),
                "interval_id": it.interval_id,
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )

        return func.HttpResponse(
            json.dumps({"status": "ok", "request_id": rid, "interval_id": it.interval_id, "range": it.range}),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "governance/interval/resolve",
                "status": "error",
                "duration_ms": int((time.time() - t0) * 1000),
                "message": str(e),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )
        return func.HttpResponse(
            json.dumps({"status": "error", "request_id": rid, "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )


@app.route(route="query/derived/today", methods=["GET"])
def query_derived_today(req: func.HttpRequest) -> func.HttpResponse:
    rid = _request_id(req)
    t0 = time.time()

    dl = None
    tenant_id = ""
    audit_container = os.getenv("STORAGE_AUDIT_CONTAINER", "audit")

    try:
        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        account = os.environ["DATALAKE_ACCOUNT"]
        derived_container = os.getenv("STORAGE_DERIVED_CONTAINER", "derived")
        dl = _datalake_client(account)
        fs = dl.get_file_system_client(derived_container)

        date_path = _utc_path_date()
        prefix = f"tenants/{tenant_id}/lab_results_interpreted/{date_path}/"

        limit_raw = req.params.get("limit", "25")
        limit = int(limit_raw) if str(limit_raw).isdigit() else 25

        names = []
        for p in fs.get_paths(path=prefix):
            if getattr(p, "is_directory", False):
                continue
            names.append(p.name)

        names = sorted(names, reverse=True)[:limit]

        items = []
        for name in names:
            data = fs.get_file_client(name).download_file().readall()
            rec = json.loads(data)
            interp = rec.get("interpretation", {}) if isinstance(rec.get("interpretation"), dict) else {}
            items.append(
                {
                    "path": f"{derived_container}/{name}",
                    "result_id": rec.get("result_id"),
                    "analyte_code": rec.get("analyte_code"),
                    "value": rec.get("value"),
                    "unit": rec.get("unit"),
                    "observation_time_utc": rec.get("observation_time_utc"),
                    "computed_flag": interp.get("computed_flag"),
                    "interval_id": interp.get("interval_id"),
                }
            )

        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "query/derived/today",
                "status": "ok",
                "duration_ms": int((time.time() - t0) * 1000),
                "count": len(items),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )

        return func.HttpResponse(
            json.dumps({"status": "ok", "request_id": rid, "count": len(items), "items": items}),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "query/derived/today",
                "status": "error",
                "duration_ms": int((time.time() - t0) * 1000),
                "message": str(e),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )
        return func.HttpResponse(
            json.dumps({"status": "error", "request_id": rid, "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )


@app.route(route="agent/explain/result", methods=["GET"])
def agent_explain_result(req: func.HttpRequest) -> func.HttpResponse:
    rid = _request_id(req)
    t0 = time.time()

    dl = None
    tenant_id = ""
    audit_container = os.getenv("STORAGE_AUDIT_CONTAINER", "audit")

    try:
        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        result_id = (req.params.get("result_id") or "").strip()
        if not result_id:
            raise ValueError("result_id query param is required")

        account = os.environ["DATALAKE_ACCOUNT"]
        derived_container = os.getenv("STORAGE_DERIVED_CONTAINER", "derived")
        dl = _datalake_client(account)
        fs = dl.get_file_system_client(derived_container)

        date_path = _utc_path_date()
        candidates = [
            f"tenants/{tenant_id}/lab_results_interpreted/{date_path}/{result_id}.json",
        ]

        data = None
        found_path = None
        for p in candidates:
            try:
                data = fs.get_file_client(p).download_file().readall()
                found_path = p
                break
            except Exception:
                pass

        if data is None:
            _audit_best_effort(
                dl=dl,
                audit_container=audit_container,
                tenant_id=tenant_id,
                request_id=rid,
                event={
                    "request_id": rid,
                    "route": "agent/explain/result",
                    "status": "not_found",
                    "duration_ms": int((time.time() - t0) * 1000),
                    "result_id": result_id,
                    "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                },
            )
            return func.HttpResponse(
                json.dumps(
                    {
                        "status": "not_found",
                        "request_id": rid,
                        "message": "Derived record not found (today)",
                        "result_id": result_id,
                    }
                ),
                status_code=404,
                mimetype="application/json",
            )

        rec = json.loads(data)
        interp = rec.get("interpretation", {}) if isinstance(rec.get("interpretation"), dict) else {}
        flag = interp.get("computed_flag", "U")
        interval_id = interp.get("interval_id")

        flag_meaning = {
            "L": "Low (below reference range)",
            "N": "Normal (within reference range)",
            "H": "High (above reference range)",
            "U": "Unknown (no applicable reference interval found)",
        }.get(flag, "Unknown")

        ctx = rec.get("context", {}) if isinstance(rec.get("context"), dict) else {}
        src = rec.get("source", {}) if isinstance(rec.get("source"), dict) else {}

        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "agent/explain/result",
                "status": "ok",
                "duration_ms": int((time.time() - t0) * 1000),
                "result_id": rec.get("result_id"),
                "computed_flag": flag,
                "interval_id": interval_id,
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )

        explanation = {
            "status": "ok",
            "request_id": rid,
            "result_id": rec.get("result_id"),
            "analyte_code": rec.get("analyte_code"),
            "value": rec.get("value"),
            "unit": rec.get("unit"),
            "observation_time_utc": rec.get("observation_time_utc"),
            "computed_flag": flag,
            "computed_flag_meaning": flag_meaning,
            "interval_id": interval_id,
            "context": ctx,
            "source_paths": src,
            "derived_path": f"{derived_container}/{found_path}",
        }

        return func.HttpResponse(
            json.dumps(explanation),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "agent/explain/result",
                "status": "error",
                "duration_ms": int((time.time() - t0) * 1000),
                "message": str(e),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )
        return func.HttpResponse(
            json.dumps({"status": "error", "request_id": rid, "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )


@app.route(route="agent/chat", methods=["POST"])
def agent_chat(req: func.HttpRequest) -> func.HttpResponse:
    rid = _request_id(req)
    t0 = time.time()

    dl = None
    tenant_id = ""
    audit_container = os.getenv("STORAGE_AUDIT_CONTAINER", "audit")

    try:
        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Body must be a JSON object")

        message = str(payload.get("message", "")).strip()
        if not message:
            raise ValueError("message is required")

        msg = message.lower()
        limit_raw = str(payload.get("limit", "25"))
        limit = int(limit_raw) if limit_raw.isdigit() else 25
        limit = max(1, min(limit, 200))

        tenant_id = os.getenv("TENANT_ID", "").strip()
        if not tenant_id:
            raise ValueError("TENANT_ID app setting is required")

        account = os.environ["DATALAKE_ACCOUNT"]
        derived_container = os.getenv("STORAGE_DERIVED_CONTAINER", "derived")
        dl = _datalake_client(account)
        fs = dl.get_file_system_client(derived_container)

        # Try to detect a result_id in the message (R1002, obs-glu-002, etc.)
        m = re.search(r"\b(R[0-9A-Za-z_][0-9A-Za-z_-]{2,}|obs-[a-z0-9-]+)\b", message, flags=re.IGNORECASE)
        result_id = m.group(1) if m else ""

        # ---------- LLM router (safe; falls back automatically) ----------
        intent_override = None
        routed_by = "rules"

        try:
            from lrg.agent.llm_router import route_intent

            routed = route_intent(message, default_limit=limit)
            if isinstance(routed, dict) and isinstance(routed.get("intent"), str):
                intent_override = routed["intent"]
                routed_by = "llm"

                if intent_override == "list_today":
                    lim = routed.get("limit", limit)
                    if not isinstance(lim, int):
                        lim = limit
                    limit = max(1, min(lim, 200))

                elif intent_override == "explain_result":
                    rid2 = routed.get("result_id")
                    if isinstance(rid2, str) and rid2.strip():
                        result_id = rid2.strip()
                    else:
                        intent_override = "help"

                elif intent_override == "help":
                    _audit_best_effort(
                        dl=dl,
                        audit_container=audit_container,
                        tenant_id=tenant_id,
                        request_id=rid,
                        event={
                            "request_id": rid,
                            "route": "agent/chat",
                            "intent": "help",
                            "status": "ok",
                            "router": routed_by,
                            "duration_ms": int((time.time() - t0) * 1000),
                            "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                        },
                    )
                    return func.HttpResponse(
                        json.dumps(
                            {
                                "status": "ok",
                                "request_id": rid,
                                "intent": "help",
                                "message": "Try: 'list today' OR 'explain R1002'.",
                                "available": [
                                    "GET /api/query/derived/today",
                                    "GET /api/agent/explain/result?result_id=R1002",
                                    "POST /api/agent/chat  {\"message\":\"list today\"}",
                                ],
                            }
                        ),
                        status_code=200,
                        mimetype="application/json",
                    )
        except Exception:
            intent_override = None
            routed_by = "rules"
        # ---------------------------------------------------------------

        # Intent 1: explain a specific result
        explain_keywords = ("explain" in msg or "why" in msg or "meaning" in msg or "what" in msg)
        if (intent_override == "explain_result") or (result_id and explain_keywords):
            date_path = _utc_path_date()
            p = f"tenants/{tenant_id}/lab_results_interpreted/{date_path}/{result_id}.json"
            try:
                data = fs.get_file_client(p).download_file().readall()
            except Exception:
                _audit_best_effort(
                    dl=dl,
                    audit_container=audit_container,
                    tenant_id=tenant_id,
                    request_id=rid,
                    event={
                        "request_id": rid,
                        "route": "agent/chat",
                        "intent": "explain_result",
                        "status": "not_found",
                        "router": routed_by,
                        "duration_ms": int((time.time() - t0) * 1000),
                        "result_id": result_id,
                        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                    },
                )
                return func.HttpResponse(
                    json.dumps(
                        {
                            "status": "not_found",
                            "request_id": rid,
                            "message": "Derived record not found (today)",
                            "result_id": result_id,
                        }
                    ),
                    status_code=404,
                    mimetype="application/json",
                )

            rec = json.loads(data)
            interp = rec.get("interpretation", {}) if isinstance(rec.get("interpretation"), dict) else {}
            flag = interp.get("computed_flag", "U")
            flag_meaning = {
                "L": "Low (below reference range)",
                "N": "Normal (within reference range)",
                "H": "High (above reference range)",
                "U": "Unknown (no applicable reference interval found)",
            }.get(flag, "Unknown")

            _audit_best_effort(
                dl=dl,
                audit_container=audit_container,
                tenant_id=tenant_id,
                request_id=rid,
                event={
                    "request_id": rid,
                    "route": "agent/chat",
                    "intent": "explain_result",
                    "status": "ok",
                    "router": routed_by,
                    "duration_ms": int((time.time() - t0) * 1000),
                    "result_id": rec.get("result_id"),
                    "computed_flag": flag,
                    "interval_id": interp.get("interval_id"),
                    "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                },
            )

            return func.HttpResponse(
                json.dumps(
                    {
                        "status": "ok",
                        "request_id": rid,
                        "intent": "explain_result",
                        "result_id": rec.get("result_id"),
                        "computed_flag": flag,
                        "computed_flag_meaning": flag_meaning,
                        "interval_id": interp.get("interval_id"),
                        "analyte_code": rec.get("analyte_code"),
                        "value": rec.get("value"),
                        "unit": rec.get("unit"),
                        "observation_time_utc": rec.get("observation_time_utc"),
                        "context": rec.get("context", {}),
                        "source_paths": rec.get("source", {}),
                        "derived_path": f"{derived_container}/{p}",
                    }
                ),
                status_code=200,
                mimetype="application/json",
            )

        # Intent 2: list today's derived
        if (intent_override == "list_today") or ("today" in msg) or ("list" in msg) or ("show" in msg):
            date_path = _utc_path_date()
            prefix = f"tenants/{tenant_id}/lab_results_interpreted/{date_path}/"

            names = []
            for p in fs.get_paths(path=prefix):
                if getattr(p, "is_directory", False):
                    continue
                names.append(p.name)

            names = sorted(names, reverse=True)[:limit]

            items = []
            for name in names:
                data = fs.get_file_client(name).download_file().readall()
                rec = json.loads(data)
                interp = rec.get("interpretation", {}) if isinstance(rec.get("interpretation"), dict) else {}
                items.append(
                    {
                        "path": f"{derived_container}/{name}",
                        "result_id": rec.get("result_id"),
                        "analyte_code": rec.get("analyte_code"),
                        "value": rec.get("value"),
                        "unit": rec.get("unit"),
                        "observation_time_utc": rec.get("observation_time_utc"),
                        "computed_flag": interp.get("computed_flag"),
                        "interval_id": interp.get("interval_id"),
                    }
                )

            _audit_best_effort(
                dl=dl,
                audit_container=audit_container,
                tenant_id=tenant_id,
                request_id=rid,
                event={
                    "request_id": rid,
                    "route": "agent/chat",
                    "intent": "list_today",
                    "status": "ok",
                    "router": routed_by,
                    "duration_ms": int((time.time() - t0) * 1000),
                    "count": len(items),
                    "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                },
            )

            return func.HttpResponse(
                json.dumps(
                    {
                        "status": "ok",
                        "request_id": rid,
                        "intent": "list_today",
                        "count": len(items),
                        "items": items,
                    }
                ),
                status_code=200,
                mimetype="application/json",
            )

        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "agent/chat",
                "intent": "help",
                "status": "ok",
                "router": routed_by,
                "duration_ms": int((time.time() - t0) * 1000),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )

        return func.HttpResponse(
            json.dumps(
                {
                    "status": "ok",
                    "request_id": rid,
                    "intent": "help",
                    "message": "Try: 'list today' OR 'explain R1002'.",
                    "available": [
                        "GET /api/query/derived/today",
                        "GET /api/agent/explain/result?result_id=R1002",
                        "POST /api/agent/chat  {\"message\":\"list today\"}",
                    ],
                }
            ),
            status_code=200,
            mimetype="application/json",
        )

    except Exception as e:
        _audit_best_effort(
            dl=dl,
            audit_container=audit_container,
            tenant_id=tenant_id,
            request_id=rid,
            event={
                "request_id": rid,
                "route": "agent/chat",
                "status": "error",
                "duration_ms": int((time.time() - t0) * 1000),
                "message": str(e),
                "ts_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
            },
        )
        return func.HttpResponse(
            json.dumps({"status": "error", "request_id": rid, "message": str(e)}),
            status_code=400,
            mimetype="application/json",
        )
