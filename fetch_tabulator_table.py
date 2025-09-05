import json
import os
import sys
from typing import Dict, List, Any, Tuple, Optional

import requests
import pandas as pd


# ArcGIS Feature Service layer candidates powering the Tabulator table (from HAR)
CANDIDATE_LAYER_URLS = [
    # Generic, high-traffic item
    "https://services1.arcgis.com/IwZZTMxZCmAmFYvF/arcgis/rest/services/DPWH_Flood_Control_Projects/FeatureServer/0",
    # App-specific corrected-coordinates item
    "https://services1.arcgis.com/IwZZTMxZCmAmFYvF/arcgis/rest/services/FloodControl_Data_20250802_v6_corrected_coordinates_for_uploading/FeatureServer/0",
]

OUT_DIR = os.path.join("data")
OUT_JSON = os.path.join(OUT_DIR, "Flood Control Projects Raw.json")
OUT_CSV = os.path.join(OUT_DIR, "Flood Control Projects Raw.csv")
OUT_JSON_FULL = os.path.join(OUT_DIR, "Flood Control Projects Full.json")
OUT_CSV_FULL = os.path.join(OUT_DIR, "Flood Control Projects Full.csv")


def _ensure_out_dir():
    os.makedirs(OUT_DIR, exist_ok=True)


def get_layer_info(layer_url: str) -> Dict[str, Any]:
    resp = requests.get(f"{layer_url}?f=json", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_all_features(layer_url: str, object_id_field: Optional[str]) -> List[Dict[str, Any]]:
    # ArcGIS pagination with resultOffset/resultRecordCount
    # We'll request attributes only (returnGeometry=false) because Latitude/Longitude exist as fields
    all_features: List[Dict[str, Any]] = []
    result_offset = 0
    page_size = 2000  # will be constrained by maxRecordCount

    # Determine maxRecordCount from service, fall back to 2000
    try:
        info = get_layer_info(layer_url)
        max_rc = info.get("maxRecordCount") or info.get("standardMaxRecordCount")
        if isinstance(max_rc, int) and max_rc > 0:
            page_size = min(page_size, max_rc)
    except Exception:
        pass

    # Request all fields for reliability; service enforces actual fields
    out_fields_param = "*"
    order_by = object_id_field or "OBJECTID"

    while True:
        params = {
            "f": "json",
            "where": "1=1",
            "outFields": out_fields_param,
            "returnGeometry": "false",
            "orderByFields": f"{order_by} ASC",
            "resultOffset": result_offset,
            "resultRecordCount": page_size,
        }
        resp = requests.get(f"{layer_url}/query", params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        if not features:
            break
        all_features.extend(features)
        # Advance offset; stop if under page size
        result_offset += len(features)
        if len(features) < page_size:
            break
    return all_features


def map_to_schema(attrs: Dict[str, Any], object_id: Optional[Any] = None) -> Dict[str, Any]:
    # Map ArcGIS attributes to the repo's schema
    # Fallbacks applied where appropriate
    def pick_cost(a: Dict[str, Any]) -> Any:
        return (
            a.get("ContractCost_String")
            or a.get("ABC_String")
            or a.get("ContractCost")
            or a.get("ABC")
        )

    funding_year = attrs.get("FundingYear") or attrs.get("infra_year")
    # Report year absent in layer; align with funding year if available
    report_year = funding_year

    # Prefer already-populated values (when backfilled earlier), then fall back to fields
    lat = attrs.get("lat") if attrs.get("lat") is not None else attrs.get("Latitude")
    lng = attrs.get("lng") if attrs.get("lng") is not None else attrs.get("Longitude")
    # Normalize zero-ish values to None
    if isinstance(lat, (int, float)) and abs(lat) < 1e-9:
        lat = None
    if isinstance(lng, (int, float)) and abs(lng) < 1e-9:
        lng = None

    return {
        "object_id": object_id,
        "project_id": str(attrs.get("ProjectID") or "").strip() or None,
        "description": attrs.get("ProjectDescription"),
        "location": attrs.get("Province"),
        "contractor": attrs.get("Contractor"),
        "cost": pick_cost(attrs),
        "completion_date": attrs.get("CompletionDateActual"),
        "report_contract_id": attrs.get("ContractID"),
        "start_date": attrs.get("StartDate"),
        "project_type": attrs.get("TypeofWork"),
        "funding_year": funding_year,
        "report_year": report_year,
        "region": attrs.get("Region"),
        "lat": lat,
        "lng": lng,
    }


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[Any] = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        key = r.get("object_id")
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def write_outputs(rows: List[Dict[str, Any]]):
    _ensure_out_dir()

    # JSON
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    # CSV (column order aligned to existing file)
    cols = [
        "project_id",
        "description",
        "location",
        "contractor",
        "cost",
        "completion_date",
        "report_contract_id",
        "start_date",
        "project_type",
        "funding_year",
        "report_year",
        "region",
        "lat",
        "lng",
    ]
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(OUT_CSV, index=False)


def write_full_outputs(full_rows: List[Dict[str, Any]]):
    _ensure_out_dir()
    # JSON
    with open(OUT_JSON_FULL, "w", encoding="utf-8") as f:
        json.dump(full_rows, f, ensure_ascii=False, indent=2)
    # CSV: union of all keys for stable columns
    all_keys: List[str] = []
    seen = set()
    for r in full_rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                all_keys.append(k)
    pd.DataFrame(full_rows, columns=all_keys).to_csv(OUT_CSV_FULL, index=False)


def _chunked(seq: List[Any], size: int) -> List[List[Any]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def fetch_geometries_by_object_ids(
    layer_url: str, oid_field: str, object_ids: List[Any]
) -> Dict[Any, Dict[str, Any]]:
    """Return mapping of object_id -> geometry dict in WGS84."""
    geom_map: Dict[Any, Dict[str, Any]] = {}
    if not object_ids:
        return geom_map
    # Query in chunks to avoid URL length limits
    for chunk in _chunked(object_ids, 500):
        params = {
            "f": "json",
            "objectIds": ",".join(map(str, chunk)),
            "outFields": oid_field,
            "returnGeometry": "true",
            "outSR": 4326,
        }
        resp = requests.get(f"{layer_url}/query", params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for feat in data.get("features", []):
            attrs = feat.get("attributes", {})
            oid = attrs.get(oid_field)
            geom = feat.get("geometry")
            if oid is not None and isinstance(geom, dict):
                geom_map[oid] = geom
    return geom_map


def main():
    last_error: Optional[str] = None
    feats: List[Dict[str, Any]] = []
    chosen_layer: Optional[str] = None
    already_mapped = False
    full_rows_ready: Optional[List[Dict[str, Any]]] = None

    for layer_url in CANDIDATE_LAYER_URLS:
        try:
            print(f"[ArcGIS] Fetching layer info: {layer_url}", file=sys.stderr)
            info = get_layer_info(layer_url)
            oid_field = (
                info.get("objectIdField")
                or info.get("objectIdFieldName")
                or info.get("fields", [{}])[0].get("name")
            )
            print(
                f"Service name: {info.get('name')} | maxRecordCount: {info.get('maxRecordCount')} | oid: {oid_field}",
                file=sys.stderr,
            )
            print("[ArcGIS] Querying features with pagination…", file=sys.stderr)
            feats = fetch_all_features(layer_url, oid_field)
            print(f"[ArcGIS] Retrieved features: {len(feats)}", file=sys.stderr)
            if feats:
                chosen_layer = layer_url
                # Backfill lat/lng from geometry for rows where missing
                # Build minimal rows and full attribute rows in parallel
                src_attrs = [f.get("attributes", {}).copy() for f in feats]
                object_ids = [
                    a.get("OBJECTID") or a.get("ObjectId") or a.get(oid_field) for a in src_attrs
                ]
                rows = [map_to_schema(a, oid) for a, oid in zip(src_attrs, object_ids)]
                full_rows = []
                for a, r in zip(src_attrs, rows):
                    fr = a.copy()
                    # add handy fields
                    fr.setdefault("object_id", r.get("object_id"))
                    fr.setdefault("lat", r.get("lat"))
                    fr.setdefault("lng", r.get("lng"))
                    full_rows.append(fr)
                missing = [r for r in rows if r.get("lat") is None or r.get("lng") is None]
                if missing:
                    print(
                        f"[ArcGIS] Backfilling geometry for {len(missing)} rows with null lat/lng…",
                        file=sys.stderr,
                    )
                    miss_ids = [r.get("object_id") for r in missing if r.get("object_id") is not None]
                    geom_map = fetch_geometries_by_object_ids(layer_url, oid_field, miss_ids)
                    # Apply backfill
                    for r, fr in zip(rows, full_rows):
                        if (r.get("lat") is None or r.get("lng") is None) and r.get("object_id") in geom_map:
                            g = geom_map[r["object_id"]]
                            # Expect point geometry {x, y}
                            if "x" in g and "y" in g:
                                r["lng"] = g.get("x")
                                r["lat"] = g.get("y")
                                fr["lng"] = g.get("x")
                                fr["lat"] = g.get("y")
                                # If numeric fields are present for Lat/Long but null, fill them too
                                if fr.get("Longitude") in (None, ""):
                                    fr["Longitude"] = g.get("x")
                                if fr.get("Latitude") in (None, ""):
                                    fr["Latitude"] = g.get("y")
                # Replace feats with rows mapped + backfilled, and proceed
                feats = [{"attributes": r} for r in rows]
                already_mapped = True
                full_rows_ready = full_rows
                break
        except Exception as e:
            last_error = str(e)
            continue

    if already_mapped:
        rows = [f.get("attributes", {}) for f in feats]
    else:
        rows = [
            map_to_schema(
                f.get("attributes", {}),
                f.get("attributes", {}).get("object_id")
                or f.get("attributes", {}).get("OBJECTID")
                or f.get("attributes", {}).get("ObjectId"),
            )
            for f in feats
        ]
    rows = dedupe_rows(rows)
    print(f"[ArcGIS] Rows after mapping + dedupe: {len(rows)}", file=sys.stderr)

    write_outputs(rows)
    # Also write full outputs when possible
    if full_rows_ready is None:
        # Build full rows from feats if we didn't already
        full_rows_ready = []
        for f, r in zip(feats, rows):
            attrs = f.get("attributes", {}).copy()
            attrs.setdefault("object_id", r.get("object_id"))
            attrs.setdefault("lat", r.get("lat"))
            attrs.setdefault("lng", r.get("lng"))
            full_rows_ready.append(attrs)
    write_full_outputs(full_rows_ready)
    if chosen_layer:
        print(
            f"[Done] From {chosen_layer}\nWrote {len(rows)} rows to:\n- {OUT_JSON}\n- {OUT_CSV}\nAnd full exports:\n- {OUT_JSON_FULL}\n- {OUT_CSV_FULL}",
            file=sys.stderr,
        )
    else:
        msg = last_error or "no features returned"
        print(
            f"[Done] Wrote {len(rows)} rows (no candidate layer returned features; last error: {msg})\n- {OUT_JSON}\n- {OUT_CSV}\nAnd full exports:\n- {OUT_JSON_FULL}\n- {OUT_CSV_FULL}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
