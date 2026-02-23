#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import time
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import requests


def _load_lookup_area():
    try:
        from entsoe.mappings import lookup_area
    except Exception as exc:
        raise SystemExit(
            "Could not import entsoe.mappings.lookup_area. Install entsoe-py or vendor entsoe/mappings.py"
        ) from exc
    return lookup_area


def load_zone_names(geojson_dir: Path) -> Set[str]:
    if not geojson_dir.exists() or not geojson_dir.is_dir():
        raise SystemExit(f"GeoJSON directory not found: {geojson_dir}")

    zones: Set[str] = set()
    for path in sorted(geojson_dir.glob("*.geojson")):
        with path.open("r", encoding="utf-8") as f:
            doc = json.load(f)
        if doc.get("type") != "FeatureCollection":
            continue
        for ft in doc.get("features", []):
            name = str((ft.get("properties") or {}).get("zoneName") or "").strip()
            if name:
                zones.add(name)

    if not zones:
        raise SystemExit(f"No zoneName found under: {geojson_dir}")
    return zones


def fetch_csv(url: str, timeout: int = 30) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text


def parse_active_eic_codes(csv_text: str) -> Set[str]:
    lines = csv_text.splitlines()
    if not lines:
        return set()

    # ENTSO-E CSV currently uses ';' delimiter and has EicCode/EicStatus columns.
    sample = "\n".join(lines[:5])
    delimiter = ";" if sample.count(";") >= sample.count(",") else ","

    reader = csv.DictReader(io.StringIO(csv_text), delimiter=delimiter)
    if not reader.fieldnames:
        return set()

    # Handle possible BOM
    normalized = {name.strip().lstrip("\ufeff"): name for name in reader.fieldnames}
    code_col = normalized.get("EicCode") or normalized.get("eic") or normalized.get("code")
    status_col = normalized.get("EicStatus") or normalized.get("status")
    if not code_col or not status_col:
        raise SystemExit(f"CSV missing required columns. Found: {reader.fieldnames}")

    active: Set[str] = set()
    for row in reader:
        code = str(row.get(code_col) or "").strip()
        status = str(row.get(status_col) or "").strip().lower()
        if code and status == "active":
            active.add(code)
    return active


def validate(geojson_dir: Path, csv_url: str) -> Tuple[List[str], Dict[str, str]]:
    lookup_area = _load_lookup_area()
    zones = load_zone_names(geojson_dir)

    zone_to_eic: Dict[str, str] = {}
    for zone in sorted(zones):
        area = lookup_area(zone)
        code = str(getattr(area, "value", "")).strip()
        if not code:
            raise SystemExit(f"No EIC mapping for zoneName={zone}")
        zone_to_eic[zone] = code

    active_codes = parse_active_eic_codes(fetch_csv(csv_url))
    missing = [f"{zone} -> {eic}" for zone, eic in zone_to_eic.items() if eic not in active_codes]
    return missing, zone_to_eic


def default_geojson_dir() -> Path:
    here = Path(__file__).resolve().parent.parent
    cands = [
        here / "entsoe" / "geo" / "geojson",
        here / "entsoe" / "geo" / "geojson",
    ]
    for p in cands:
        if p.is_dir():
            return p
    return cands[0]


def run_once(geojson_dir: Path, csv_url: str) -> int:
    missing, zone_to_eic = validate(geojson_dir, csv_url)
    print(f"[ok] zones loaded: {len(zone_to_eic)}")
    print(f"[ok] checked against: {csv_url}")
    if missing:
        print("[fail] Missing/inactive EIC codes:")
        for line in missing:
            print(f"  - {line}")
        return 1
    print("[ok] all zone EICs exist and are Active in registry")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate zoneName->EIC mappings against ENTSO-E EIC registry CSV")
    parser.add_argument(
        "--geojson-dir",
        type=Path,
        default=default_geojson_dir(),
        help="Directory containing ENTSO-E zone GeoJSON files",
    )
    parser.add_argument(
        "--csv-url",
        default="https://eepublicdownloads.blob.core.windows.net/cio-lio/csv/CIO_extraction-all-eic-codes.csv",
        help="ENTSO-E EIC registry CSV URL",
    )
    parser.add_argument("--interval-s", type=int, default=0, help="If >0, run periodically every N seconds")
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.interval_s <= 0:
        return run_once(args.geojson_dir, args.csv_url)

    while True:
        rc = run_once(args.geojson_dir, args.csv_url)
        if rc != 0:
            return rc
        time.sleep(args.interval_s)


if __name__ == "__main__":
    sys.exit(main())
