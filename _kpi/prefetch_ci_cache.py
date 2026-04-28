#!/usr/bin/env python3
from __future__ import annotations

import argparse
import contextlib
import errno
import fcntl
import importlib.util
import json
import os
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Optional, Tuple

import requests


CI_CACHE_TMP_MAX_AGE_S = int(os.getenv("CI_CACHE_TMP_MAX_AGE_S", "3600"))


def to_iso_z(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def load_lookup_area():
    try:
        from entsoe.mappings import lookup_area
        return lookup_area
    except Exception:
        pass

    for mod in ("entose.mappings",):
        try:
            m = __import__(mod, fromlist=["lookup_area"])
            if callable(getattr(m, "lookup_area", None)):
                return m.lookup_area
        except Exception:
            pass

    env_path = os.getenv("BZ_MAPPINGS_PY")
    candidates = [Path(env_path)] if env_path else []
    here = Path(__file__).resolve().parent
    candidates.extend([
        (here.parent / "entsoe" / "mappings.py").resolve(),
        (here.parent / "entose" / "mappings.py").resolve(),
    ])
    for p in candidates:
        if not p.is_file():
            continue
        spec = importlib.util.spec_from_file_location("entsoe_mappings_vendored", p)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        fn = getattr(module, "lookup_area", None)
        if callable(fn):
            return fn

    raise RuntimeError("lookup_area not available")


def default_geojson_dir() -> Path:
    env = os.getenv("BZ_GEOJSON_DIR")
    if env:
        return Path(env)
    here = Path(__file__).resolve().parent
    for p in [
        (here.parent / "entsoe" / "geo" / "geojson").resolve(),
        (here.parent / "entose" / "geo" / "geojson").resolve(),
    ]:
        if p.is_dir():
            return p
    return (here.parent / "entsoe" / "geo" / "geojson").resolve()


def _iter_zone_points(geojson_dir: Path) -> Iterator[Tuple[str, float, float]]:
    for path in sorted(geojson_dir.glob("*.geojson")):
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if doc.get("type") != "FeatureCollection":
            continue
        for ft in doc.get("features", []):
            props = ft.get("properties") or {}
            zone_name = str(props.get("zoneName") or "").strip()
            geom = ft.get("geometry") or {}
            if not zone_name:
                continue

            coords = geom.get("coordinates")
            gtype = geom.get("type")
            ring = None
            if gtype == "Polygon" and isinstance(coords, list) and coords:
                ring = coords[0]
            elif gtype == "MultiPolygon" and isinstance(coords, list) and coords and coords[0]:
                ring = coords[0][0]
            if not isinstance(ring, list) or len(ring) < 4:
                continue

            pts = [(float(p[0]), float(p[1])) for p in ring if isinstance(p, list) and len(p) >= 2]
            if len(pts) < 3:
                continue
            # Simple representative point: arithmetic mean of ring vertices.
            core = pts[:-1] if pts[0] == pts[-1] else pts
            lon = sum(p[0] for p in core) / len(core)
            lat = sum(p[1] for p in core) / len(core)
            yield zone_name, lat, lon
            break


def _load_cache(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    if not text.strip():
        return {}
    data = json.loads(text)
    entries = data.get("entries") if isinstance(data, dict) else {}
    return entries if isinstance(entries, dict) else {}


def _cache_temp_prefix(path: Path) -> str:
    return f".{path.name}."


def _is_cache_temp_file(name: str, path: Path) -> bool:
    return (name.startswith(_cache_temp_prefix(path)) and name.endswith(".tmp")) or name.startswith("tmp")


def _cleanup_stale_cache_temp_files(path: Path, max_age_s: int = CI_CACHE_TMP_MAX_AGE_S) -> int:
    if not path.parent.exists():
        return 0
    now = time.time()
    removed = 0
    for item in path.parent.iterdir():
        if not _is_cache_temp_file(item.name, path):
            continue
        try:
            if not item.is_file() or now - item.stat().st_mtime < max_age_s:
                continue
            item.unlink()
            removed += 1
        except FileNotFoundError:
            continue
        except Exception as exc:
            print(f"[prefetch] failed to remove stale temp file {item}: {exc}")
    if removed:
        print(f"[prefetch] removed={removed} stale_temp_dir={path.parent}")
    return removed


@contextlib.contextmanager
def _cache_file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.parent / f".{path.name}.lock"
    with lock_path.open("a", encoding="utf-8") as lock_fh:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)


def _save_cache(path: Path, entries: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(str(path.parent), exist_ok=True)
    payload = {
        "saved_at": to_iso_z(datetime.now(timezone.utc)),
        "entries": entries,
    }
    tmp: Optional[str] = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            delete=False,
            dir=str(path.parent),
            prefix=_cache_temp_prefix(path),
            suffix=".tmp",
        ) as tf:
            tmp = tf.name
            json.dump(payload, tf, separators=(",", ":"))
            tf.flush()
            try:
                os.fsync(tf.fileno())
            except OSError as exc:
                if exc.errno not in (errno.EINVAL, errno.ENOTSUP, errno.EROFS):
                    raise
        os.replace(tmp, path)
        tmp = None
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except Exception:
                pass
    os.chmod(path, 0o644)


def _merge_save_cache(path: Path, updates: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    with _cache_file_lock(path):
        _cleanup_stale_cache_temp_files(path)
        entries = _load_cache(path)
        entries.update(updates)
        _save_cache(path, entries)
        return entries


def prefetch_once(geojson_dir: Path, cache_file: Path, aggregate: str = "true") -> int:
    lookup_area = load_lookup_area()
    wattnet_base = os.getenv("WATTNET_BASE") or os.getenv("WATTPRINT_BASE", "https://api.wattnet.eu")
    token = os.getenv("WATTNET_TOKEN") or os.getenv("WATTPRINT_TOKEN")
    if not token:
        raise RuntimeError("WATTNET_TOKEN/WATTPRINT_TOKEN is required")

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    # Prefetch only the latest interval ending "now" (no future window).
    start = now - timedelta(minutes=5)
    end = now
    params_json = json.dumps({"aggregate": aggregate}, sort_keys=True)

    headers = {"Accept": "application/json", "Authorization": f"Bearer {token}", "aggregate": aggregate}
    entries: Dict[str, Dict[str, Any]] = {}
    inserted = 0
    failed = 0

    for zone_name, lat, lon in _iter_zone_points(geojson_dir):
        try:
            bz_eic = str(lookup_area(zone_name).value)
        except Exception:
            failed += 1
            continue

        url = f"{wattnet_base}/v1/footprints"
        params = {
            "lat": lat,
            "lon": lon,
            "footprint_type": "carbon",
            "start": to_iso_z(start),
            "end": to_iso_z(end),
            "aggregate": aggregate,
        }
        try:
            r = requests.get(url, params=params, headers=headers, timeout=20)
            r.raise_for_status()
            data = r.json()
            payload = data[0] if isinstance(data, list) else data
            if not isinstance(payload, dict):
                failed += 1
                continue
        except Exception:
            failed += 1
            continue

        key = "|".join([f"region:{bz_eic}", to_iso_z(start), to_iso_z(end), params_json])
        entries[key] = {
            "payload": payload,
            "fetched_at": int(datetime.now(timezone.utc).timestamp()),
        }
        inserted += 1

    _merge_save_cache(cache_file, entries)
    print(f"[prefetch] updated={inserted} failed={failed} cache_file={cache_file}")
    return 0 if inserted > 0 else 1


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Prefetch CI payloads per bidding zone into local cache")
    parser.add_argument("--once", action="store_true", help="run once and exit")
    parser.add_argument("--geojson-dir", default=str(default_geojson_dir()))
    parser.add_argument("--cache-file", default=os.getenv("CI_CACHE_FILE", "/data/ci_cache.json"))
    parser.add_argument("--aggregate", default="true")
    args = parser.parse_args(list(argv) if argv is not None else None)

    return prefetch_once(Path(args.geojson_dir), Path(args.cache_file), aggregate=args.aggregate)


if __name__ == "__main__":
    raise SystemExit(main())
