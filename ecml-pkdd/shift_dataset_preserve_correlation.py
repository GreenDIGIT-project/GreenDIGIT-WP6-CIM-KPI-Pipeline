#!/usr/bin/env python3
import argparse
import csv
import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Shift dataset timestamps and values while preserving correlations."
    )
    p.add_argument("--input", required=True, help="Input CSV path")
    p.add_argument("--output", required=True, help="Output CSV path")
    p.add_argument(
        "--timestamp-col",
        default="bucket_15m",
        help="Timestamp column name (default: bucket_15m)",
    )
    p.add_argument(
        "--step-minutes",
        type=int,
        default=15,
        help="Time step in minutes for shift granularity (default: 15)",
    )
    p.add_argument(
        "--min-shift-days",
        type=int,
        default=14,
        help="Minimum absolute day shift (default: 14)",
    )
    p.add_argument(
        "--max-shift-days",
        type=int,
        default=120,
        help="Maximum absolute day shift (default: 120)",
    )
    p.add_argument(
        "--value-mode",
        choices=["none", "affine"],
        default="affine",
        help="Value transformation mode (default: affine)",
    )
    p.add_argument(
        "--numeric-cols",
        default="jobs,energy_wh,cfp_g,work,ncores,ncores_per_job",
        help="Comma-separated numeric columns to transform if present",
    )
    p.add_argument(
        "--scale-jitter",
        type=float,
        default=0.03,
        help="Affine scale jitter around 1.0 (default: 0.03 -> [0.97,1.03])",
    )
    p.add_argument(
        "--offset-std-frac",
        type=float,
        default=0.02,
        help="Offset as fraction of column stddev (default: 0.02)",
    )
    p.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible shifting (default: 42)",
    )
    p.add_argument(
        "--metadata-output",
        default="",
        help="Optional JSON metadata output path",
    )
    return p.parse_args()


def parse_ts(value: str) -> datetime:
    v = value.strip()
    if not v:
        raise ValueError("Empty timestamp")
    # Accept "YYYY-mm-dd HH:MM:SS" and ISO forms.
    if v.endswith("Z"):
        v = v[:-1]
    return datetime.fromisoformat(v.replace(" ", "T")).replace(tzinfo=None)


def fmt_ts(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def to_float_or_none(value: str):
    if value is None:
        return None
    v = str(value).strip()
    if v == "":
        return None
    try:
        return float(v)
    except ValueError:
        return None


def sample_std(values):
    n = len(values)
    if n <= 1:
        return 0.0
    mean = sum(values) / n
    var = sum((x - mean) ** 2 for x in values) / (n - 1)
    return math.sqrt(var)


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    in_path = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with in_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if args.timestamp_col not in fieldnames:
        raise ValueError(f"Timestamp column '{args.timestamp_col}' not found in CSV")

    # Pick a random signed shift in whole step units.
    min_steps = (args.min_shift_days * 24 * 60) // args.step_minutes
    max_steps = (args.max_shift_days * 24 * 60) // args.step_minutes
    if min_steps <= 0 or max_steps < min_steps:
        raise ValueError("Invalid shift day/step settings")
    sign = -1 if rng.random() < 0.5 else 1
    shift_steps = sign * rng.randint(min_steps, max_steps)
    shift_delta = timedelta(minutes=args.step_minutes * shift_steps)

    numeric_cols = [c.strip() for c in args.numeric_cols.split(",") if c.strip()]
    numeric_cols = [c for c in numeric_cols if c in fieldnames]

    col_values = {c: [] for c in numeric_cols}
    for row in rows:
        for c in numeric_cols:
            x = to_float_or_none(row.get(c))
            if x is not None:
                col_values[c].append(x)

    transforms = {}
    for c in numeric_cols:
        if args.value_mode == "none":
            transforms[c] = {"a": 1.0, "b": 0.0}
            continue
        a = 1.0 + rng.uniform(-args.scale_jitter, args.scale_jitter)
        # Keep positive scale to preserve correlation sign.
        a = max(a, 1e-8)
        std = sample_std(col_values[c]) if col_values[c] else 0.0
        b = rng.uniform(-args.offset_std_frac * std, args.offset_std_frac * std)
        transforms[c] = {"a": a, "b": b}

    out_rows = []
    for row in rows:
        out_row = dict(row)
        ts = parse_ts(row[args.timestamp_col])
        out_row[args.timestamp_col] = fmt_ts(ts + shift_delta)
        for c in numeric_cols:
            x = to_float_or_none(row.get(c))
            if x is None:
                continue
            t = transforms[c]
            y = t["a"] * x + t["b"]
            out_row[c] = f"{y:.10g}"
        out_rows.append(out_row)

    # Keep monotonic order by shifted timestamp for forecasting datasets.
    out_rows.sort(key=lambda r: parse_ts(r[args.timestamp_col]))

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows)

    metadata_path = Path(args.metadata_output) if args.metadata_output else out_path.with_suffix(".shift_meta.json")
    meta = {
        "input": str(in_path),
        "output": str(out_path),
        "timestamp_col": args.timestamp_col,
        "step_minutes": args.step_minutes,
        "shift_steps": shift_steps,
        "shift_minutes": shift_steps * args.step_minutes,
        "value_mode": args.value_mode,
        "seed": args.seed,
        "transforms": transforms,
        "note": "Per-column affine transforms preserve Pearson correlation (exactly for non-degenerate columns).",
    }
    metadata_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"Shifted dataset written: {out_path}")
    print(f"Shift metadata written: {metadata_path}")


if __name__ == "__main__":
    main()
