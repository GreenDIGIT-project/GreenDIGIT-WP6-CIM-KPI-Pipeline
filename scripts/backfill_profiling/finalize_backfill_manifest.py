#!/usr/bin/env python3
"""Write checksums and sizes for every completed file in a backfill package."""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("folder", type=Path)
    args = parser.parse_args()
    folder = args.folder.resolve()
    output = folder / "final_manifest.json"
    paths = sorted(path for path in folder.rglob("*") if path.is_file() and path != output)
    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": [
            {"path": str(path.relative_to(folder)), "bytes": path.stat().st_size, "sha256": digest(path)}
            for path in paths
        ],
    }
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"manifest": str(output), "files": len(paths)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
