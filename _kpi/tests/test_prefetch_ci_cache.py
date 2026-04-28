from __future__ import annotations

import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from prefetch_ci_cache import _cleanup_stale_cache_temp_files, _merge_save_cache


class PrefetchCiCacheTests(unittest.TestCase):
    def test_merge_save_cache_removes_stale_temp_files_and_keeps_existing_entries(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache_path = Path(td) / "ci_cache.json"
            cache_path.write_text(
                json.dumps(
                    {
                        "saved_at": "2026-01-01T00:00:00Z",
                        "entries": {"existing": {"payload": {"value": 10}, "fetched_at": 1}},
                    }
                ),
                encoding="utf-8",
            )

            stale_temp = Path(td) / "tmpstale"
            stale_temp.write_text("partial", encoding="utf-8")
            old_ts = time.time() - 7200
            os.utime(stale_temp, (old_ts, old_ts))

            _merge_save_cache(cache_path, {"new": {"payload": {"value": 20}, "fetched_at": 2}})

            self.assertFalse(stale_temp.exists())
            saved = json.loads(cache_path.read_text(encoding="utf-8"))
            self.assertEqual(set(saved["entries"]), {"existing", "new"})

    def test_cleanup_keeps_recent_temp_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            cache_path = Path(td) / "ci_cache.json"
            recent_temp = Path(td) / ".ci_cache.json.active.tmp"
            recent_temp.write_text("partial", encoding="utf-8")

            removed = _cleanup_stale_cache_temp_files(cache_path, max_age_s=3600)

            self.assertEqual(removed, 0)
            self.assertTrue(recent_temp.exists())


if __name__ == "__main__":
    unittest.main()
