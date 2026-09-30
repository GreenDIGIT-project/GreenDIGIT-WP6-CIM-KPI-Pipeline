import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "process_dump", ROOT / "scripts" / "batch_submit_cnr" / "process_dump.py"
)
process_dump = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(process_dump)


def test_resolve_group_preserves_explicit_group():
    assert process_dump._resolve_group(" private ", "greendigit") == "private"


def test_resolve_group_uses_default_for_missing_or_blank_group():
    assert process_dump._resolve_group(None, "greendigit") == "greendigit"
    assert process_dump._resolve_group("  ", "greendigit") == "greendigit"


def test_resolve_group_remains_null_without_default():
    assert process_dump._resolve_group(None, None) is None
