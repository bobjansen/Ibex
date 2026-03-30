from __future__ import annotations

import importlib.util
from pathlib import Path


_ROOT_IMPL = Path(__file__).resolve().parents[1] / "ibex_ipython.py"
_SPEC = importlib.util.spec_from_file_location("_ibex_ipython_impl", _ROOT_IMPL)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"could not load ibex_ipython implementation from {_ROOT_IMPL}")

_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

for _name in dir(_MODULE):
    if _name.startswith("_"):
        continue
    globals()[_name] = getattr(_MODULE, _name)

__all__ = [name for name in globals() if not name.startswith("_")]
