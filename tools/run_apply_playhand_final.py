from __future__ import annotations

import base64
import zlib
from pathlib import Path

source = Path("tools/apply_playhand_final.py").read_text(encoding="utf-8")
payload = source.split('"""', 2)[1]
code = zlib.decompress(base64.b85decode("".join(payload.split())))
exec(compile(code, "apply_playhand_final_v2.py", "exec"))
