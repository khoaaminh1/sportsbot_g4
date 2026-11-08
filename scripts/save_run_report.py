"""Save the `<PREFIX> {json}` line a Spark job printed as docs/reports/runs/<name>-<run_id>.json.

Usage: python scripts/save_run_report.py CLASSIFIER_REPORT classifier data/classifier.log
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


def main(prefix: str, name: str, log_path: str) -> int:
    lines = [line for line in Path(log_path).read_text(encoding="utf-8").splitlines()]
    hits = [line[len(prefix) + 1 :] for line in lines if line.startswith(prefix + " ")]
    if not hits:
        print(f"No '{prefix}' line in {log_path}: the job failed (see the log).", file=sys.stderr)
        return 1
    report = json.loads(hits[-1])
    out = Path("docs/reports/runs") / f"{name}-{report['run_id']}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Report: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(*sys.argv[1:4]))
