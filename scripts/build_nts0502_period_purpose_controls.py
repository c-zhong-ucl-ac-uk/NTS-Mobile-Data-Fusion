#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from uk_travel_pipeline.config import DEFAULT_NTS0502_PERIOD_PURPOSE_CSV
from uk_travel_pipeline.nts0502 import build_nts0502_period_purpose_controls


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build BT-period purpose-control shares from NTS0502 ODS."
    )
    parser.add_argument(
        "--nts0502-ods",
        type=Path,
        required=True,
        help="Path to the downloaded NTS0502 ODS workbook.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=DEFAULT_NTS0502_PERIOD_PURPOSE_CSV,
        help="Output CSV consumed by --purpose-calibration nts0502.",
    )
    parser.add_argument(
        "--year",
        default=None,
        help="Optional NTS0502 year label, for example '2023 to 2024'. Defaults to the latest label.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    controls = build_nts0502_period_purpose_controls(args.nts0502_ods, year=args.year)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    controls.to_csv(args.output_csv, index=False, float_format="%.10f")
    print(f"Wrote {args.output_csv}")


if __name__ == "__main__":
    main()
