from __future__ import annotations

import argparse
from pathlib import Path

from .config import (
    DEFAULT_ADJUSTED_PARQUET,
    DEFAULT_BT_PARQUET,
    DEFAULT_FACTOR_MAX,
    DEFAULT_FACTOR_MIN,
    DEFAULT_LEGACY_OUTPUT_ROOT,
    DEFAULT_MODES,
    DEFAULT_MSOA_GEOJSON,
    DEFAULT_MSOA_REGION_LOOKUP,
    DEFAULT_NTS_FILE,
    DEFAULT_OUTPUTS_ROOT,
    DEFAULT_REGION,
    DEFAULT_YEAR,
    MatrixConfig,
    ReassignConfig,
)
from .matrix import run_matrices
from .reassign import run_reassign


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--api-data-path",
        "--bt-parquet",
        dest="bt_parquet",
        type=Path,
        default=DEFAULT_BT_PARQUET,
        help="Path to BT API parquet input data.",
    )
    parser.add_argument(
        "--msoa-filter-list-path",
        "--msoa-filter-list",
        dest="msoa_filter_list",
        type=Path,
        default=None,
        help="Optional MSOA filter list csv (MSOA21CD). If omitted, process all UK MSOAs in API data.",
    )
    parser.add_argument("--msoa-geojson", type=Path, default=DEFAULT_MSOA_GEOJSON)
    parser.add_argument(
        "--msoa-region-lookup-path",
        type=Path,
        default=None,
        help=(
            "Optional CSV mapping MSOA21CD to NTS region name (Region of residence). "
            f"If omitted and {DEFAULT_MSOA_REGION_LOOKUP} exists, it will be used automatically."
        ),
    )
    parser.add_argument("--nts-file", "--nts-csv", dest="nts_file", type=Path, default=DEFAULT_NTS_FILE)
    parser.add_argument("--adjusted-parquet", type=Path, default=DEFAULT_ADJUSTED_PARQUET)
    parser.add_argument("--outputs-root", type=Path, default=DEFAULT_OUTPUTS_ROOT)
    parser.add_argument("--legacy-output", action="store_true")
    parser.add_argument("--legacy-output-root", type=Path, default=DEFAULT_LEGACY_OUTPUT_ROOT)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="UK travel model pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="Run reassignment and matrix generation")
    _add_common_args(run_parser)
    run_parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    run_parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help="Optional fallback NTS region if origin MSOA region cannot be resolved.",
    )
    run_parser.add_argument("--factor-min", type=float, default=DEFAULT_FACTOR_MIN)
    run_parser.add_argument("--factor-max", type=float, default=DEFAULT_FACTOR_MAX)
    run_parser.add_argument("--modes", default=",".join(DEFAULT_MODES))

    reassign_parser = sub.add_parser("reassign", help="Run reassignment stage")
    _add_common_args(reassign_parser)
    reassign_parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    reassign_parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help="Optional fallback NTS region if origin MSOA region cannot be resolved.",
    )
    reassign_parser.add_argument("--factor-min", type=float, default=DEFAULT_FACTOR_MIN)
    reassign_parser.add_argument("--factor-max", type=float, default=DEFAULT_FACTOR_MAX)

    matrix_parser = sub.add_parser("matrices", help="Generate OD matrices from adjusted parquet")
    _add_common_args(matrix_parser)
    matrix_parser.add_argument("--modes", default=",".join(DEFAULT_MODES))
    return parser


def _modes_from_arg(arg: str) -> tuple[str, ...]:
    items = tuple(x.strip().upper() for x in arg.split(",") if x.strip())
    if not items:
        raise ValueError("At least one mode must be provided in --modes.")
    return items


def main() -> None:
    args = build_parser().parse_args()
    legacy_root = args.legacy_output_root if args.legacy_output else None
    auto_region_lookup = DEFAULT_MSOA_REGION_LOOKUP if DEFAULT_MSOA_REGION_LOOKUP.exists() else None
    region_lookup = args.msoa_region_lookup_path or auto_region_lookup

    if args.command in {"run", "reassign"}:
        reassign_cfg = ReassignConfig(
            bt_parquet=args.bt_parquet,
            msoa_filter_csv=args.msoa_filter_list,
            msoa_region_lookup_csv=region_lookup,
            msoa_geojson=args.msoa_geojson,
            nts_file=args.nts_file,
            adjusted_parquet=args.adjusted_parquet,
            outputs_root=args.outputs_root,
            year=args.year,
            region=args.region,
            factor_min=args.factor_min,
            factor_max=args.factor_max,
        )
        print("[reassign] starting")
        run_reassign(reassign_cfg, legacy_output_root=legacy_root)
        print(f"[reassign] wrote {reassign_cfg.adjusted_parquet}")

    if args.command in {"run", "matrices"}:
        matrix_cfg = MatrixConfig(
            adjusted_parquet=args.adjusted_parquet,
            outputs_root=args.outputs_root,
            modes=_modes_from_arg(args.modes),
        )
        print("[matrices] starting")
        run_matrices(matrix_cfg, legacy_output_root=legacy_root)
        print(f"[matrices] wrote outputs under {matrix_cfg.outputs_root / 'matrices'}")


if __name__ == "__main__":
    main()
