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
    DEFAULT_MODES_WITH_ROAD_SPLIT,
    DEFAULT_MSOA_GEOJSON,
    DEFAULT_MSOA_REGION_LOOKUP,
    DEFAULT_NTS_FILE,
    DEFAULT_NTS0502_PERIOD_PURPOSE_CSV,
    DEFAULT_NTS_MODE_TIME_SPLIT,
    DEFAULT_OUTPUTS_ROOT,
    DEFAULT_POP_LSOA_INTERNAL,
    DEFAULT_PURPOSE_PARQUET,
    DEFAULT_PURPOSES_CSV,
    DEFAULT_REGION,
    DEFAULT_TFN_AREA_TYPE_LSOA,
    DEFAULT_LSOA_MSOA_LOOKUP,
    DEFAULT_YEAR,
    LocalAuthorityVisualConfig,
    MatrixConfig,
    ReassignConfig,
)
from .local_authority import LocalAuthorityUseCaseConfig, run_local_authority_use_case
from .export import export_adjusted_trip_csv
from .matrix import run_matrices
from .reassign import run_reassign


def run_local_authority_visualisations(config: LocalAuthorityVisualConfig) -> None:
    """Load plotting dependencies only when the visualisation command runs."""
    from .local_authority_visuals import run_local_authority_visualisations as run_visualisations

    run_visualisations(config)


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
    parser.add_argument("--purpose-parquet", type=Path, default=DEFAULT_PURPOSE_PARQUET)
    parser.add_argument(
        "--pop-lsoa-internal-csv",
        "--population-csv",
        dest="pop_lsoa_internal_csv",
        type=Path,
        default=DEFAULT_POP_LSOA_INTERNAL,
        help="Wide TfN population CSV with demographic columns and LSOA population columns.",
    )
    parser.add_argument("--tfn-area-type-lsoa-csv", type=Path, default=DEFAULT_TFN_AREA_TYPE_LSOA)
    parser.add_argument("--lsoa-msoa-lookup-csv", type=Path, default=DEFAULT_LSOA_MSOA_LOOKUP)
    parser.add_argument("--nts-mode-time-split-csv", type=Path, default=DEFAULT_NTS_MODE_TIME_SPLIT)
    parser.add_argument("--purposes-csv", type=Path, default=DEFAULT_PURPOSES_CSV)
    parser.add_argument(
        "--nts0502-period-purpose-csv",
        type=Path,
        default=DEFAULT_NTS0502_PERIOD_PURPOSE_CSV,
        help="Preprocessed NTS0502 weekday period/purpose control CSV for NTS0502 purpose calibration.",
    )
    parser.add_argument(
        "--purpose-calibration",
        choices=("mode_time_split", "nts0502"),
        default="mode_time_split",
        help=(
            "Purpose allocation method. `mode_time_split` keeps the existing local purpose shares; "
            "`nts0502` rakes local priors to NTS0502 weekday period/purpose controls."
        ),
    )
    parser.add_argument(
        "--skip-purpose-estimation",
        action="store_true",
        help="Skip purpose estimation, by-purpose matrices, and purpose QA (including existing purpose data).",
    )
    parser.add_argument(
        "--skip-mode-time-constraint",
        action="store_true",
        help="Disable the NTS mode/time share constraint after mode reassignment.",
    )
    parser.add_argument(
        "--skip-child-origin-uplift",
        action="store_true",
        help="Keep the legacy adult-only BT base instead of uplifting origins to an all-age base.",
    )
    parser.add_argument(
        "--split-road-mode",
        action="store_true",
        help="Split ROAD into CYCLE/PRIVATE_CAR/MOTORCYCLE/BUS using NTS shares.",
    )
    parser.add_argument("--adjusted-parquet", type=Path, default=DEFAULT_ADJUSTED_PARQUET)
    parser.add_argument(
        "--adjusted-csv-dir",
        type=Path,
        default=None,
        help="Also export adjusted trip records as numbered CSV parts in this directory.",
    )
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
    run_parser.add_argument(
        "--skip-qa-summary",
        action="store_true",
        help="Skip run-level QA summary CSVs after matrix generation.",
    )

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
    matrix_parser.add_argument(
        "--purpose-only",
        action="store_true",
        help="Only generate adjusted by-purpose matrices from --purpose-parquet.",
    )
    matrix_parser.add_argument(
        "--skip-qa-summary",
        action="store_true",
        help="Skip run-level QA summary CSVs after matrix generation.",
    )

    la_defaults = LocalAuthorityUseCaseConfig()
    local_parser = sub.add_parser(
        "local-authority-use-case",
        aliases=["local-authority"],
        help="Build Luton-Dunstable and Luton Airport local authority evidence extracts",
    )
    local_parser.add_argument("--purpose-parquet", type=Path, default=la_defaults.purpose_parquet)
    local_parser.add_argument("--adjusted-parquet", type=Path, default=la_defaults.adjusted_parquet)
    local_parser.add_argument("--msoa-geojson", type=Path, default=la_defaults.msoa_geojson)
    local_parser.add_argument("--msoa-lad-lookup-csv", type=Path, default=la_defaults.msoa_lad_lookup_csv)
    local_parser.add_argument("--eeh-msoa-lookup-csv", type=Path, default=la_defaults.eeh_msoa_lookup_csv)
    local_parser.add_argument("--output-dir", type=Path, default=la_defaults.output_dir)
    local_parser.add_argument("--matrix-dir", type=Path, default=la_defaults.matrix_dir)
    local_parser.add_argument("--dunstable-core-km", type=float, default=la_defaults.dunstable_core_km)
    local_parser.add_argument("--dunstable-wider-km", type=float, default=la_defaults.dunstable_wider_km)
    local_parser.add_argument("--airport-core-km", type=float, default=la_defaults.airport_core_km)
    local_parser.add_argument("--airport-wider-km", type=float, default=la_defaults.airport_wider_km)
    local_parser.add_argument("--dunstable-easting", type=float, default=la_defaults.dunstable_easting)
    local_parser.add_argument("--dunstable-northing", type=float, default=la_defaults.dunstable_northing)
    local_parser.add_argument("--airport-easting", type=float, default=la_defaults.airport_easting)
    local_parser.add_argument("--airport-northing", type=float, default=la_defaults.airport_northing)
    local_parser.add_argument("--top-n", type=int, default=la_defaults.top_n)
    local_parser.add_argument("--map-flow-limit", type=int, default=la_defaults.map_flow_limit)
    local_parser.add_argument("--near-zero-threshold", type=float, default=la_defaults.near_zero_threshold)

    viz_defaults = LocalAuthorityVisualConfig()
    viz_parser = sub.add_parser(
        "local-authority-visualisations",
        aliases=["local-authority-visualizations", "local-authority-visuals"],
        help="Build static visualisations from local authority evidence extracts",
    )
    viz_parser.add_argument("--input-dir", type=Path, default=viz_defaults.input_dir)
    viz_parser.add_argument("--output-dir", type=Path, default=viz_defaults.output_dir)
    viz_parser.add_argument("--msoa-geojson", type=Path, default=viz_defaults.msoa_geojson)
    viz_parser.add_argument("--top-n", type=int, default=viz_defaults.top_n)
    return parser


def _modes_from_arg(arg: str) -> tuple[str, ...]:
    items = tuple(x.strip().upper() for x in arg.split(",") if x.strip())
    if not items:
        raise ValueError("At least one mode must be provided in --modes.")
    return items


def main() -> None:
    args = build_parser().parse_args()
    legacy_root = args.legacy_output_root if getattr(args, "legacy_output", False) else None
    auto_region_lookup = DEFAULT_MSOA_REGION_LOOKUP if DEFAULT_MSOA_REGION_LOOKUP.exists() else None
    region_lookup = getattr(args, "msoa_region_lookup_path", None) or auto_region_lookup

    if args.command in {"run", "reassign"}:
        reassign_cfg = ReassignConfig(
            bt_parquet=args.bt_parquet,
            msoa_filter_csv=args.msoa_filter_list,
            msoa_region_lookup_csv=region_lookup,
            msoa_geojson=args.msoa_geojson,
            nts_file=args.nts_file,
            adjusted_parquet=args.adjusted_parquet,
            purpose_parquet=args.purpose_parquet,
            pop_lsoa_internal_csv=args.pop_lsoa_internal_csv,
            tfn_area_type_lsoa_csv=args.tfn_area_type_lsoa_csv,
            lsoa_msoa_lookup_csv=args.lsoa_msoa_lookup_csv,
            nts_mode_time_split_csv=args.nts_mode_time_split_csv,
            purposes_csv=args.purposes_csv,
            nts0502_period_purpose_csv=args.nts0502_period_purpose_csv,
            estimate_purpose=not args.skip_purpose_estimation,
            purpose_calibration=args.purpose_calibration,
            split_road_mode=args.split_road_mode,
            constrain_mode_time_share=not args.skip_mode_time_constraint,
            apply_child_origin_uplift=not args.skip_child_origin_uplift,
            outputs_root=args.outputs_root,
            year=args.year,
            region=args.region,
            factor_min=args.factor_min,
            factor_max=args.factor_max,
        )
        print("[reassign] starting")
        run_reassign(reassign_cfg, legacy_output_root=legacy_root)
        print(f"[reassign] wrote {reassign_cfg.adjusted_parquet}")

    if args.command in {"run", "reassign", "matrices"} and args.adjusted_csv_dir is not None:
        print(f"[export] writing adjusted trip CSV parts -> {args.adjusted_csv_dir}")
        csv_paths = export_adjusted_trip_csv(args.adjusted_parquet, args.adjusted_csv_dir)
        print(f"[export] wrote {len(csv_paths)} CSV parts")

    if args.command in {"run", "matrices"}:
        modes_arg = args.modes
        if args.split_road_mode and args.modes.strip().upper() == ",".join(DEFAULT_MODES):
            modes_arg = ",".join(DEFAULT_MODES_WITH_ROAD_SPLIT)
        matrix_cfg = MatrixConfig(
            adjusted_parquet=args.adjusted_parquet,
            purpose_parquet=None if args.skip_purpose_estimation else args.purpose_parquet,
            nts_mode_time_split_csv=args.nts_mode_time_split_csv,
            purposes_csv=args.purposes_csv,
            outputs_root=args.outputs_root,
            modes=_modes_from_arg(modes_arg),
            purpose_only=getattr(args, "purpose_only", False),
            write_qa_summary=not getattr(args, "skip_qa_summary", False),
        )
        print("[matrices] starting")
        run_matrices(matrix_cfg, legacy_output_root=legacy_root)
        print(f"[matrices] wrote outputs under {matrix_cfg.outputs_root / 'matrices'}")

    if args.command in {"local-authority-use-case", "local-authority"}:
        local_cfg = LocalAuthorityUseCaseConfig(
            purpose_parquet=args.purpose_parquet,
            adjusted_parquet=args.adjusted_parquet,
            msoa_geojson=args.msoa_geojson,
            msoa_lad_lookup_csv=args.msoa_lad_lookup_csv,
            eeh_msoa_lookup_csv=args.eeh_msoa_lookup_csv,
            output_dir=args.output_dir,
            matrix_dir=args.matrix_dir,
            dunstable_core_km=args.dunstable_core_km,
            dunstable_wider_km=args.dunstable_wider_km,
            airport_core_km=args.airport_core_km,
            airport_wider_km=args.airport_wider_km,
            top_n=args.top_n,
            map_flow_limit=args.map_flow_limit,
            near_zero_threshold=args.near_zero_threshold,
            dunstable_easting=args.dunstable_easting,
            dunstable_northing=args.dunstable_northing,
            airport_easting=args.airport_easting,
            airport_northing=args.airport_northing,
        )
        print("[local-authority] starting")
        run_local_authority_use_case(local_cfg)
        print(f"[local-authority] wrote outputs under {local_cfg.output_dir}")

    if args.command in {"local-authority-visualisations", "local-authority-visualizations", "local-authority-visuals"}:
        viz_cfg = LocalAuthorityVisualConfig(
            input_dir=args.input_dir,
            output_dir=args.output_dir,
            msoa_geojson=args.msoa_geojson,
            top_n=args.top_n,
        )
        print("[local-authority-visualisations] starting")
        run_local_authority_visualisations(viz_cfg)
        print(f"[local-authority-visualisations] wrote outputs under {viz_cfg.output_dir}")


if __name__ == "__main__":
    main()
