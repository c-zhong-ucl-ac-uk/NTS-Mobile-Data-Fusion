from __future__ import annotations

from pathlib import Path

import dask.dataframe as dd
import pandas as pd

from .config import MatrixConfig
from .io import assert_dataframe_columns, assert_files_exist, ensure_dir
from .summary import write_qa_summary_tables


ADJUSTED_REQUIRED_COLUMNS = [
    "origin_msoa",
    "destination_msoa",
    "mode_of_transport",
    "time_period",
    "weekend_flag",
    "days_used",
]
DEFAULT_PURPOSE_CODES = tuple(range(1, 9))


def _write_dense_matrix_chunked(
    df: pd.DataFrame,
    out_path: Path,
    value_col: str,
    matrix_zones: list[str] | None = None,
    origin_chunk_size: int = 400,
) -> None:
    if matrix_zones is None:
        destinations = sorted(df["destination_msoa"].dropna().astype(str).unique().tolist())
        origins = sorted(df["origin_msoa"].dropna().astype(str).unique().tolist())
    else:
        destinations = matrix_zones
        origins = matrix_zones

    ensure_dir(out_path.parent)
    if df.empty and not origins:
        pd.DataFrame(columns=["origin_msoa"]).to_csv(out_path, index=False)
        return

    df = df.copy()
    df["origin_msoa"] = df["origin_msoa"].astype(str)
    df["destination_msoa"] = df["destination_msoa"].astype(str)

    first = True
    for i in range(0, len(origins), origin_chunk_size):
        chunk_origins = origins[i : i + origin_chunk_size]
        chunk = df[df["origin_msoa"].isin(chunk_origins)]
        mat = chunk.pivot(index="origin_msoa", columns="destination_msoa", values=value_col)
        mat = mat.reindex(index=chunk_origins, columns=destinations, fill_value=0)
        mat.index.name = "origin_msoa"
        mat = mat.reset_index()
        mat.to_csv(out_path, index=False, mode="w" if first else "a", header=first)
        first = False


def _write_mode_matrices(
    df: pd.DataFrame,
    modes: tuple[str, ...],
    out_dir: Path,
    value_col: str,
    matrix_suffix: str,
    matrix_zones: list[str],
) -> None:
    ensure_dir(out_dir)
    for mode in modes:
        mode_df = df[df["mode_of_transport"] == mode][["origin_msoa", "destination_msoa", value_col]]
        _write_dense_matrix_chunked(
            mode_df,
            out_dir / f"OD_matrix_{mode}_{matrix_suffix}.csv",
            value_col=value_col,
            matrix_zones=matrix_zones,
        )


def _write_mode_purpose_matrices(
    df: pd.DataFrame,
    modes: tuple[str, ...],
    out_dir: Path,
    value_col: str,
    matrix_suffix: str,
    matrix_zones: list[str],
) -> None:
    ensure_dir(out_dir)
    if "purpose" not in df.columns:
        return
    purposes = sorted(df["purpose"].dropna().astype(int).unique().tolist())
    for mode in modes:
        for purpose in purposes:
            mode_purpose_df = df[(df["mode_of_transport"] == mode) & (df["purpose"] == purpose)][
                ["origin_msoa", "destination_msoa", value_col]
            ]
            out = out_dir / f"OD_matrix_{mode}_{matrix_suffix}_by_purpose{purpose}.csv"
            _write_dense_matrix_chunked(mode_purpose_df, out, value_col=value_col, matrix_zones=matrix_zones)


def _write_mode_purpose_matrices_from_dask(
    df: dd.DataFrame,
    modes: tuple[str, ...],
    out_dir: Path,
    value_col: str,
    matrix_suffix: str,
    matrix_zones: list[str],
    am_peak_only: bool = False,
    purposes: tuple[int, ...] | None = None,
) -> None:
    ensure_dir(out_dir)
    if "purpose" not in df.columns:
        return

    purposes = purposes or DEFAULT_PURPOSE_CODES
    if am_peak_only:
        df = df[(df["weekend_flag"] == 0) & (df["time_period"] == "AM_peak")]

    for purpose in purposes:
        purpose_dd = df[df["purpose"] == purpose]
        grouped = (
            purpose_dd.groupby(["origin_msoa", "destination_msoa", "mode_of_transport", "purpose"])[value_col]
            .sum(split_out=1)
            .reset_index()
            .compute()
        )
        for mode in modes:
            mode_purpose_df = grouped[grouped["mode_of_transport"] == mode][
                ["origin_msoa", "destination_msoa", value_col]
            ]
            out = out_dir / f"OD_matrix_{mode}_{matrix_suffix}_by_purpose{purpose}.csv"
            _write_dense_matrix_chunked(mode_purpose_df, out, value_col=value_col, matrix_zones=matrix_zones)


def run_matrices(config: MatrixConfig, legacy_output_root: Path | None = None) -> None:
    assert_files_exist([config.adjusted_parquet])
    all_data_dd = dd.read_parquet(config.adjusted_parquet)
    assert_dataframe_columns(all_data_dd._meta, ADJUSTED_REQUIRED_COLUMNS, "Adjusted parquet")
    matrix_zones = sorted(
        pd.concat(
            [
                all_data_dd["origin_msoa"].dropna().astype(str).drop_duplicates().compute(),
                all_data_dd["destination_msoa"].dropna().astype(str).drop_duplicates().compute(),
            ],
            ignore_index=True,
        )
        .drop_duplicates()
        .tolist()
    )

    typical_dir = config.outputs_root / "matrices" / "typical_week_by_mode"
    ampeak_dir = config.outputs_root / "matrices" / "weekday_AMpeak_by_mode"

    if not config.purpose_only:
        if "volume" in all_data_dd.columns:
            raw_base_vol = dd.to_numeric(all_data_dd["volume"], errors="coerce").fillna(0.0)
        elif "volume_adj" in all_data_dd.columns:
            raw_base_vol = dd.to_numeric(all_data_dd["volume_adj"], errors="coerce").fillna(0.0)
        else:
            raise ValueError("Adjusted parquet must contain `volume` or `volume_adj`.")

        if "volume_adj" in all_data_dd.columns:
            adjusted_base_vol = dd.to_numeric(all_data_dd["volume_adj"], errors="coerce").fillna(0.0)
        else:
            adjusted_base_vol = raw_base_vol

        days_used = dd.to_numeric(all_data_dd["days_used"], errors="coerce").replace(0, float("nan"))
        all_data_dd["raw_volume_per_day"] = (raw_base_vol / days_used).fillna(0.0)
        all_data_dd["adjusted_volume_per_day"] = (adjusted_base_vol / days_used).fillna(0.0)
        weekend_multiplier = 5 - 3 * all_data_dd["weekend_flag"].astype("int8")
        all_data_dd["raw_volume_per_typical_week"] = all_data_dd["raw_volume_per_day"] * weekend_multiplier
        all_data_dd["adjusted_volume_per_typical_week"] = all_data_dd["adjusted_volume_per_day"] * weekend_multiplier

        raw_by_typical_week = (
            all_data_dd.groupby(["origin_msoa", "destination_msoa", "mode_of_transport"])["raw_volume_per_typical_week"]
            .sum()
            .reset_index()
            .compute()
        )
        raw_by_am_peak = (
            all_data_dd[(all_data_dd["weekend_flag"] == 0) & (all_data_dd["time_period"] == "AM_peak")]
            .groupby(["origin_msoa", "destination_msoa", "mode_of_transport"])["raw_volume_per_day"]
            .sum()
            .reset_index()
            .compute()
        )
        adjusted_by_typical_week = (
            all_data_dd.groupby(["origin_msoa", "destination_msoa", "mode_of_transport"])[
                "adjusted_volume_per_typical_week"
            ]
            .sum()
            .reset_index()
            .compute()
        )
        adjusted_by_am_peak = (
            all_data_dd[(all_data_dd["weekend_flag"] == 0) & (all_data_dd["time_period"] == "AM_peak")]
            .groupby(["origin_msoa", "destination_msoa", "mode_of_transport"])["adjusted_volume_per_day"]
            .sum()
            .reset_index()
            .compute()
        )

        _write_mode_matrices(
            raw_by_typical_week, config.modes, typical_dir, "raw_volume_per_typical_week", "raw", matrix_zones
        )
        _write_mode_matrices(raw_by_am_peak, config.modes, ampeak_dir, "raw_volume_per_day", "raw", matrix_zones)
        _write_mode_matrices(
            adjusted_by_typical_week,
            config.modes,
            typical_dir,
            "adjusted_volume_per_typical_week",
            "adjusted",
            matrix_zones,
        )
        _write_mode_matrices(
            adjusted_by_am_peak, config.modes, ampeak_dir, "adjusted_volume_per_day", "adjusted", matrix_zones
        )

        if legacy_output_root is not None:
            legacy_typical = legacy_output_root / "matrices" / "typical_week_by_mode"
            legacy_ampeak = legacy_output_root / "matrices" / "weekday_AMpeak_by_mode"
            _write_mode_matrices(
                raw_by_typical_week, config.modes, legacy_typical, "raw_volume_per_typical_week", "raw", matrix_zones
            )
            _write_mode_matrices(raw_by_am_peak, config.modes, legacy_ampeak, "raw_volume_per_day", "raw", matrix_zones)
            _write_mode_matrices(
                adjusted_by_typical_week,
                config.modes,
                legacy_typical,
                "adjusted_volume_per_typical_week",
                "adjusted",
                matrix_zones,
            )
            _write_mode_matrices(
                adjusted_by_am_peak, config.modes, legacy_ampeak, "adjusted_volume_per_day", "adjusted", matrix_zones
            )

    if config.purpose_parquet is not None and config.purpose_parquet.exists():
        purpose_dd = dd.read_parquet(config.purpose_parquet)
        if "volume_adj_purpose" in purpose_dd.columns and "purpose" in purpose_dd.columns:
            purpose_dd["volume_adj_purpose"] = dd.to_numeric(purpose_dd["volume_adj_purpose"], errors="coerce").fillna(0.0)
            purpose_days = dd.to_numeric(purpose_dd["days_used"], errors="coerce").replace(0, float("nan"))
            purpose_dd["volume_per_day_purpose"] = (purpose_dd["volume_adj_purpose"] / purpose_days).fillna(0.0)
            purpose_weekend_multiplier = 5 - 3 * purpose_dd["weekend_flag"].astype("int8")
            purpose_dd["volume_per_typical_week_purpose"] = (
                purpose_dd["volume_per_day_purpose"] * purpose_weekend_multiplier
            )

            _write_mode_purpose_matrices_from_dask(
                purpose_dd,
                config.modes,
                typical_dir,
                "volume_per_typical_week_purpose",
                "adjusted",
                matrix_zones,
            )
            _write_mode_purpose_matrices_from_dask(
                purpose_dd,
                config.modes,
                ampeak_dir,
                "volume_per_day_purpose",
                "adjusted",
                matrix_zones,
                am_peak_only=True,
            )
            if legacy_output_root is not None:
                legacy_typical = legacy_output_root / "matrices" / "typical_week_by_mode"
                legacy_ampeak = legacy_output_root / "matrices" / "weekday_AMpeak_by_mode"
                _write_mode_purpose_matrices_from_dask(
                    purpose_dd,
                    config.modes,
                    legacy_typical,
                    "volume_per_typical_week_purpose",
                    "adjusted",
                    matrix_zones,
                )
                _write_mode_purpose_matrices_from_dask(
                    purpose_dd,
                    config.modes,
                    legacy_ampeak,
                    "volume_per_day_purpose",
                    "adjusted",
                    matrix_zones,
                    am_peak_only=True,
                )

    if config.write_qa_summary:
        write_qa_summary_tables(
            adjusted_parquet=config.adjusted_parquet,
            purpose_parquet=config.purpose_parquet,
            nts_mode_time_split_csv=config.nts_mode_time_split_csv,
            purposes_csv=config.purposes_csv,
            output_dir=config.outputs_root / "qa",
        )
        if legacy_output_root is not None:
            write_qa_summary_tables(
                adjusted_parquet=config.adjusted_parquet,
                purpose_parquet=config.purpose_parquet,
                nts_mode_time_split_csv=config.nts_mode_time_split_csv,
                purposes_csv=config.purposes_csv,
                output_dir=legacy_output_root / "qa",
            )
